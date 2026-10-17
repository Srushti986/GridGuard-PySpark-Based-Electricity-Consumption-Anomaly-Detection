"""Storage abstraction: real HDFS or the local filesystem - never a fake mix.

* ``resolve_storage`` decides which backend is *actually* usable and records why.
  HDFS is only chosen when it is enabled in the config AND the NameNode port answers.
* A local folder is never described as HDFS: ``StorageContext.mode`` is either
  ``"hdfs"`` or ``"local"`` and ``StorageContext.label`` says so in plain words.
* HDFS file operations go through Spark's own Hadoop client (py4j), so no extra
  Python package is needed.
"""
from __future__ import annotations

import socket
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

from .config import HDFS_DIRECTORIES, Config


@dataclass(frozen=True)
class StorageContext:
    mode: str                 # "hdfs" or "local"
    root: str                 # hdfs://host:port/gridguard  OR  /abs/path/to/outputs
    reason: str               # why this mode is active (shown to the user)
    namenode_uri: str = ""    # only for mode == "hdfs"

    @property
    def is_hdfs(self) -> bool:
        return self.mode == "hdfs"

    @property
    def label(self) -> str:
        if self.is_hdfs:
            return f"HDFS ({self.root})"
        return f"Local filesystem ({self.root}) - NOT HDFS"

    def path(self, rel: str) -> str:
        """Full path string understood by Spark for a relative table location."""
        return f"{self.root.rstrip('/')}/{rel.strip('/')}"


def check_hdfs_reachable(uri: str, timeout: float = 3.0) -> tuple[bool, str]:
    """Cheap TCP check of the NameNode RPC port (does not need Spark)."""
    parsed = urlparse(uri)
    if parsed.scheme != "hdfs" or not parsed.hostname:
        return False, f"'{uri}' is not a valid hdfs:// URI"
    port = parsed.port or 8020
    try:
        with socket.create_connection((parsed.hostname, port), timeout=timeout):
            return True, f"NameNode port {parsed.hostname}:{port} is reachable"
    except OSError as exc:
        return False, f"Cannot reach NameNode at {parsed.hostname}:{port} ({exc})"


def local_context(cfg: Config, reason: str = "Local filesystem selected") -> StorageContext:
    return StorageContext(mode="local", root=str(Path(cfg.output_dir).resolve()), reason=reason)


def resolve_storage(cfg: Config, use_hdfs: bool | None = None) -> StorageContext:
    """Pick the storage backend that will REALLY be used.

    ``use_hdfs=None`` means "follow cfg.hdfs_enabled". If HDFS was requested but the
    NameNode is unreachable, we fall back to local storage and say so in ``reason``.
    """
    wanted = cfg.hdfs_enabled if use_hdfs is None else use_hdfs
    if not wanted:
        return local_context(cfg, "HDFS not enabled - running in standalone (local) mode")
    ok, msg = check_hdfs_reachable(cfg.hdfs_namenode_uri)
    if not ok:
        return local_context(
            cfg, f"HDFS was requested but is unavailable: {msg}. Falling back to local storage."
        )
    return StorageContext(
        mode="hdfs",
        root=f"{cfg.hdfs_namenode_uri.rstrip('/')}{cfg.hdfs_base_dir}",
        reason=msg,
        namenode_uri=cfg.hdfs_namenode_uri,
    )


# ---------------------------------------------------------------------------
# Operations that need a SparkSession
# ---------------------------------------------------------------------------
def _hadoop(spark, ctx: StorageContext):
    """Return (filesystem, Path class) from Spark's JVM Hadoop client."""
    jvm = spark._jvm
    conf = spark._jsc.hadoopConfiguration()
    uri = ctx.namenode_uri if ctx.is_hdfs else "file:///"
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(jvm.java.net.URI.create(uri), conf)
    return fs, jvm.org.apache.hadoop.fs.Path


def ensure_directories(ctx: StorageContext, spark) -> list[str]:
    """Create the GridGuard directory layout. Returns the created paths."""
    created = []
    if ctx.is_hdfs:
        fs, Path_ = _hadoop(spark, ctx)
        for rel in HDFS_DIRECTORIES:
            p = ctx.path(rel)
            fs.mkdirs(Path_(p))
            created.append(p)
    else:
        for rel in HDFS_DIRECTORIES:
            if rel == "raw":
                continue  # standalone mode reads the raw file straight from data/
            p = Path(ctx.path(rel))
            p.mkdir(parents=True, exist_ok=True)
            created.append(str(p))
    return created


def upload_raw(ctx: StorageContext, spark, local_file: Path) -> str:
    """Put the raw file into HDFS /raw and return the path Spark should read from.

    In local mode nothing is copied: the original file is read where it is.
    """
    local_file = Path(local_file).resolve()
    if not local_file.is_file():
        raise FileNotFoundError(f"Raw dataset not found: {local_file}")
    if not ctx.is_hdfs:
        return str(local_file)
    fs, Path_ = _hadoop(spark, ctx)
    dest = ctx.path(f"raw/{local_file.name}")
    # copyFromLocalFile(delSrc=False, overwrite=True, src, dst)
    fs.copyFromLocalFile(False, True, Path_("file://" + str(local_file)), Path_(dest))
    if not fs.exists(Path_(dest)):
        raise RuntimeError(f"Upload to HDFS reported success but {dest} does not exist")
    return dest


def write_table(ctx: StorageContext, df, rel: str) -> str:
    """Write a (small) result table as a single headered CSV part file."""
    path = ctx.path(rel)
    (
        df.coalesce(1).write.mode("overwrite")
        .option("header", "true")
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
        .csv(path)
    )
    return path


def verify_table(ctx: StorageContext, spark, rel: str) -> dict:
    """Check that a table directory has a _SUCCESS marker and non-empty part files."""
    path = ctx.path(rel)
    files = []
    if ctx.is_hdfs:
        fs, Path_ = _hadoop(spark, ctx)
        if fs.exists(Path_(path)):
            for st in fs.listStatus(Path_(path)):
                files.append({"name": st.getPath().getName(), "bytes": int(st.getLen())})
    else:
        p = Path(path)
        if p.is_dir():
            files = [{"name": f.name, "bytes": f.stat().st_size} for f in p.iterdir() if f.is_file()]
    has_success = any(f["name"] == "_SUCCESS" for f in files)
    parts = [f for f in files if f["name"].startswith("part-") and f["bytes"] > 0]
    return {
        "path": path,
        "storage": ctx.mode,
        "ok": bool(has_success and parts),
        "has_success_marker": has_success,
        "part_files": len(parts),
        "total_bytes": sum(f["bytes"] for f in parts),
    }
