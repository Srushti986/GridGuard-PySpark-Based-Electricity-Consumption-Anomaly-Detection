"""Reading pre-computed GridGuard outputs for the dashboard (no Spark needed).

Two sources are supported:
  * local   - CSV part files under the configured output directory
  * hdfs    - the same files read through WebHDFS (HTTP, port 9870 by default)

Every loader returns a ``TableResult`` instead of raising, so the dashboard can show
a friendly message when files are missing, empty, or HDFS is unavailable.
"""
from __future__ import annotations

import io
import json
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path

import pandas as pd

from .config import (
    ALL_OUTPUT_TABLES, ANOMALY_TABLE, DAILY_TABLE, HOURLY_TABLE, PEAK_FREQ_TABLE,
    PEAK_TOP_TABLE, SUMMARY_TABLE, Config,
)

TABLE_KEYS = {
    "hourly": HOURLY_TABLE,
    "daily": DAILY_TABLE,
    "summary": SUMMARY_TABLE,
    "anomalies": ANOMALY_TABLE,
    "peak_top": PEAK_TOP_TABLE,
    "peak_freq": PEAK_FREQ_TABLE,
}
DATE_COLUMNS = ("timestamp", "date", "processed_at", "data_start", "data_end")


@dataclass
class TableResult:
    name: str
    df: pd.DataFrame = field(default_factory=pd.DataFrame)
    status: str = "missing"       # ok | missing | empty | error
    message: str = ""
    source: str = "local"         # local | hdfs
    location: str = ""

    @property
    def ok(self) -> bool:
        return self.status == "ok" and not self.df.empty


def _parse_dates(df: pd.DataFrame) -> pd.DataFrame:
    for col in DATE_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")
    return df


def _is_data_file(name: str) -> bool:
    return name.endswith(".csv") and not name.startswith((".", "_"))


def _concat(frames: list) -> pd.DataFrame:
    frames = [f for f in frames if f is not None and not f.empty]
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


# ---------------------------------------------------------------------------
# Local files
# ---------------------------------------------------------------------------
def load_local_table(output_dir: Path, rel: str, name: str = "") -> TableResult:
    folder = Path(output_dir) / rel
    name = name or rel
    if not folder.exists():
        return TableResult(name, status="missing", location=str(folder),
                           message=f"{folder} does not exist. Run `python run_pipeline.py` first.")
    parts = sorted(p for p in folder.glob("*.csv") if _is_data_file(p.name))
    if not parts:
        return TableResult(name, status="empty", location=str(folder),
                           message=f"{folder} has no CSV part files (the table may be empty).")
    try:
        df = _concat([pd.read_csv(p) for p in parts])
    except Exception as exc:  # corrupt / partially written file
        return TableResult(name, status="error", location=str(folder),
                           message=f"Could not read {folder}: {exc}")
    if df.empty:
        return TableResult(name, status="empty", location=str(folder),
                           message=f"{folder} contains no rows.")
    return TableResult(name, _parse_dates(df), "ok", "", "local", str(folder))


# ---------------------------------------------------------------------------
# WebHDFS (read-only)
# ---------------------------------------------------------------------------
def _webhdfs_url(base: str, path: str, op: str, user: str) -> str:
    q = urllib.parse.urlencode({"op": op, "user.name": user})
    return f"{base.rstrip('/')}/webhdfs/v1{urllib.parse.quote(path)}?{q}"


def check_webhdfs(base_url: str, user: str = "hadoop", timeout: float = 3.0) -> tuple[bool, str]:
    try:
        with urllib.request.urlopen(_webhdfs_url(base_url, "/", "LISTSTATUS", user), timeout=timeout) as r:
            json.load(r)
        return True, f"WebHDFS reachable at {base_url}"
    except Exception as exc:
        return False, f"WebHDFS not reachable at {base_url} ({exc})"


def load_hdfs_table(cfg: Config, rel: str, name: str = "", timeout: float = 20.0) -> TableResult:
    path = f"{cfg.hdfs_base_dir.rstrip('/')}/{rel}"
    name = name or rel
    location = f"{cfg.hdfs_namenode_uri}{path}"
    try:
        with urllib.request.urlopen(
            _webhdfs_url(cfg.webhdfs_url, path, "LISTSTATUS", cfg.hdfs_user), timeout=timeout
        ) as r:
            listing = json.load(r)["FileStatuses"]["FileStatus"]
    except urllib.error.HTTPError as exc:
        status = "missing" if exc.code == 404 else "error"
        return TableResult(name, status=status, source="hdfs", location=location,
                           message=f"HDFS path {path} not available (HTTP {exc.code}).")
    except Exception as exc:
        return TableResult(name, status="error", source="hdfs", location=location,
                           message=f"HDFS unavailable: {exc}")
    parts = [f["pathSuffix"] for f in listing if f.get("type") == "FILE" and _is_data_file(f["pathSuffix"])]
    if not parts:
        return TableResult(name, status="empty", source="hdfs", location=location,
                           message=f"No CSV part files in HDFS path {path}.")
    frames = []
    try:
        for part in sorted(parts):
            url = _webhdfs_url(cfg.webhdfs_url, f"{path}/{part}", "OPEN", cfg.hdfs_user)
            with urllib.request.urlopen(url, timeout=timeout) as r:  # follows the DataNode redirect
                frames.append(pd.read_csv(io.BytesIO(r.read())))
    except Exception as exc:
        return TableResult(name, status="error", source="hdfs", location=location,
                           message=(f"Could not read file data from HDFS: {exc}. The NameNode answered, but the "
                                    "DataNode redirect failed - add '127.0.0.1 datanode' to your hosts file "
                                    "(see hadoop/README.md)."))
    df = _concat(frames)
    if df.empty:
        return TableResult(name, status="empty", source="hdfs", location=location,
                           message=f"HDFS path {path} contains no rows.")
    return TableResult(name, _parse_dates(df), "ok", "", "hdfs", location)


# ---------------------------------------------------------------------------
# Selection logic
# ---------------------------------------------------------------------------
def resolve_backend(cfg: Config, requested: str = "auto") -> tuple[str, str]:
    """Return (backend, explanation). requested: auto | local | hdfs."""
    requested = requested.lower()
    if requested == "local":
        return "local", "Local output files selected."
    if requested == "hdfs":
        ok, msg = check_webhdfs(cfg.webhdfs_url, cfg.hdfs_user)
        return ("hdfs", msg) if ok else ("local", f"HDFS selected but unavailable - {msg}. Using local files instead.")
    if cfg.hdfs_enabled:
        ok, msg = check_webhdfs(cfg.webhdfs_url, cfg.hdfs_user)
        if ok:
            return "hdfs", msg
        return "local", f"HDFS is enabled in the config but unavailable - {msg}. Using local files instead."
    return "local", "HDFS is not enabled (GRIDGUARD_HDFS_ENABLED=false) - reading local output files."


def load_table(cfg: Config, key: str, backend: str) -> TableResult:
    rel = TABLE_KEYS[key]
    if backend == "hdfs":
        return load_hdfs_table(cfg, rel, key)
    return load_local_table(cfg.output_dir, rel, key)


def load_all(cfg: Config, backend: str) -> dict:
    return {key: load_table(cfg, key, backend) for key in TABLE_KEYS}


def summary_dict(tables: dict) -> dict:
    """First row of the pipeline summary as a plain dict ({} if the pipeline has not run)."""
    res = tables.get("summary")
    if res is None or not res.ok:
        return {}
    return {k: (None if pd.isna(v) else v) for k, v in res.df.iloc[0].to_dict().items()}
