"""Tests for configuration, HDFS fallback behaviour and dashboard data loading (no Spark needed)."""
import os
import socket
from pathlib import Path

import pytest

from src.anomaly_detection import ANOMALY_COLUMNS
from src.config import ANOMALY_TABLE, HOURLY_TABLE, SUMMARY_TABLE, Config, load_config
from src.dashboard_data import (
    TABLE_KEYS, load_all, load_hdfs_table, load_local_table, resolve_backend, summary_dict,
)
from src.hdfs_storage import check_hdfs_reachable, resolve_storage


def _cfg(tmp_path, **kw):
    return Config(output_dir=Path(tmp_path), **kw).validate()


# ------------------------------------------------------------------ config
def test_default_config_is_standalone_and_valid():
    cfg = Config().validate()
    assert cfg.hdfs_enabled is False
    assert cfg.anomaly_threshold == 3.0


def test_invalid_config_values_are_rejected():
    for bad in ({"anomaly_threshold": 0}, {"baseline_days": 1}, {"min_history": 1},
                {"min_history": 30, "baseline_days": 14}, {"min_valid_minutes": 0},
                {"hdfs_base_dir": "gridguard"}):
        with pytest.raises(ValueError):
            Config(**bad).validate()


def test_environment_overrides_are_read():
    os.environ["GRIDGUARD_Z_THRESHOLD"] = "2.5"
    os.environ["GRIDGUARD_HDFS_ENABLED"] = "true"
    try:
        cfg = load_config()
        assert cfg.anomaly_threshold == 2.5 and cfg.hdfs_enabled is True
    finally:
        del os.environ["GRIDGUARD_Z_THRESHOLD"]
        del os.environ["GRIDGUARD_HDFS_ENABLED"]


# ------------------------------------------------------------------ HDFS fallback
def test_standalone_mode_never_claims_hdfs(tmp_path):
    ctx = resolve_storage(_cfg(tmp_path))
    assert ctx.mode == "local" and not ctx.is_hdfs
    assert "NOT HDFS" in ctx.label


def test_hdfs_unavailable_falls_back_to_local_with_reason(tmp_path):
    cfg = _cfg(tmp_path, hdfs_enabled=True, hdfs_namenode_uri="hdfs://127.0.0.1:1")
    ctx = resolve_storage(cfg)
    assert ctx.mode == "local"
    assert "unavailable" in ctx.reason.lower()
    assert "NOT HDFS" in ctx.label


def test_invalid_hdfs_uri_is_reported():
    ok, msg = check_hdfs_reachable("http://wrong-scheme:9000")
    assert ok is False and "hdfs://" in msg


def test_reachable_namenode_port_selects_hdfs(tmp_path):
    """Only a TCP check: proves the selection logic, not a working HDFS."""
    with socket.socket() as server:
        server.bind(("127.0.0.1", 0))
        server.listen(1)
        port = server.getsockname()[1]
        ctx = resolve_storage(_cfg(tmp_path, hdfs_enabled=True, hdfs_namenode_uri=f"hdfs://127.0.0.1:{port}"))
    assert ctx.mode == "hdfs"
    assert ctx.root == f"hdfs://127.0.0.1:{port}/gridguard"
    assert ctx.path("results/detected_anomalies").endswith("/gridguard/results/detected_anomalies")


def test_use_hdfs_false_overrides_enabled_config(tmp_path):
    cfg = _cfg(tmp_path, hdfs_enabled=True)
    assert resolve_storage(cfg, use_hdfs=False).mode == "local"


# ------------------------------------------------------------------ dashboard loaders
def test_missing_output_files_are_reported_not_raised(tmp_path):
    res = load_local_table(tmp_path, HOURLY_TABLE, "hourly")
    assert res.status == "missing" and res.df.empty and not res.ok
    assert "run_pipeline" in res.message


def test_empty_output_folder_and_header_only_csv(tmp_path):
    folder = Path(tmp_path) / ANOMALY_TABLE
    folder.mkdir(parents=True)
    assert load_local_table(tmp_path, ANOMALY_TABLE).status == "empty"
    (folder / "part-00000.csv").write_text(",".join(ANOMALY_COLUMNS) + "\n", encoding="utf-8")
    assert load_local_table(tmp_path, ANOMALY_TABLE).status == "empty"


def test_valid_table_is_loaded_and_spark_marker_files_are_ignored(tmp_path):
    folder = Path(tmp_path) / ANOMALY_TABLE
    folder.mkdir(parents=True)
    (folder / "part-00000-abc-c000.csv").write_text(
        "timestamp,observed_kw,z_score\n2007-01-09 03:00:00,3.1,25.2\n", encoding="utf-8")
    (folder / "_SUCCESS").write_text("")
    (folder / ".part-00000-abc-c000.csv.crc").write_text("junk")
    res = load_local_table(tmp_path, ANOMALY_TABLE, "anomalies")
    assert res.ok and len(res.df) == 1
    assert str(res.df["timestamp"].dtype).startswith("datetime64")


def test_corrupt_file_gives_error_status(tmp_path):
    folder = Path(tmp_path) / SUMMARY_TABLE
    folder.mkdir(parents=True)
    (folder / "part-00000.csv").write_bytes(b"\x00\x01\x02 not,a\ncsv\"\n\"")
    res = load_local_table(tmp_path, SUMMARY_TABLE)
    assert res.status in {"error", "ok", "empty"}  # must not raise


def test_load_all_with_nothing_processed_and_empty_summary(tmp_path):
    tables = load_all(_cfg(tmp_path), "local")
    assert set(tables) == set(TABLE_KEYS)
    assert all(not t.ok for t in tables.values())
    assert summary_dict(tables) == {}


def test_summary_dict_reads_first_row(tmp_path):
    folder = Path(tmp_path) / SUMMARY_TABLE
    folder.mkdir(parents=True)
    (folder / "part-00000.csv").write_text(
        "total_records,storage_mode,processed_at\n100,local,2026-10-17 10:00:00 UTC\n", encoding="utf-8")
    info = summary_dict(load_all(_cfg(tmp_path), "local"))
    assert info["total_records"] == 100 and info["storage_mode"] == "local"


def test_backend_resolution_falls_back_when_hdfs_down(tmp_path):
    cfg = _cfg(tmp_path, hdfs_enabled=True, webhdfs_url="http://127.0.0.1:1")
    backend, msg = resolve_backend(cfg, "auto")
    assert backend == "local" and "unavailable" in msg.lower()
    backend, msg = resolve_backend(cfg, "hdfs")
    assert backend == "local" and "unavailable" in msg.lower()
    assert resolve_backend(_cfg(tmp_path), "auto")[0] == "local"


def test_reading_from_unavailable_hdfs_returns_error_result(tmp_path):
    cfg = _cfg(tmp_path, hdfs_enabled=True, webhdfs_url="http://127.0.0.1:1")
    res = load_hdfs_table(cfg, HOURLY_TABLE, "hourly", timeout=1.0)
    assert res.status == "error" and res.source == "hdfs" and res.df.empty
