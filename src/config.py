"""Central configuration for GridGuard.

All settings can be overridden with environment variables (or a local ``.env``
file, see ``.env.example``). Nothing secret is stored here.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, replace
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

# ---------------------------------------------------------------------------
# Relative table locations. They are identical under the HDFS base directory
# (/gridguard/<rel>) and under the local output directory (outputs/<rel>).
# ---------------------------------------------------------------------------
RAW_DIR = "raw"
HOURLY_TABLE = "processed/hourly_consumption"
DAILY_TABLE = "processed/daily_consumption"          # additional (not in the brief)
SUMMARY_TABLE = "processed/pipeline_summary"          # additional (not in the brief)
ANOMALY_TABLE = "results/detected_anomalies"
PEAK_TOP_TABLE = "results/peak_analysis/top_periods"
PEAK_FREQ_TABLE = "results/peak_analysis/peak_hour_frequency"

ALL_OUTPUT_TABLES = [
    HOURLY_TABLE, DAILY_TABLE, SUMMARY_TABLE,
    ANOMALY_TABLE, PEAK_TOP_TABLE, PEAK_FREQ_TABLE,
]
HDFS_DIRECTORIES = [RAW_DIR] + ALL_OUTPUT_TABLES


def _load_dotenv(path: Path) -> None:
    """Tiny .env reader (KEY=VALUE lines). Existing environment variables win."""
    if not path.is_file():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_num(name: str, default, cast):
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return cast(raw)
    except ValueError as exc:
        raise ValueError(
            f"Environment variable {name}={raw!r} is not a valid {cast.__name__}"
        ) from exc


def _env_path(name: str, default: Path) -> Path:
    raw = os.environ.get(name)
    if not raw:
        return default
    p = Path(raw).expanduser()
    return p if p.is_absolute() else (PROJECT_ROOT / p)


@dataclass(frozen=True)
class Config:
    # --- paths -------------------------------------------------------------
    dataset_path: Path = PROJECT_ROOT / "data" / "household_power_consumption.txt"
    sample_path: Path = PROJECT_ROOT / "data" / "sample_electricity.csv"
    output_dir: Path = PROJECT_ROOT / "outputs"
    # --- HDFS --------------------------------------------------------------
    hdfs_enabled: bool = False
    hdfs_namenode_uri: str = "hdfs://localhost:8020"
    hdfs_base_dir: str = "/gridguard"
    hdfs_user: str = "hadoop"
    webhdfs_url: str = "http://localhost:9870"
    # --- anomaly detection ---------------------------------------------------
    anomaly_threshold: float = 3.0
    baseline_days: int = 14
    min_history: int = 5
    min_valid_minutes: int = 30
    baseline_by_day_type: bool = False
    top_n_peaks: int = 50
    # --- Spark -------------------------------------------------------------
    spark_app_name: str = "GridGuard"
    spark_master: str = "local[*]"
    spark_driver_memory: str = "2g"

    def validate(self) -> "Config":
        if self.anomaly_threshold <= 0:
            raise ValueError("anomaly_threshold must be > 0")
        if self.baseline_days < 2:
            raise ValueError("baseline_days must be >= 2")
        if self.min_history < 2:
            raise ValueError("min_history must be >= 2 (a standard deviation needs two points)")
        if self.min_history > self.baseline_days:
            raise ValueError("min_history cannot exceed baseline_days")
        if not (1 <= self.min_valid_minutes <= 60):
            raise ValueError("min_valid_minutes must be between 1 and 60")
        if not self.hdfs_base_dir.startswith("/"):
            raise ValueError("hdfs_base_dir must be an absolute HDFS path such as /gridguard")
        return self

    def with_overrides(self, **kwargs) -> "Config":
        clean = {k: v for k, v in kwargs.items() if v is not None}
        return replace(self, **clean).validate()


def load_config(**overrides) -> Config:
    """Build a Config from environment variables, then apply keyword overrides."""
    _load_dotenv(PROJECT_ROOT / ".env")
    d = Config()
    cfg = Config(
        dataset_path=_env_path("GRIDGUARD_DATASET_PATH", d.dataset_path),
        sample_path=_env_path("GRIDGUARD_SAMPLE_PATH", d.sample_path),
        output_dir=_env_path("GRIDGUARD_OUTPUT_DIR", d.output_dir),
        hdfs_enabled=_env_bool("GRIDGUARD_HDFS_ENABLED", d.hdfs_enabled),
        hdfs_namenode_uri=os.environ.get("GRIDGUARD_HDFS_URI", d.hdfs_namenode_uri).rstrip("/"),
        hdfs_base_dir="/" + os.environ.get("GRIDGUARD_HDFS_BASE_DIR", d.hdfs_base_dir).strip("/"),
        hdfs_user=os.environ.get("GRIDGUARD_HDFS_USER", d.hdfs_user),
        webhdfs_url=os.environ.get("GRIDGUARD_WEBHDFS_URL", d.webhdfs_url).rstrip("/"),
        anomaly_threshold=_env_num("GRIDGUARD_Z_THRESHOLD", d.anomaly_threshold, float),
        baseline_days=_env_num("GRIDGUARD_BASELINE_DAYS", d.baseline_days, int),
        min_history=_env_num("GRIDGUARD_MIN_HISTORY", d.min_history, int),
        min_valid_minutes=_env_num("GRIDGUARD_MIN_VALID_MINUTES", d.min_valid_minutes, int),
        baseline_by_day_type=_env_bool("GRIDGUARD_BASELINE_BY_DAY_TYPE", d.baseline_by_day_type),
        top_n_peaks=_env_num("GRIDGUARD_TOP_N_PEAKS", d.top_n_peaks, int),
        spark_app_name=os.environ.get("GRIDGUARD_SPARK_APP_NAME", d.spark_app_name),
        spark_master=os.environ.get("GRIDGUARD_SPARK_MASTER", d.spark_master),
        spark_driver_memory=os.environ.get("GRIDGUARD_SPARK_DRIVER_MEMORY", d.spark_driver_memory),
    )
    return cfg.with_overrides(**overrides) if overrides else cfg.validate()
