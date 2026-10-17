"""Shared pytest setup: make ``src`` importable and provide a local Spark fixture."""
import os
import shutil
import sys
import time
from pathlib import Path

import pytest

# Spark returns timestamps to Python in the *process* time zone while the pipeline parses
# them in UTC. Pinning the process to UTC keeps wall-clock values identical in assertions.
os.environ["TZ"] = "UTC"
if hasattr(time, "tzset"):
    time.tzset()

ROOT = Path(__file__).resolve().parent.parent
for p in (ROOT, ROOT / "tests"):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))


@pytest.fixture(scope="session")
def spark():
    """A small local SparkSession. Tests that use it are skipped if PySpark or Java is missing."""
    pytest.importorskip("pyspark")
    if shutil.which("java") is None:
        pytest.skip("Java is not installed (required by Spark)")
    from src.config import load_config
    from src.spark_session import get_spark

    cfg = load_config().with_overrides(spark_master="local[2]", spark_driver_memory="1g")
    session = get_spark(cfg)
    session.conf.set("spark.sql.shuffle.partitions", "2")
    yield session
    session.stop()
