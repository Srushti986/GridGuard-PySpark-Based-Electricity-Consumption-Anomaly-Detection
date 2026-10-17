"""Tests for the z-score anomaly logic.

* The pure-Python tests run anywhere (no Spark needed).
* The Spark tests use the ``spark`` fixture and are skipped if PySpark/Java is missing.
  They check that the Spark implementation agrees with the pure-Python reference.
"""
import math
from collections import defaultdict
from datetime import date, datetime, timedelta

import pytest

from generate_sample_data import generate_lines
from src.anomaly_detection import ANOMALY_COLUMNS, score_value, severity_for

HISTORY = [1.0, 1.1, 0.9, 1.0, 1.05, 0.95]


# ------------------------------------------------------------------ pure Python
def test_high_anomaly_detected():
    r = score_value(5.0, HISTORY, threshold=3.0, min_history=5)
    assert r["baseline_status"] == "ok"
    assert r["is_anomaly"] is True
    assert r["anomaly_direction"] == "High"
    assert r["z_score"] > 3
    assert r["severity"] == "Extreme"


def test_low_anomaly_detected():
    r = score_value(0.05, HISTORY, threshold=3.0, min_history=5)
    assert r["is_anomaly"] is True
    assert r["anomaly_direction"] == "Low"
    assert r["z_score"] < -3


def test_normal_value_is_not_flagged():
    r = score_value(1.02, HISTORY)
    assert r["is_anomaly"] is False
    assert r["anomaly_direction"] == "Normal"
    assert r["severity"] == "None"


def test_z_score_formula():
    r = score_value(2.0, [1.0, 2.0, 3.0, 4.0, 5.0], min_history=5)
    # mean = 3, sample std = sqrt(2.5)
    assert r["baseline_mean_kw"] == pytest.approx(3.0)
    assert r["baseline_std_kw"] == pytest.approx(math.sqrt(2.5))
    assert r["z_score"] == pytest.approx((2.0 - 3.0) / math.sqrt(2.5))


def test_constant_baseline_is_zero_variance_not_a_crash():
    r = score_value(3.0, [1.0] * 8)
    assert r["baseline_status"] == "zero_variance"
    assert r["z_score"] is None
    assert r["is_anomaly"] is False
    assert r["anomaly_direction"] == "Not evaluated"


def test_insufficient_history():
    r = score_value(5.0, [1.0, 1.2, 0.9], min_history=5)
    assert r["baseline_status"] == "insufficient_history"
    assert r["z_score"] is None and r["is_anomaly"] is False


def test_empty_history():
    r = score_value(1.0, [])
    assert r["history_count"] == 0
    assert r["baseline_status"] == "insufficient_history"
    assert r["baseline_mean_kw"] is None


def test_missing_negative_or_nan_observation():
    for bad in (None, -1.0, float("nan"), float("inf")):
        r = score_value(bad, HISTORY)
        assert r["baseline_status"] == "missing_observation"
        assert r["is_anomaly"] is False


def test_invalid_history_values_are_ignored():
    r = score_value(5.0, HISTORY + [None, -3.0, float("nan")])
    assert r["history_count"] == len(HISTORY)
    assert r["is_anomaly"] is True


def test_threshold_is_configurable():
    mild = score_value(1.15, HISTORY, threshold=3.0)
    strict = score_value(1.15, HISTORY, threshold=1.0)
    assert mild["is_anomaly"] is False
    assert strict["is_anomaly"] is True


def test_severity_bands_are_relative_to_threshold():
    assert severity_for(2.9, 3.0) == "None"
    assert severity_for(3.0, 3.0) == "Moderate"
    assert severity_for(4.0, 3.0) == "Strong"
    assert severity_for(5.0, 3.0) == "Extreme"


def test_output_columns_contain_required_fields():
    for col in ("timestamp", "observed_kw", "baseline_mean_kw", "baseline_std_kw",
                "z_score", "anomaly_direction", "severity", "explanation"):
        assert col in ANOMALY_COLUMNS


def test_injected_anomalies_in_synthetic_sample_are_found_by_reference_logic():
    """End-to-end check of the method on the synthetic sample (pure Python, no leakage)."""
    sums, counts = defaultdict(float), defaultdict(int)
    for line in generate_lines()[1:]:
        parts = line.split(";")
        try:
            ts = datetime.strptime(parts[0] + " " + parts[1], "%d/%m/%Y %H:%M:%S")
            gap = float(parts[2])
        except ValueError:
            continue
        if gap < 0:
            continue
        key = ts.replace(minute=0, second=0)
        sums[key] += gap
        counts[key] += 1
    hourly = {k: sums[k] / counts[k] for k in sums if counts[k] >= 30}

    flagged = {}
    for ts, value in hourly.items():
        history = [hourly[ts - timedelta(days=d)] for d in range(1, 15) if (ts - timedelta(days=d)) in hourly]
        r = score_value(value, history, threshold=3.0, min_history=5)
        if r["is_anomaly"]:
            flagged[ts] = r["anomaly_direction"]
    assert flagged.get(datetime(2007, 1, 9, 3)) == "High"    # injected spike (day index 8, hour 3)
    assert flagged.get(datetime(2007, 1, 10, 19)) == "Low"   # injected drop  (day index 9, hour 19)


# ------------------------------------------------------------------ Spark vs reference
def _hourly_frame(spark, rows):
    """rows: list of (date, hour_of_day, observed_kw or None)."""
    from pyspark.sql.types import (BooleanType, DateType, DoubleType, IntegerType,
                                   StringType, StructField, StructType, TimestampType)
    schema = StructType([
        StructField("timestamp", TimestampType()), StructField("date", DateType()),
        StructField("hour_of_day", IntegerType()), StructField("day_name", StringType()),
        StructField("is_weekend", BooleanType()), StructField("observed_kw", DoubleType()),
    ])
    data = [(datetime(d.year, d.month, d.day, h), d, h, d.strftime("%a"), d.weekday() >= 5, v)
            for d, h, v in rows]
    return spark.createDataFrame(data, schema)


def test_spark_matches_reference_and_has_no_lookahead(spark):
    from src.anomaly_detection import detect_anomalies
    values = [1.0, 1.1, 0.9, 1.0, 1.05, 0.95, 5.0, 1.0, 0.05]
    d0 = date(2007, 1, 1)
    rows = [(d0 + timedelta(days=i), 0, v) for i, v in enumerate(values)]
    out = detect_anomalies(_hourly_frame(spark, rows), threshold=3.0, baseline_days=14, min_history=5)
    got = {r["date"]: r for r in out.collect()}
    for i, v in enumerate(values):
        expected = score_value(v, values[:i], threshold=3.0, min_history=5)  # strictly PAST values only
        row = got[d0 + timedelta(days=i)]
        assert row["history_count"] == expected["history_count"]
        assert row["baseline_status"] == expected["baseline_status"]
        assert row["is_anomaly"] == expected["is_anomaly"]
        if expected["z_score"] is None:
            assert row["z_score"] is None
        else:
            assert row["z_score"] == pytest.approx(expected["z_score"], abs=1e-3)
    assert got[d0 + timedelta(days=6)]["anomaly_direction"] == "High"
    assert "unusually high" in got[d0 + timedelta(days=6)]["explanation"]


def test_spark_constant_baseline_and_missing_values(spark):
    from src.anomaly_detection import detect_anomalies
    d0 = date(2007, 1, 1)
    rows = [(d0 + timedelta(days=i), 0, 1.0) for i in range(7)]
    rows.append((d0 + timedelta(days=7), 0, 3.0))   # constant history -> undefined z
    rows.append((d0 + timedelta(days=8), 0, None))  # missing observation
    out = {r["date"]: r for r in detect_anomalies(_hourly_frame(spark, rows), min_history=5).collect()}
    assert out[d0 + timedelta(days=7)]["baseline_status"] == "zero_variance"
    assert out[d0 + timedelta(days=7)]["z_score"] is None
    assert out[d0 + timedelta(days=7)]["is_anomaly"] is False
    assert out[d0 + timedelta(days=8)]["baseline_status"] == "missing_observation"


def test_spark_baseline_window_ignores_old_days(spark):
    from src.anomaly_detection import detect_anomalies
    d0 = date(2007, 1, 1)
    rows = [(d0 + timedelta(days=i), 5, 1.0 + 0.1 * i) for i in range(6)]
    rows.append((d0 + timedelta(days=60), 5, 1.0))  # all previous rows are > 14 days old
    out = {r["date"]: r for r in detect_anomalies(_hourly_frame(spark, rows), baseline_days=14).collect()}
    assert out[d0 + timedelta(days=60)]["history_count"] == 0


def test_spark_empty_dataset(spark):
    from src.anomaly_detection import detect_anomalies, extract_anomalies
    out = detect_anomalies(_hourly_frame(spark, []))
    assert out.count() == 0
    assert extract_anomalies(out).count() == 0
    assert "z_score" in out.columns and "explanation" in out.columns


def test_spark_extract_anomalies_schema_and_order(spark):
    from src.anomaly_detection import detect_anomalies, extract_anomalies
    d0 = date(2007, 1, 1)
    values = [1.0, 1.1, 0.9, 1.0, 1.05, 0.95, 5.0, 0.0]
    rows = [(d0 + timedelta(days=i), 0, v) for i, v in enumerate(values)]
    flagged = extract_anomalies(detect_anomalies(_hourly_frame(spark, rows)))
    assert flagged.columns == ANOMALY_COLUMNS
    zs = [r["abs_z_score"] for r in flagged.collect()]
    assert zs == sorted(zs, reverse=True) and len(zs) >= 1
