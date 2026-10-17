"""Tests for reading, cleaning and hourly aggregation.

Tests that take the ``spark`` fixture need PySpark + Java and are skipped otherwise.
"""
from datetime import datetime
from pathlib import Path

import pytest

from generate_sample_data import HEADER, generate_lines

ROOT = Path(__file__).resolve().parent.parent
SAMPLE = ROOT / "data" / "sample_electricity.csv"


def _write(tmp_path, rows, name="raw.txt"):
    p = Path(tmp_path) / name
    p.write_text("\n".join([HEADER] + rows) + "\n", encoding="utf-8")
    return str(p)


# ------------------------------------------------------------------ pure Python
def test_sample_file_exists_with_uci_header():
    assert SAMPLE.is_file()
    assert SAMPLE.read_text(encoding="utf-8").splitlines()[0] == HEADER


def test_sample_file_is_reproducible_from_generator():
    assert SAMPLE.read_text(encoding="utf-8").splitlines() == generate_lines()


def test_sample_contains_missing_and_invalid_rows():
    text = SAMPLE.read_text(encoding="utf-8")
    assert ";?;?;?;" in text          # '?' missing markers like the real dataset
    assert ";-4.000;" in text         # deliberately negative reading
    assert "not-a-date" in text       # deliberately unparseable timestamp


def test_sample_dates_use_day_first_format():
    first = SAMPLE.read_text(encoding="utf-8").splitlines()[1].split(";")
    assert datetime.strptime(first[0] + " " + first[1], "%d/%m/%Y %H:%M:%S") == datetime(2007, 1, 1, 0, 0)


# ------------------------------------------------------------------ Spark
def test_timestamp_parsing_day_first(spark, tmp_path):
    from src.preprocessing import clean_raw, read_raw
    path = _write(tmp_path, ["16/12/2006;17:24:00;4.216;0.418;234.84;18.4;0;1;17",
                             "1/2/2007;00:05:00;1.000;0.100;240.0;4.2;0;0;0"])
    rows = {r["timestamp"] for r in clean_raw(read_raw(spark, path)).collect()}
    assert rows == {datetime(2006, 12, 16, 17, 24), datetime(2007, 2, 1, 0, 5)}


def test_missing_markers_become_null_not_zero(spark, tmp_path):
    from src.preprocessing import clean_raw, read_raw
    path = _write(tmp_path, ["16/12/2006;17:24:00;?;?;?;?;?;?;",
                             "16/12/2006;17:25:00;2.500;0.1;240.0;10.0;0;0;0"])
    out = {r["timestamp"]: r for r in clean_raw(read_raw(spark, path)).collect()}
    bad = out[datetime(2006, 12, 16, 17, 24)]
    assert bad["global_active_power"] is None and bad["voltage"] is None and bad["sub_metering_3"] is None
    assert out[datetime(2006, 12, 16, 17, 25)]["global_active_power"] == pytest.approx(2.5)


def test_negative_nan_and_text_values_are_invalid(spark, tmp_path):
    from src.preprocessing import clean_raw, data_quality_summary, parse_raw, read_raw
    path = _write(tmp_path, ["1/1/2007;00:00:00;-1.5;0.1;240;1;0;0;0",
                             "1/1/2007;00:01:00;abc;0.1;240;1;0;0;0",
                             "1/1/2007;00:02:00;NaN;0.1;240;1;0;0;0",
                             "1/1/2007;00:03:00;1.5;0.1;240;1;0;0;0"])
    parsed = parse_raw(read_raw(spark, path))
    q = data_quality_summary(parsed)
    assert q["total_records"] == 4
    assert q["valid_gap_rows"] == 1
    assert q["missing_gap_rows"] == 1      # 'abc' cannot be parsed -> treated as missing
    assert q["invalid_gap_rows"] == 2      # -1.5 and NaN parse but are not valid readings
    valid = [r["global_active_power"] for r in clean_raw(read_raw(spark, path)).collect()
             if r["global_active_power"] is not None]
    assert valid == [pytest.approx(1.5)]


def test_unparseable_timestamps_and_duplicates_are_dropped(spark, tmp_path):
    from src.preprocessing import clean_raw, data_quality_summary, parse_raw, read_raw
    path = _write(tmp_path, ["bad;99:99:99;1.0;0.1;240;1;0;0;0",
                             "1/1/2007;00:00:00;1.0;0.1;240;1;0;0;0",
                             "1/1/2007;00:00:00;1.0;0.1;240;1;0;0;0"])
    assert data_quality_summary(parse_raw(read_raw(spark, path)))["invalid_timestamp_rows"] == 1
    assert clean_raw(read_raw(spark, path)).count() == 1


def test_empty_dataset_header_only(spark, tmp_path):
    from src.analytics import hourly_aggregate
    from src.preprocessing import clean_raw, data_quality_summary, parse_raw, read_raw
    path = _write(tmp_path, [])
    assert data_quality_summary(parse_raw(read_raw(spark, path)))["total_records"] == 0
    clean = clean_raw(read_raw(spark, path))
    assert clean.count() == 0
    assert hourly_aggregate(clean).count() == 0


def test_hourly_aggregation_requires_enough_valid_minutes(spark, tmp_path):
    from src.analytics import HOURLY_COLUMNS, hourly_aggregate
    from src.preprocessing import clean_raw, read_raw
    good = [f"1/1/2007;00:{m:02d}:00;2.0;0.1;240;1;0;0;0" for m in range(40)]
    sparse = [f"1/1/2007;01:{m:02d}:00;{'?;?;?;?;?;?;?' if m >= 5 else '2.0;0.1;240;1;0;0;0'}" for m in range(40)]
    out = {r["hour_of_day"]: r for r in hourly_aggregate(
        clean_raw(read_raw(spark, _write(tmp_path, good + sparse))), min_valid_minutes=30).collect()}
    assert out[0]["observed_kw"] == pytest.approx(2.0) and out[0]["valid_readings"] == 40
    assert out[1]["observed_kw"] is None and out[1]["valid_readings"] == 5
    assert hourly_aggregate(clean_raw(read_raw(spark, _write(tmp_path, good, "b.txt")))).columns == HOURLY_COLUMNS


def test_pipeline_on_synthetic_sample_finds_injected_anomalies(spark):
    from src.analytics import daily_aggregate, hourly_aggregate, peak_hour_frequency, top_peak_periods
    from src.anomaly_detection import detect_anomalies, extract_anomalies
    from src.preprocessing import clean_raw, read_raw
    hourly = hourly_aggregate(clean_raw(read_raw(spark, str(SAMPLE))), 30)
    scored = detect_anomalies(hourly, 3.0, 14, 5)
    flagged = {r["timestamp"]: r["anomaly_direction"] for r in extract_anomalies(scored).collect()}
    assert flagged[datetime(2007, 1, 9, 3)] == "High"
    assert flagged[datetime(2007, 1, 10, 19)] == "Low"
    assert daily_aggregate(scored).count() >= 10
    assert top_peak_periods(scored, 5).count() == 5
    assert peak_hour_frequency(scored).count() >= 1
