"""GridGuard batch pipeline.

    Raw CSV -> (HDFS raw) -> PySpark cleaning -> hourly/daily aggregation
            -> rolling z-score anomaly detection -> peak analysis
            -> HDFS (or local) processed/result tables -> Streamlit dashboard

Usage examples
--------------
    python run_pipeline.py                      # standalone mode, full UCI dataset
    python run_pipeline.py --sample             # tiny synthetic test file
    python run_pipeline.py --hdfs               # real HDFS (falls back to local if unreachable)
    python run_pipeline.py --threshold 2.5 --baseline-days 21
"""
from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from src.config import (
    ANOMALY_TABLE, DAILY_TABLE, HOURLY_TABLE, PEAK_FREQ_TABLE, PEAK_TOP_TABLE,
    SUMMARY_TABLE, Config, load_config,
)

SUMMARY_SCHEMA = (
    "total_records long, invalid_timestamp_rows long, missing_gap_rows long, "
    "invalid_gap_rows long, valid_gap_rows long, hourly_rows long, evaluated_hours long, "
    "anomaly_count long, anomaly_threshold double, baseline_days int, min_history int, "
    "min_valid_minutes int, baseline_by_day_type boolean, data_start string, data_end string, "
    "source_file string, storage_mode string, copy_location string, storage_root string, "
    "processed_at string"
)


def build_summary_df(spark, info: dict, copy_location: str):
    row = dict(info, copy_location=copy_location)
    order = [c.split()[0] for c in SUMMARY_SCHEMA.split(", ")]
    return spark.createDataFrame([tuple(row[c] for c in order)], schema=SUMMARY_SCHEMA)


def run(cfg: Config, use_hdfs: bool | None = None, input_path: str | None = None, log=print) -> dict:
    from src import analytics, anomaly_detection, hdfs_storage, preprocessing
    from src.spark_session import get_spark
    from pyspark.sql import functions as F

    t0 = time.time()
    ctx = hdfs_storage.resolve_storage(cfg, use_hdfs)
    log(f"[1/8] Storage backend : {ctx.label}")
    log(f"      reason          : {ctx.reason}")

    spark = get_spark(cfg, use_hdfs=ctx.is_hdfs)
    log(f"[2/8] Spark {spark.version} started (master={cfg.spark_master})")

    created = hdfs_storage.ensure_directories(ctx, spark)
    log(f"[3/8] Directories ready ({len(created)})")

    source = input_path or str(cfg.dataset_path)
    read_path = hdfs_storage.upload_raw(ctx, spark, source)
    log(f"[4/8] Raw data path   : {read_path}")

    parsed = preprocessing.parse_raw(preprocessing.read_raw(spark, read_path)).persist()
    quality = preprocessing.data_quality_summary(parsed)
    clean = preprocessing.finalize_clean(parsed)
    log(f"[5/8] Data quality    : {quality}")

    hourly = analytics.hourly_aggregate(clean, cfg.min_valid_minutes)
    scored = anomaly_detection.detect_anomalies(
        hourly, cfg.anomaly_threshold, cfg.baseline_days, cfg.min_history, cfg.baseline_by_day_type
    ).persist()
    stats = scored.agg(
        F.count(F.lit(1)).alias("hourly_rows"),
        F.sum(F.when(F.col("baseline_status") == "ok", 1).otherwise(0)).alias("evaluated_hours"),
        F.sum(F.col("is_anomaly").cast("int")).alias("anomaly_count"),
        F.min("timestamp").alias("data_start"),
        F.max("timestamp").alias("data_end"),
    ).collect()[0]
    log(f"[6/8] Scored {stats['hourly_rows']} hourly rows; "
        f"{stats['anomaly_count'] or 0} anomalies among {stats['evaluated_hours'] or 0} evaluated hours")

    daily = analytics.daily_aggregate(scored)
    anomalies = anomaly_detection.extract_anomalies(scored)
    peak_top = analytics.top_peak_periods(scored, cfg.top_n_peaks)
    peak_freq = analytics.peak_hour_frequency(scored)

    info = {
        **quality,
        "hourly_rows": int(stats["hourly_rows"] or 0),
        "evaluated_hours": int(stats["evaluated_hours"] or 0),
        "anomaly_count": int(stats["anomaly_count"] or 0),
        "anomaly_threshold": float(cfg.anomaly_threshold),
        "baseline_days": int(cfg.baseline_days),
        "min_history": int(cfg.min_history),
        "min_valid_minutes": int(cfg.min_valid_minutes),
        "baseline_by_day_type": bool(cfg.baseline_by_day_type),
        "data_start": str(stats["data_start"] or ""),
        "data_end": str(stats["data_end"] or ""),
        "source_file": str(source),
        "storage_mode": ctx.mode,
        "storage_root": ctx.root,
        "processed_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
    }

    tables = [
        (HOURLY_TABLE, scored), (DAILY_TABLE, daily), (ANOMALY_TABLE, anomalies),
        (PEAK_TOP_TABLE, peak_top), (PEAK_FREQ_TABLE, peak_freq),
    ]
    contexts = [(ctx, ctx.mode)]
    if ctx.is_hdfs:
        # The dashboard can read this local export without WebHDFS/DataNode networking.
        contexts.append((hdfs_storage.local_context(cfg), "local_export_of_hdfs"))

    for target, copy_location in contexts:
        if not target.is_hdfs and target is not ctx:
            hdfs_storage.ensure_directories(target, spark)
        for rel, df in tables:
            hdfs_storage.write_table(target, df, rel)
        hdfs_storage.write_table(target, build_summary_df(spark, info, copy_location), SUMMARY_TABLE)
        log(f"[7/8] Wrote results to {target.label}")

    log("[8/8] Verifying outputs")
    report = {}
    for target, _ in contexts:
        for rel, _df in tables + [(SUMMARY_TABLE, None)]:
            r = hdfs_storage.verify_table(target, spark, rel)
            report[f"{target.mode}:{rel}"] = r
            log(f"      {'OK ' if r['ok'] else 'FAIL'} {r['path']} ({r['total_bytes']} bytes)")
    scored.unpersist()
    parsed.unpersist()
    log(f"Done in {time.time() - t0:.1f}s")
    return {"info": info, "verification": report, "storage": ctx}


def parse_args(argv=None):
    p = argparse.ArgumentParser(description="Run the GridGuard PySpark pipeline")
    p.add_argument("--input", help="path to the raw semicolon-separated file")
    p.add_argument("--sample", action="store_true", help="use data/sample_electricity.csv (synthetic test data)")
    g = p.add_mutually_exclusive_group()
    g.add_argument("--hdfs", action="store_true", help="use HDFS (falls back to local if unreachable)")
    g.add_argument("--local", action="store_true", help="force standalone local mode")
    p.add_argument("--output-dir")
    p.add_argument("--threshold", type=float, help="|z| threshold (default 3.0)")
    p.add_argument("--baseline-days", type=int)
    p.add_argument("--min-history", type=int)
    return p.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    cfg = load_config().with_overrides(
        output_dir=Path(args.output_dir).resolve() if args.output_dir else None,
        anomaly_threshold=args.threshold,
        baseline_days=args.baseline_days,
        min_history=args.min_history,
    )
    use_hdfs = True if args.hdfs else False if args.local else None
    source = args.input or str(cfg.sample_path if args.sample else cfg.dataset_path)
    try:
        run(cfg, use_hdfs=use_hdfs, input_path=source)
    except FileNotFoundError as exc:
        print(f"\nERROR: {exc}\nDownload the dataset first with `python data_download.py` "
              f"or place the file at {cfg.dataset_path}.", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
