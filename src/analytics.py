"""Hourly / daily aggregation and peak-demand analysis with PySpark."""
from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

HOURLY_COLUMNS = [
    "timestamp", "date", "hour_of_day", "day_of_week", "day_name", "is_weekend",
    "observed_kw", "max_minute_kw", "min_minute_kw", "valid_readings", "total_readings",
    "avg_voltage",
]


def hourly_aggregate(clean_df: DataFrame, min_valid_minutes: int = 30) -> DataFrame:
    """Minute readings -> one row per hour.

    ``observed_kw`` is the mean Global_active_power (kW) of the valid minutes in that
    hour. If fewer than ``min_valid_minutes`` valid minutes exist the hour is not
    trusted and ``observed_kw`` is NULL (it is then never scored or used in baselines).
    """
    hour = clean_df.withColumn("hour_start", F.date_trunc("hour", F.col("timestamp")))
    agg = hour.groupBy("hour_start").agg(
        F.count(F.lit(1)).alias("total_readings"),
        F.count("global_active_power").alias("valid_readings"),
        F.avg("global_active_power").alias("_avg_kw"),
        F.max("global_active_power").alias("max_minute_kw"),
        F.min("global_active_power").alias("min_minute_kw"),
        F.avg("voltage").alias("avg_voltage"),
    )
    out = (
        agg.withColumnRenamed("hour_start", "timestamp")
        .withColumn(
            "observed_kw",
            F.when(F.col("valid_readings") >= min_valid_minutes, F.round(F.col("_avg_kw"), 4)),
        )
        .drop("_avg_kw")
        .withColumn("date", F.to_date("timestamp"))
        .withColumn("hour_of_day", F.hour("timestamp"))
        .withColumn("day_of_week", F.dayofweek("timestamp"))  # 1 = Sunday ... 7 = Saturday
        .withColumn("day_name", F.date_format("timestamp", "E"))
        .withColumn("is_weekend", F.dayofweek("timestamp").isin(1, 7))
        .withColumn("max_minute_kw", F.round("max_minute_kw", 4))
        .withColumn("min_minute_kw", F.round("min_minute_kw", 4))
        .withColumn("avg_voltage", F.round("avg_voltage", 3))
    )
    return out.select(*HOURLY_COLUMNS).orderBy("timestamp")


def daily_aggregate(scored_df: DataFrame) -> DataFrame:
    """Hourly (scored) -> one row per day.

    ``energy_kwh`` is the sum of hourly mean kW over hours with valid data (kW x 1 h).
    Days with missing hours therefore under-state energy; ``hours_with_data`` shows this.
    """
    return (
        scored_df.groupBy("date")
        .agg(
            F.round(F.avg("observed_kw"), 4).alias("avg_kw"),
            F.round(F.max("observed_kw"), 4).alias("peak_hourly_kw"),
            F.round(F.min("observed_kw"), 4).alias("min_hourly_kw"),
            F.round(F.sum("observed_kw"), 3).alias("energy_kwh"),
            F.count("observed_kw").alias("hours_with_data"),
            F.sum(F.col("is_anomaly").cast("int")).alias("anomaly_count"),
        )
        .withColumn("day_name", F.date_format("date", "E"))
        .orderBy("date")
    )


def top_peak_periods(scored_df: DataFrame, top_n: int = 50) -> DataFrame:
    """Highest hourly demand periods with their historical baseline for comparison."""
    obs, mean = F.col("observed_kw"), F.col("baseline_mean_kw")
    return (
        scored_df.filter(obs.isNotNull())
        .orderBy(F.desc("observed_kw"))
        .limit(int(top_n))
        .select(
            "timestamp", "date", "hour_of_day", "day_name", "observed_kw",
            "baseline_mean_kw", "baseline_std_kw", "history_count", "z_score",
            F.round(obs - mean, 4).alias("diff_from_baseline_kw"),
            F.round(F.when(mean > 0, (obs - mean) / mean * 100), 2).alias("diff_from_baseline_pct"),
        )
        .orderBy(F.desc("observed_kw"))
    )


def peak_hour_frequency(scored_df: DataFrame) -> DataFrame:
    """For each hour-of-day: on how many days was it the single highest-demand hour?

    This identifies *recurring* peak-demand periods.
    """
    w = Window.partitionBy("date").orderBy(F.desc("observed_kw"), F.asc("hour_of_day"))
    daily_peaks = (
        scored_df.filter(F.col("observed_kw").isNotNull())
        .withColumn("_rank", F.row_number().over(w))
        .filter(F.col("_rank") == 1)
    )
    total_days = daily_peaks.count()
    freq = daily_peaks.groupBy("hour_of_day").agg(
        F.count(F.lit(1)).alias("days_as_daily_peak"),
        F.round(F.avg("observed_kw"), 4).alias("avg_peak_kw"),
    )
    share = (
        F.round(F.col("days_as_daily_peak") * 100.0 / F.lit(total_days), 2)
        if total_days > 0 else F.lit(None).cast("double")
    )
    return freq.withColumn("share_of_days_pct", share).orderBy(F.desc("days_as_daily_peak"))
