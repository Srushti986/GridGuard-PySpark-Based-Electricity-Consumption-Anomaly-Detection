"""Reading and cleaning the UCI household power consumption data with PySpark.

Source file layout (semicolon separated, one row per minute):

  Date                   dd/mm/yyyy
  Time                   hh:mm:ss
  Global_active_power    household global minute-averaged active power (kW)   <- primary metric
  Global_reactive_power  household global minute-averaged reactive power (kW)
  Voltage                minute-averaged voltage (V)
  Global_intensity       household global minute-averaged current (A)
  Sub_metering_1..3      energy sub-metering (watt-hour of active energy)

Missing values are written as ``?`` in the original file. The raw file is never
modified; cleaned data lives in separate output tables.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

RAW_COLUMNS = [
    "Date", "Time", "Global_active_power", "Global_reactive_power",
    "Voltage", "Global_intensity", "Sub_metering_1", "Sub_metering_2", "Sub_metering_3",
]
RAW_SCHEMA = StructType([StructField(c, StringType(), True) for c in RAW_COLUMNS])

# raw column -> clean column (all numeric)
NUMERIC_COLUMNS = {
    "Global_active_power": "global_active_power",
    "Global_reactive_power": "global_reactive_power",
    "Voltage": "voltage",
    "Global_intensity": "global_intensity",
    "Sub_metering_1": "sub_metering_1",
    "Sub_metering_2": "sub_metering_2",
    "Sub_metering_3": "sub_metering_3",
}
CLEAN_COLUMNS = ["timestamp"] + list(NUMERIC_COLUMNS.values())

TIMESTAMP_FORMAT = "d/M/yyyy HH:mm:ss"
# Anything above this is not a physical household reading (also excludes Infinity).
MAX_PLAUSIBLE_VALUE = 1_000_000.0


def read_raw(spark: SparkSession, path: str) -> DataFrame:
    """Read the semicolon-separated file as strings (no silent type guessing)."""
    return spark.read.csv(path, schema=RAW_SCHEMA, sep=";", header=True, mode="PERMISSIVE")


def _sanitise(column):
    """String -> double; '?', '', text, NaN, negatives and implausible values become NULL."""
    parsed = F.trim(column).cast("double")
    return F.when(parsed.between(0.0, MAX_PLAUSIBLE_VALUE), parsed)


def parse_raw(raw: DataFrame) -> DataFrame:
    """Parse timestamp and numeric columns. Rows are *not* dropped here.

    The extra column ``_gap_parsed`` keeps the unsanitised Global_active_power so
    that data-quality counts can tell "missing" apart from "invalid".
    """
    df = raw.withColumn(
        "timestamp",
        F.to_timestamp(
            F.concat_ws(" ", F.trim(F.col("Date")), F.trim(F.col("Time"))), TIMESTAMP_FORMAT
        ),
    )
    df = df.withColumn("_gap_parsed", F.trim(F.col("Global_active_power")).cast("double"))
    for src, dst in NUMERIC_COLUMNS.items():
        df = df.withColumn(dst, _sanitise(F.col(src)))
    return df.select("timestamp", "_gap_parsed", *NUMERIC_COLUMNS.values())


def data_quality_summary(parsed: DataFrame) -> dict:
    """Count rows and problems in one pass over the parsed data."""
    row = parsed.agg(
        F.count(F.lit(1)).alias("total_records"),
        F.sum(F.when(F.col("timestamp").isNull(), 1).otherwise(0)).alias("invalid_timestamp_rows"),
        F.sum(F.when(F.col("_gap_parsed").isNull(), 1).otherwise(0)).alias("missing_gap_rows"),
        F.sum(
            F.when(F.col("_gap_parsed").isNotNull() & F.col("global_active_power").isNull(), 1)
            .otherwise(0)
        ).alias("invalid_gap_rows"),
        F.sum(F.when(F.col("global_active_power").isNotNull(), 1).otherwise(0)).alias("valid_gap_rows"),
    ).collect()[0]
    return {k: int(v or 0) for k, v in row.asDict().items()}


def finalize_clean(parsed: DataFrame) -> DataFrame:
    """Drop unparseable timestamps and duplicate timestamps; keep missing readings as NULL.

    Missing power readings are *kept* as NULL (not imputed) so that aggregation
    functions ignore them and the hourly step can count how many valid minutes exist.
    """
    return (
        parsed.drop("_gap_parsed")
        .filter(F.col("timestamp").isNotNull())
        .dropDuplicates(["timestamp"])
        .select(*CLEAN_COLUMNS)
    )


def clean_raw(raw: DataFrame) -> DataFrame:
    """Convenience wrapper: parse + finalize."""
    return finalize_clean(parse_raw(raw))
