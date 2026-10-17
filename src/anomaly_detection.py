"""Statistical anomaly detection (rolling z-score against a historical baseline).

Method
------
For every hourly observation x at date d and hour-of-day h, the baseline is
computed ONLY from observations of the same hour-of-day on the previous
``baseline_days`` calendar days (d-baseline_days ... d-1). The row being scored is
excluded from its own baseline, so there is no look-ahead leakage:

    z = (x - mean_baseline) / std_baseline          (sample std, n-1)

A reading is flagged when |z| >= threshold (default 3).

Safety rules (identical in Spark and in the pure-Python reference below):
  * observation missing / negative / not finite  -> "missing_observation", not scored
  * fewer than ``min_history`` baseline points    -> "insufficient_history", not scored
  * baseline std is NULL or ~0 (constant history) -> "zero_variance", not scored
  * otherwise                                     -> "ok", scored

A flag is a *statistical deviation only*. It does not prove theft, equipment
failure or any fault.
"""
from __future__ import annotations

import math
from typing import Optional

ZERO_STD_EPS = 1e-9

DISCLAIMER = (
    "This is a statistical deviation only and does not by itself indicate theft, "
    "equipment failure, or a fault."
)

ANOMALY_COLUMNS = [
    "timestamp", "date", "hour_of_day", "day_name",
    "observed_kw", "baseline_mean_kw", "baseline_std_kw", "history_count",
    "z_score", "abs_z_score", "anomaly_direction", "severity", "baseline_status", "explanation",
]


# ---------------------------------------------------------------------------
# Pure-Python reference implementation (used for unit tests and documentation)
# ---------------------------------------------------------------------------
def severity_for(abs_z: float, threshold: float) -> str:
    """Severity bands are relative to the threshold: +0..1 Moderate, +1..2 Strong, >=+2 Extreme."""
    if abs_z < threshold:
        return "None"
    if abs_z >= threshold + 2:
        return "Extreme"
    if abs_z >= threshold + 1:
        return "Strong"
    return "Moderate"


def score_value(
    observed: Optional[float],
    history: list,
    threshold: float = 3.0,
    min_history: int = 5,
) -> dict:
    """Score one observation against a list of prior observations (reference logic)."""
    clean = [
        float(v) for v in history
        if v is not None and math.isfinite(float(v)) and float(v) >= 0
    ]
    n = len(clean)
    mean = sum(clean) / n if n else None
    std = None
    if n >= 2:
        std = math.sqrt(sum((v - mean) ** 2 for v in clean) / (n - 1))

    if observed is None or not math.isfinite(float(observed)) or float(observed) < 0:
        status = "missing_observation"
    elif n < min_history:
        status = "insufficient_history"
    elif std is None or std <= ZERO_STD_EPS:
        status = "zero_variance"
    else:
        status = "ok"

    z = None
    if status == "ok":
        z = (float(observed) - mean) / std
    is_anomaly = z is not None and abs(z) >= threshold
    direction = "Not evaluated" if status != "ok" else "Normal"
    if is_anomaly:
        direction = "High" if z > 0 else "Low"
    return {
        "history_count": n,
        "baseline_mean_kw": mean,
        "baseline_std_kw": std,
        "z_score": z,
        "baseline_status": status,
        "is_anomaly": bool(is_anomaly),
        "anomaly_direction": direction,
        "severity": severity_for(abs(z), threshold) if z is not None else "None",
    }


# ---------------------------------------------------------------------------
# Spark implementation
# ---------------------------------------------------------------------------
def add_baseline(hourly_df, baseline_days: int, by_day_type: bool = False):
    """Add history_count, baseline_mean_kw, baseline_std_kw using a RANGE window over past days.

    ``rangeBetween(-baseline_days, -1)`` on the day number means: rows whose date is between
    d-baseline_days and d-1 (inclusive) within the same hour-of-day partition. The current
    day (offset 0) is never included, and gaps in the data do not stretch the window.
    """
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    df = hourly_df.withColumn(
        "date_num", F.datediff(F.col("date"), F.to_date(F.lit("1970-01-01")))
    )
    partition_cols = ["hour_of_day"] + (["is_weekend"] if by_day_type else [])
    w = Window.partitionBy(*partition_cols).orderBy("date_num").rangeBetween(-baseline_days, -1)
    return (
        df.withColumn("history_count", F.count("observed_kw").over(w))
        .withColumn("baseline_mean_kw", F.avg("observed_kw").over(w))
        .withColumn("baseline_std_kw", F.stddev_samp("observed_kw").over(w))
        .drop("date_num")
    )


def detect_anomalies(
    hourly_df,
    threshold: float = 3.0,
    baseline_days: int = 14,
    min_history: int = 5,
    by_day_type: bool = False,
):
    """Return the hourly table with baseline, z-score, flags, severity and explanation."""
    from pyspark.sql import functions as F

    if min_history < 2:
        raise ValueError("min_history must be >= 2")

    df = add_baseline(hourly_df, baseline_days, by_day_type)
    obs, mean, std = F.col("observed_kw"), F.col("baseline_mean_kw"), F.col("baseline_std_kw")
    n = F.col("history_count")

    status = (
        F.when(obs.isNull(), "missing_observation")
        .when(n < min_history, "insufficient_history")
        .when(std.isNull() | (std <= ZERO_STD_EPS), "zero_variance")
        .otherwise("ok")
    )
    df = df.withColumn("baseline_status", status)
    ok = F.col("baseline_status") == "ok"

    df = df.withColumn("z_score", F.when(ok, F.round((obs - mean) / std, 4)))
    df = df.withColumn("abs_z_score", F.abs(F.col("z_score")))
    df = df.withColumn("is_anomaly", F.coalesce(F.col("abs_z_score") >= threshold, F.lit(False)))
    df = df.withColumn(
        "anomaly_direction",
        F.when(~ok, "Not evaluated")
        .when(F.col("is_anomaly") & (F.col("z_score") > 0), "High")
        .when(F.col("is_anomaly") & (F.col("z_score") < 0), "Low")
        .otherwise("Normal"),
    )
    az = F.col("abs_z_score")
    df = df.withColumn(
        "severity",
        F.when(~F.col("is_anomaly"), "None")
        .when(az >= threshold + 2, "Extreme")
        .when(az >= threshold + 1, "Strong")
        .otherwise("Moderate"),
    )

    base_text = (
        "compared with the historical baseline for this time of day "
        "(baseline mean %.2f kW, std %.2f kW over %d prior observations; z-score %.2f). "
    )
    explanation = (
        F.when(
            F.col("anomaly_direction") == "High",
            F.format_string(
                "Consumption of %.2f kW is unusually high " + base_text + DISCLAIMER,
                obs, mean, std, n, F.col("z_score"),
            ),
        )
        .when(
            F.col("anomaly_direction") == "Low",
            F.format_string(
                "Consumption of %.2f kW is unusually low " + base_text + DISCLAIMER,
                obs, mean, std, n, F.col("z_score"),
            ),
        )
        .when(
            F.col("anomaly_direction") == "Normal",
            F.format_string(
                "Within the expected range for this time of day (z-score %.2f).", F.col("z_score")
            ),
        )
        .when(
            F.col("baseline_status") == "insufficient_history",
            F.format_string(
                "Not evaluated: only %d prior observation(s) available; at least "
                + str(int(min_history)) + " are required.",
                n,
            ),
        )
        .when(
            F.col("baseline_status") == "zero_variance",
            F.lit("Not evaluated: the historical baseline has zero variance, so a z-score is undefined."),
        )
        .otherwise(
            F.lit("Not evaluated: no valid observation for this hour (too few valid minute readings).")
        )
    )
    df = df.withColumn("explanation", explanation)
    for c in ("baseline_mean_kw", "baseline_std_kw"):
        df = df.withColumn(c, F.round(F.col(c), 4))
    return df


def extract_anomalies(scored_df):
    """Only flagged rows, strongest first, with the documented output columns."""
    from pyspark.sql import functions as F

    return (
        scored_df.filter(F.col("is_anomaly"))
        .select(*ANOMALY_COLUMNS)
        .orderBy(F.desc("abs_z_score"))
    )
