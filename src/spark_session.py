"""SparkSession factory.

Spark runs in local mode (``local[*]``) by default, which is what Google Colab
and a laptop use. When HDFS is genuinely enabled, a few Hadoop client settings
are added so that a Spark driver running *outside* Docker can talk to a
DataNode running *inside* Docker (see hadoop/README.md).
"""
from __future__ import annotations

import os

from .config import Config


def get_spark(cfg: Config, use_hdfs: bool = False):
    """Create (or reuse) a SparkSession configured for GridGuard."""
    from pyspark.sql import SparkSession  # imported lazily so other modules work without Spark

    if use_hdfs:
        # Used by the Hadoop client when no Kerberos is configured (dev clusters only).
        os.environ.setdefault("HADOOP_USER_NAME", cfg.hdfs_user)

    builder = (
        SparkSession.builder.appName(cfg.spark_app_name)
        .master(cfg.spark_master)
        .config("spark.driver.memory", cfg.spark_driver_memory)
        .config("spark.sql.shuffle.partitions", "8")
        # UTC avoids DST gaps/overlaps corrupting the meter's wall-clock timestamps.
        .config("spark.sql.session.timeZone", "UTC")
        # Identical behaviour on Spark 3.5 and 4.x: bad casts become NULL instead of errors.
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .config("spark.ui.showConsoleProgress", "false")
    )
    if use_hdfs:
        builder = (
            builder
            # Connect to DataNodes by hostname (needed when they sit behind Docker port mappings).
            .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
            # Fail fast instead of retrying for minutes when the NameNode is down.
            .config("spark.hadoop.ipc.client.connect.max.retries", "2")
            .config("spark.hadoop.ipc.client.connect.max.retries.on.timeouts", "2")
            .config("spark.hadoop.ipc.client.connect.timeout", "5000")
        )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
