from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession

ICEBERG_CATALOG = "ais"
ICEBERG_WAREHOUSE = "hdfs://namenode:9000/warehouse/iceberg"
TABLES = [
    "weather.weather_history_bronze",
    "air_quality.openaq_hourly_bronze",
    "satellite.sentinel5p_summary_bronze",
    "satellite.maiac_summary_bronze",
]


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("AIS_IcebergMaintenance")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.warehouse", ICEBERG_WAREHOUSE)
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
        .getOrCreate()
    )


def run_maintenance(spark: SparkSession, retention_hours: int) -> None:
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=retention_hours)).strftime("%Y-%m-%d %H:%M:%S")

    for table_suffix in TABLES:
        fq_table = f"{ICEBERG_CATALOG}.{table_suffix}"
        if not spark.catalog.tableExists(fq_table):
            print(f"[SKIP] Table does not exist: {fq_table}")
            continue

        print(f"[RUN] rewrite_data_files: {fq_table}")
        try:
            spark.sql(f"CALL {ICEBERG_CATALOG}.system.rewrite_data_files(table => '{table_suffix}')")
        except Exception as exc:
            print(f"[WARN] rewrite_data_files failed for {fq_table}: {exc}")

        print(f"[RUN] expire_snapshots: {fq_table} older_than={cutoff}")
        try:
            spark.sql(
                f"CALL {ICEBERG_CATALOG}.system.expire_snapshots(table => '{table_suffix}', older_than => TIMESTAMP '{cutoff}')"
            )
        except Exception as exc:
            print(f"[WARN] expire_snapshots failed for {fq_table}: {exc}")

        print(f"[RUN] remove_orphan_files: {fq_table} older_than={cutoff}")
        try:
            spark.sql(
                f"CALL {ICEBERG_CATALOG}.system.remove_orphan_files(table => '{table_suffix}', older_than => TIMESTAMP '{cutoff}')"
            )
        except Exception as exc:
            print(f"[WARN] remove_orphan_files failed for {fq_table}: {exc}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run Iceberg maintenance procedures")
    parser.add_argument("--retention-hours", type=int, default=168)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    run_maintenance(spark, retention_hours=max(1, int(args.retention_hours)))
    spark.stop()


if __name__ == "__main__":
    main()
