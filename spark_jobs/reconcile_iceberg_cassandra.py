from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, to_timestamp

ICEBERG_CATALOG = "ais"
ICEBERG_WAREHOUSE = "hdfs://namenode:9000/warehouse/iceberg"
CASSANDRA_HOST = "cassandra"
CASSANDRA_KEYSPACE = "ais_serving"

DATASETS = {
    "weather": {
        "iceberg_table": f"{ICEBERG_CATALOG}.weather.weather_history_bronze",
        "cassandra_table": "weather_hourly_by_province_day",
        "time_col": "event_time",
        "key_col": "event_id",
    },
    "openaq": {
        "iceberg_table": f"{ICEBERG_CATALOG}.air_quality.openaq_hourly_bronze",
        "cassandra_table": "openaq_hourly_by_city_parameter_day",
        "time_col": "event_time",
        "key_col": "event_id",
    },
}


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("AIS_ReconcileServing")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.warehouse", ICEBERG_WAREHOUSE)
        .config("spark.cassandra.connection.host", CASSANDRA_HOST)
        .config("spark.cassandra.connection.port", "9042")
        .getOrCreate()
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Reconcile Iceberg historical vs Cassandra serving")
    parser.add_argument("--lookback-hours", type=int, default=24)
    parser.add_argument("--tolerance", type=float, default=0.95)
    parser.add_argument("--datasets", type=str, default="weather,openaq")
    return parser.parse_args()


def count_recent(df, time_col: str, key_col: str, window_start_utc: datetime) -> int:
    window_start_text = window_start_utc.strftime("%Y-%m-%d %H:%M:%S")
    return (
        df.where(col(time_col) >= to_timestamp(lit(window_start_text)))
        .select(key_col)
        .dropna()
        .distinct()
        .count()
    )


def reconcile_dataset(
    spark: SparkSession,
    dataset: str,
    lookback_hours: int,
    tolerance: float,
) -> None:
    cfg = DATASETS[dataset]
    window_start = datetime.now(timezone.utc) - timedelta(hours=lookback_hours)

    if not spark.catalog.tableExists(cfg["iceberg_table"]):
        raise RuntimeError(f"Iceberg table missing for dataset={dataset}: {cfg['iceberg_table']}")

    iceberg_df = spark.read.table(cfg["iceberg_table"])
    cassandra_df = (
        spark.read.format("org.apache.spark.sql.cassandra")
        .options(table=cfg["cassandra_table"], keyspace=CASSANDRA_KEYSPACE)
        .load()
    )

    iceberg_count = count_recent(
        iceberg_df,
        time_col=cfg["time_col"],
        key_col=cfg["key_col"],
        window_start_utc=window_start,
    )
    cassandra_count = count_recent(
        cassandra_df,
        time_col=cfg["time_col"],
        key_col=cfg["key_col"],
        window_start_utc=window_start,
    )

    ratio = 1.0 if iceberg_count == 0 else (cassandra_count / iceberg_count)
    print(
        f"dataset={dataset} window_hours={lookback_hours} "
        f"iceberg={iceberg_count} cassandra={cassandra_count} ratio={ratio:.4f}"
    )

    if ratio + 1e-9 < tolerance:
        raise RuntimeError(
            f"Reconciliation failed for {dataset}: cassandra/iceberg ratio {ratio:.4f} < tolerance {tolerance:.4f}"
        )


def main() -> None:
    args = parse_args()
    selected = [item.strip() for item in args.datasets.split(",") if item.strip()]

    for ds in selected:
        if ds not in DATASETS:
            raise SystemExit(f"Unsupported dataset: {ds}. Supported: {','.join(sorted(DATASETS))}")

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    try:
        for ds in selected:
            reconcile_dataset(
                spark=spark,
                dataset=ds,
                lookback_hours=max(1, int(args.lookback_hours)),
                tolerance=float(args.tolerance),
            )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
