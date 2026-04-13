from __future__ import annotations

import sys
from typing import Iterable

from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_date, to_timestamp, date_format

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
ICEBERG_CATALOG = "ais"
ICEBERG_WAREHOUSE = "hdfs://namenode:9000/warehouse/iceberg"
CASSANDRA_HOST = "cassandra"
CASSANDRA_KEYSPACE = "ais_serving"

SOURCE_TABLES = {
    "weather": {
        "source": f"{ICEBERG_CATALOG}.weather.weather_history_bronze",
        "target": "weather_hourly_by_province_day",
    },
    "openaq": {
        "source": f"{ICEBERG_CATALOG}.air_quality.openaq_hourly_bronze",
        "target": "openaq_hourly_by_city_parameter_day",
    },
}


def build_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("IcebergToCassandra_Load")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.warehouse", ICEBERG_WAREHOUSE)
        .config("spark.cassandra.connection.host", CASSANDRA_HOST)
        .config("spark.cassandra.connection.port", "9042")
        .getOrCreate()
    )


def enrich_weather(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("query_date", to_date(col("query_date"), "yyyy-MM-dd"))
        .withColumn("event_time_ts", to_timestamp(col("time"), "yyyy-MM-dd HH:mm"))
        .withColumn("day", date_format(col("query_date"), "yyyy-MM-dd"))
        .select(
            col("event_id"),
            col("province"),
            col("query_date"),
            col("day"),
            col("event_time_ts").alias("event_time"),
            col("time_epoch"),
            col("location_name"),
            col("lat"),
            col("lon"),
            col("temp_c"),
            col("temp_f"),
            col("humidity"),
            col("wind_kph"),
            col("wind_degree"),
            col("wind_dir"),
            col("precip_mm"),
            col("condition_text"),
            col("source"),
            col("ingest_time"),
        )
    )


def enrich_openaq(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("event_time_ts", to_timestamp(col("datetime_utc")))
        .withColumn("day", date_format(col("event_time_ts"), "yyyy-MM-dd"))
        .select(
            col("event_id"),
            col("city"),
            col("parameter"),
            col("day"),
            col("event_time_ts").alias("event_time"),
            col("location_id"),
            col("location_name"),
            col("provider"),
            col("sensor_id"),
            col("unit"),
            col("value"),
            col("min"),
            col("max"),
            col("sd"),
            col("coverage_pct"),
            col("source"),
            col("ingest_time"),
        )
    )


def write_to_cassandra(df: DataFrame, table_name: str) -> None:
    (
        df.write
        .format("org.apache.spark.sql.cassandra")
        .mode("append")
        .options(table=table_name, keyspace=CASSANDRA_KEYSPACE)
        .save()
    )


def main() -> None:
    if len(sys.argv) != 2 or sys.argv[1] not in SOURCE_TABLES:
        valid = ", ".join(sorted(SOURCE_TABLES))
        raise SystemExit(f"Usage: iceberg_to_cassandra.py <{valid}>")

    dataset = sys.argv[1]
    table_cfg = SOURCE_TABLES[dataset]

    spark = build_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    source_df = spark.read.table(table_cfg["source"])

    if dataset == "weather":
        target_df = enrich_weather(source_df)
    else:
        target_df = enrich_openaq(source_df)

    write_to_cassandra(target_df, table_cfg["target"])

    print(f"Loaded {dataset} from {table_cfg['source']} into Cassandra table {table_cfg['target']}")
    spark.stop()


if __name__ == "__main__":
    main()
