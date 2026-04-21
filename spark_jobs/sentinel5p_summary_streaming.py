"""
Sentinel-5P summary Kafka -> Iceberg streaming processor.

Default mode is long-running streaming.
Use --stop-after-batch 1 for bootstrap/backfill catchup runs.
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    coalesce,
    dayofmonth,
    from_json,
    month as spark_month,
    to_timestamp,
    year as spark_year,
)
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)

from runtime_utils import apply_stream_trigger, parse_streaming_runtime

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sentinel5p-summary")
KAFKA_STARTING_OFFSETS = os.getenv("KAFKA_STARTING_OFFSETS", "latest")
CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", "hdfs://namenode:9000/checkpoints/sentinel5p_summary/")
ICEBERG_CATALOG = os.getenv("ICEBERG_CATALOG", "ais")
ICEBERG_WAREHOUSE = os.getenv("ICEBERG_WAREHOUSE", "hdfs://namenode:9000/warehouse/iceberg")
ICEBERG_TABLE = os.getenv("ICEBERG_TABLE", f"{ICEBERG_CATALOG}.satellite.sentinel5p_summary_bronze")

SENTINEL5P_SCHEMA = StructType(
    [
        StructField("product", StringType(), True),
        StructField("collection", StringType(), True),
        StructField("content_start", StringType(), True),
        StructField("content_end", StringType(), True),
        StructField("bbox", ArrayType(DoubleType()), True),
        StructField("product_name", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("file_name", StringType(), True),
        StructField(
            "stats",
            StructType(
                [
                    StructField("min", DoubleType(), True),
                    StructField("max", DoubleType(), True),
                    StructField("mean", DoubleType(), True),
                    StructField("valid_pct", DoubleType(), True),
                ]
            ),
            True,
        ),
        StructField("unit", StringType(), True),
        StructField("ingest_time", StringType(), True),
        StructField("window_mode", StringType(), True),
        StructField("window_start_utc", StringType(), True),
        StructField("window_end_utc", StringType(), True),
        StructField("window_now_utc", StringType(), True),
        StructField("event_id", StringType(), True),
        StructField("source", StringType(), True),
    ]
)


def main() -> None:
    stop_after_batch, processing_time = parse_streaming_runtime(default_processing_time="45 seconds")

    spark = (
        SparkSession.builder
        .appName("Sentinel5PSummary_Streaming")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.warehouse", ICEBERG_WAREHOUSE)
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_PATH)
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", KAFKA_STARTING_OFFSETS)
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed_df = (
        kafka_df
        .selectExpr("CAST(value AS STRING) AS json_str")
        .select(from_json(col("json_str"), SENTINEL5P_SCHEMA).alias("data"))
        .select("data.*")
    )

    final_df = (
        parsed_df
        .withColumn("stats_min", col("stats.min"))
        .withColumn("stats_max", col("stats.max"))
        .withColumn("stats_mean", col("stats.mean"))
        .withColumn("stats_valid_pct", col("stats.valid_pct"))
        .drop("stats")
        .withColumn("content_start_ts", to_timestamp(col("content_start")))
        .withColumn("window_end_utc", to_timestamp(col("window_end_utc")))
        .withColumn("window_start_utc", to_timestamp(col("window_start_utc")))
        .withColumn("window_now_utc", to_timestamp(col("window_now_utc")))
        .withColumn("ingest_time", to_timestamp(col("ingest_time")))
        .withColumn("event_time", coalesce(col("content_start_ts"), col("window_end_utc"), col("ingest_time")))
        .withColumn("year", spark_year(col("event_time")))
        .withColumn("month", spark_month(col("event_time")))
        .withColumn("day", dayofmonth(col("event_time")))
        .withColumn("spark_processed_at", col("ingest_time").cast("timestamp"))
        .select(
            "product",
            "collection",
            "content_start",
            "content_end",
            "bbox",
            "product_name",
            "product_id",
            "file_name",
            "stats_min",
            "stats_max",
            "stats_mean",
            "stats_valid_pct",
            "unit",
            "ingest_time",
            "window_mode",
            "window_start_utc",
            "window_end_utc",
            "window_now_utc",
            "event_id",
            "source",
            "event_time",
            "spark_processed_at",
            "year",
            "month",
            "day",
        )
        .drop("content_start_ts")
    )

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ICEBERG_CATALOG}.satellite")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {ICEBERG_TABLE} (
            product STRING,
            collection STRING,
            content_start STRING,
            content_end STRING,
            bbox ARRAY<DOUBLE>,
            product_name STRING,
            product_id STRING,
            file_name STRING,
            stats_min DOUBLE,
            stats_max DOUBLE,
            stats_mean DOUBLE,
            stats_valid_pct DOUBLE,
            unit STRING,
            ingest_time TIMESTAMP,
            window_mode STRING,
            window_start_utc TIMESTAMP,
            window_end_utc TIMESTAMP,
            window_now_utc TIMESTAMP,
            event_id STRING,
            source STRING,
            event_time TIMESTAMP,
            spark_processed_at TIMESTAMP,
            year INT,
            month INT,
            day INT
        )
        USING ICEBERG
        PARTITIONED BY (product, year, month, day)
        TBLPROPERTIES ('format-version'='2')
        """
    )

    writer = (
        final_df.writeStream
        .format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .queryName("sentinel5p_summary_to_iceberg")
    )

    writer = apply_stream_trigger(writer, stop_after_batch=stop_after_batch, processing_time=processing_time)
    query = writer.toTable(ICEBERG_TABLE)

    print(f"Sentinel-5P stream mode: {'availableNow' if stop_after_batch else processing_time}")
    print(f"Kafka startingOffsets: {KAFKA_STARTING_OFFSETS}")
    query.awaitTermination()


if __name__ == "__main__":
    main()
