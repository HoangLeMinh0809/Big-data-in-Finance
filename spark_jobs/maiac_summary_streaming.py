"""
MAIAC summary Kafka -> Iceberg streaming processor.

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
    to_date,
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

from hanoi_config import get_table_names
from runtime_utils import apply_stream_trigger, parse_streaming_runtime

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "maiac-summary")
KAFKA_STARTING_OFFSETS = os.getenv("KAFKA_STARTING_OFFSETS", "latest")
CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", "hdfs://namenode:9000/checkpoints/maiac_summary/")
ICEBERG_CATALOG = os.getenv("ICEBERG_CATALOG", "ais")
ICEBERG_WAREHOUSE = os.getenv("ICEBERG_WAREHOUSE", "hdfs://namenode:9000/warehouse/iceberg")

TABLE_NAMES = get_table_names()
ICEBERG_TABLE = os.getenv("ICEBERG_TABLE", TABLE_NAMES["maiac_bronze"])

MAIAC_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("granule_id", StringType(), True),
        StructField("granule_name", StringType(), True),
        StructField("producer_granule_id", StringType(), True),
        StructField("short_name", StringType(), True),
        StructField("version", StringType(), True),
        StructField("tile", StringType(), True),
        StructField("acquisition_date", StringType(), True),
        StructField("time_start", StringType(), True),
        StructField("time_end", StringType(), True),
        StructField("updated", StringType(), True),
        StructField("download_url", StringType(), True),
        StructField("bbox", ArrayType(DoubleType()), True),
        StructField("source", StringType(), True),
        StructField("ingest_time", StringType(), True),
        StructField("window_mode", StringType(), True),
        StructField("window_start_utc", StringType(), True),
        StructField("window_end_utc", StringType(), True),
        StructField("window_now_utc", StringType(), True),
    ]
)


def main() -> None:
    stop_after_batch, processing_time = parse_streaming_runtime(default_processing_time="60 seconds")

    spark = (
        SparkSession.builder
        .appName("MAIACSummary_Streaming")
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
        .select(from_json(col("json_str"), MAIAC_SCHEMA).alias("data"))
        .select("data.*")
    )

    final_df = (
        parsed_df
        .withColumn("event_time", coalesce(to_timestamp(col("time_start")), to_timestamp(col("window_end_utc"))))
        .withColumn("acquisition_date", to_date(col("acquisition_date"), "yyyy-MM-dd"))
        .withColumn("ingest_time", to_timestamp(col("ingest_time")))
        .withColumn("window_start_utc", to_timestamp(col("window_start_utc")))
        .withColumn("window_end_utc", to_timestamp(col("window_end_utc")))
        .withColumn("window_now_utc", to_timestamp(col("window_now_utc")))
        .withColumn("event_time", coalesce(col("event_time"), col("ingest_time")))
        .withColumn("year", spark_year(col("event_time")))
        .withColumn("month", spark_month(col("event_time")))
        .withColumn("day", dayofmonth(col("event_time")))
        .withColumn("spark_processed_at", col("ingest_time").cast("timestamp"))
        .select(
            "event_id", "granule_id", "granule_name", "producer_granule_id", "short_name",
            "version", "tile", "acquisition_date", "time_start", "time_end", "updated",
            "download_url", "bbox", "source", "ingest_time", "window_mode", "window_start_utc",
            "window_end_utc", "window_now_utc", "event_time", "spark_processed_at",
            "year", "month", "day"
        )
    )

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ICEBERG_CATALOG}.satellite")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {ICEBERG_TABLE} (
            event_id STRING,
            granule_id STRING,
            granule_name STRING,
            producer_granule_id STRING,
            short_name STRING,
            version STRING,
            tile STRING,
            acquisition_date DATE,
            time_start STRING,
            time_end STRING,
            updated STRING,
            download_url STRING,
            bbox ARRAY<DOUBLE>,
            source STRING,
            ingest_time TIMESTAMP,
            window_mode STRING,
            window_start_utc TIMESTAMP,
            window_end_utc TIMESTAMP,
            window_now_utc TIMESTAMP,
            event_time TIMESTAMP,
            spark_processed_at TIMESTAMP,
            year INT,
            month INT,
            day INT
        )
        USING ICEBERG
        PARTITIONED BY (short_name, year, month, day, tile)
        TBLPROPERTIES ('format-version'='2')
        """
    )

    writer = (
        final_df.writeStream
        .format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .queryName("maiac_summary_to_iceberg")
    )

    writer = apply_stream_trigger(writer, stop_after_batch=stop_after_batch, processing_time=processing_time)
    query = writer.toTable(ICEBERG_TABLE)

    print(f"MAIAC stream mode: {'availableNow' if stop_after_batch else processing_time}")
    print(f"Kafka startingOffsets: {KAFKA_STARTING_OFFSETS}")
    query.awaitTermination()


if __name__ == "__main__":
    main()
