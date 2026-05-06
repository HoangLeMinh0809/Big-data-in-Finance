"""
Weather history Kafka -> Iceberg streaming processor.

Default mode is long-running streaming.
Use --stop-after-batch 1 for bootstrap/backfill catchup runs.
"""

from __future__ import annotations

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    month as spark_month,
    to_date,
    to_timestamp,
    year as spark_year,
)
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from hanoi_config import get_table_names
from runtime_utils import apply_stream_trigger, parse_streaming_runtime

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "weather_history")
KAFKA_STARTING_OFFSETS = os.getenv("KAFKA_STARTING_OFFSETS", "latest")
CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", "hdfs://namenode:9000/checkpoints/weather_history/")
ICEBERG_CATALOG = os.getenv("ICEBERG_CATALOG", "ais")
ICEBERG_WAREHOUSE = os.getenv("ICEBERG_WAREHOUSE", "hdfs://namenode:9000/warehouse/iceberg")

TABLE_NAMES = get_table_names()
ICEBERG_TABLE = os.getenv("ICEBERG_TABLE", TABLE_NAMES["weather_bronze"])

WEATHER_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("province", StringType(), True),
        StructField("country", StringType(), True),
        StructField("region", StringType(), True),
        StructField("location_name", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
        StructField("tz_id", StringType(), True),
        StructField("query_date", StringType(), True),
        StructField("time", StringType(), True),
        StructField("time_epoch", LongType(), True),
        StructField("is_day", IntegerType(), True),
        StructField("temp_c", DoubleType(), True),
        StructField("temp_f", DoubleType(), True),
        StructField("feelslike_c", DoubleType(), True),
        StructField("feelslike_f", DoubleType(), True),
        StructField("windchill_c", DoubleType(), True),
        StructField("windchill_f", DoubleType(), True),
        StructField("heatindex_c", DoubleType(), True),
        StructField("heatindex_f", DoubleType(), True),
        StructField("dewpoint_c", DoubleType(), True),
        StructField("dewpoint_f", DoubleType(), True),
        StructField("condition_text", StringType(), True),
        StructField("condition_code", IntegerType(), True),
        StructField("condition_icon", StringType(), True),
        StructField("wind_mph", DoubleType(), True),
        StructField("wind_kph", DoubleType(), True),
        StructField("wind_degree", IntegerType(), True),
        StructField("wind_dir", StringType(), True),
        StructField("gust_mph", DoubleType(), True),
        StructField("gust_kph", DoubleType(), True),
        StructField("pressure_mb", DoubleType(), True),
        StructField("pressure_in", DoubleType(), True),
        StructField("precip_mm", DoubleType(), True),
        StructField("precip_in", DoubleType(), True),
        StructField("snow_cm", DoubleType(), True),
        StructField("humidity", IntegerType(), True),
        StructField("cloud", IntegerType(), True),
        StructField("vis_km", DoubleType(), True),
        StructField("vis_miles", DoubleType(), True),
        StructField("uv", DoubleType(), True),
        StructField("will_it_rain", IntegerType(), True),
        StructField("chance_of_rain", IntegerType(), True),
        StructField("will_it_snow", IntegerType(), True),
        StructField("chance_of_snow", IntegerType(), True),
        StructField("source", StringType(), True),
        StructField("source_file", StringType(), True),
        StructField("ingest_time", StringType(), True),
        StructField("window_mode", StringType(), True),
        StructField("window_start_utc", StringType(), True),
        StructField("window_end_utc", StringType(), True),
        StructField("window_now_utc", StringType(), True),
    ]
)

WEATHER_TABLE_COLUMNS = [
    "event_id",
    "province",
    "country",
    "region",
    "location_name",
    "lat",
    "lon",
    "tz_id",
    "query_date",
    "time",
    "event_time",
    "time_epoch",
    "is_day",
    "temp_c",
    "temp_f",
    "feelslike_c",
    "feelslike_f",
    "windchill_c",
    "windchill_f",
    "heatindex_c",
    "heatindex_f",
    "dewpoint_c",
    "dewpoint_f",
    "condition_text",
    "condition_code",
    "condition_icon",
    "wind_mph",
    "wind_kph",
    "wind_degree",
    "wind_dir",
    "gust_mph",
    "gust_kph",
    "pressure_mb",
    "pressure_in",
    "precip_mm",
    "precip_in",
    "snow_cm",
    "humidity",
    "cloud",
    "vis_km",
    "vis_miles",
    "uv",
    "will_it_rain",
    "chance_of_rain",
    "will_it_snow",
    "chance_of_snow",
    "source",
    "source_file",
    "ingest_time",
    "window_mode",
    "window_start_utc",
    "window_end_utc",
    "window_now_utc",
    "spark_processed_at",
    "year",
    "month",
]


def main() -> None:
    stop_after_batch, processing_time = parse_streaming_runtime(default_processing_time="30 seconds")

    spark = (
        SparkSession.builder
        .appName("WeatherHistory_Streaming")
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
        .option("maxOffsetsPerTrigger", 10000)
        .load()
    )

    parsed_df = (
        kafka_df
        .selectExpr("CAST(key AS STRING) AS kafka_key", "CAST(value AS STRING) AS json_str")
        .select(col("kafka_key"), from_json(col("json_str"), WEATHER_SCHEMA).alias("data"))
        .select("data.*")
    )

    final_df = (
        parsed_df
        .withColumn("query_date", to_date(col("query_date"), "yyyy-MM-dd"))
        .withColumn("event_time", to_timestamp(col("time"), "yyyy-MM-dd HH:mm"))
        .withColumn("ingest_time", to_timestamp(col("ingest_time")))
        .withColumn("window_start_utc", to_timestamp(col("window_start_utc")))
        .withColumn("window_end_utc", to_timestamp(col("window_end_utc")))
        .withColumn("window_now_utc", to_timestamp(col("window_now_utc")))
        .withColumn("year", spark_year(col("query_date")))
        .withColumn("month", spark_month(col("query_date")))
        .withColumn("spark_processed_at", col("ingest_time").cast("timestamp"))
        .select(*WEATHER_TABLE_COLUMNS)
    )

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ICEBERG_CATALOG}.weather")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {ICEBERG_TABLE} (
            event_id STRING,
            province STRING,
            country STRING,
            region STRING,
            location_name STRING,
            lat DOUBLE,
            lon DOUBLE,
            tz_id STRING,
            query_date DATE,
            time STRING,
            event_time TIMESTAMP,
            time_epoch BIGINT,
            is_day INT,
            temp_c DOUBLE,
            temp_f DOUBLE,
            feelslike_c DOUBLE,
            feelslike_f DOUBLE,
            windchill_c DOUBLE,
            windchill_f DOUBLE,
            heatindex_c DOUBLE,
            heatindex_f DOUBLE,
            dewpoint_c DOUBLE,
            dewpoint_f DOUBLE,
            condition_text STRING,
            condition_code INT,
            condition_icon STRING,
            wind_mph DOUBLE,
            wind_kph DOUBLE,
            wind_degree INT,
            wind_dir STRING,
            gust_mph DOUBLE,
            gust_kph DOUBLE,
            pressure_mb DOUBLE,
            pressure_in DOUBLE,
            precip_mm DOUBLE,
            precip_in DOUBLE,
            snow_cm DOUBLE,
            humidity INT,
            cloud INT,
            vis_km DOUBLE,
            vis_miles DOUBLE,
            uv DOUBLE,
            will_it_rain INT,
            chance_of_rain INT,
            will_it_snow INT,
            chance_of_snow INT,
            source STRING,
            source_file STRING,
            ingest_time TIMESTAMP,
            window_mode STRING,
            window_start_utc TIMESTAMP,
            window_end_utc TIMESTAMP,
            window_now_utc TIMESTAMP,
            spark_processed_at TIMESTAMP,
            year INT,
            month INT
        )
        USING ICEBERG
        PARTITIONED BY (year, month)
        TBLPROPERTIES ('format-version'='2')
        """
    )

    writer = (
        final_df.writeStream
        .format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .queryName("weather_history_to_iceberg")
    )

    writer = apply_stream_trigger(writer, stop_after_batch=stop_after_batch, processing_time=processing_time)
    query = writer.toTable(ICEBERG_TABLE)

    print(f"Weather stream mode: {'availableNow' if stop_after_batch else processing_time}")
    print(f"Kafka startingOffsets: {KAFKA_STARTING_OFFSETS}")
    query.awaitTermination()


if __name__ == "__main__":
    main()
