"""
Output:
    Iceberg table on HDFS warehouse
Checkpoint:
    hdfs://namenode:9000/checkpoints/openaq_hourly/
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json,
    col,
    year as spark_year,
    month as spark_month,
    dayofmonth,
    hour,
    to_timestamp,
    current_timestamp,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType,
)

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "openaq-hourly"
CHECKPOINT_PATH = "hdfs://namenode:9000/checkpoints/openaq_hourly/"
ICEBERG_CATALOG = "ais"
ICEBERG_WAREHOUSE = "hdfs://namenode:9000/warehouse/iceberg"
ICEBERG_TABLE = f"{ICEBERG_CATALOG}.air_quality.openaq_hourly_bronze"

OPENAQ_SCHEMA = StructType([
    StructField("location_id", LongType(), True),
    StructField("location_name", StringType(), True),
    StructField("city", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("provider", StringType(), True),
    StructField("sensor_id", LongType(), True),
    StructField("parameter", StringType(), True),
    StructField("unit", StringType(), True),
    StructField("datetime_utc", StringType(), True),
    StructField("datetime_local", StringType(), True),
    StructField("value", DoubleType(), True),
    StructField("min", DoubleType(), True),
    StructField("max", DoubleType(), True),
    StructField("sd", DoubleType(), True),
    StructField("expected_count", LongType(), True),
    StructField("observed_count", LongType(), True),
    StructField("coverage_pct", DoubleType(), True),
    StructField("source", StringType(), True),
    StructField("ingest_time", StringType(), True),
    StructField("window_mode", StringType(), True),
    StructField("window_start_utc", StringType(), True),
    StructField("window_end_utc", StringType(), True),
    StructField("window_now_utc", StringType(), True),
    StructField("event_id", StringType(), True),
])


def main():
    spark = (
        SparkSession.builder
        .appName("OpenAQHourly_Streaming")
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
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed_df = (
        kafka_df
        .selectExpr("CAST(value AS STRING) AS json_str")
        .select(from_json(col("json_str"), OPENAQ_SCHEMA).alias("data"))
        .select("data.*")
    )

    # Parse datetime_utc 
    final_df = (
        parsed_df
        .withColumn("event_time", to_timestamp(col("datetime_utc")))
        .withColumn("window_start_utc", to_timestamp(col("window_start_utc")))
        .withColumn("window_end_utc", to_timestamp(col("window_end_utc")))
        .withColumn("window_now_utc", to_timestamp(col("window_now_utc")))
        .withColumn("year", spark_year(col("event_time")))
        .withColumn("month", spark_month(col("event_time")))
        .withColumn("day", dayofmonth(col("event_time")))
        .withColumn("hour", hour(col("event_time")))
        .withColumn("spark_processed_at", current_timestamp())
    )

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ICEBERG_CATALOG}.air_quality")
    if not spark.catalog.tableExists(ICEBERG_TABLE):
        (
            final_df
            .limit(0)
            .writeTo(ICEBERG_TABLE)
            .using("iceberg")
            .tableProperty("format-version", "2")
            .partitionedBy(col("year"), col("month"), col("day"), col("hour"))
            .create()
        )

    query = (
        final_df.writeStream
        .format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(availableNow=True)
        .queryName("openaq_hourly_to_iceberg")
        .toTable(ICEBERG_TABLE)
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
