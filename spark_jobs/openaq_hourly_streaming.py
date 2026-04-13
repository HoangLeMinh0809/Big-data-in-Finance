"""
Output:
  hdfs://namenode:9000/data/openaq_hourly/
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
HDFS_OUTPUT_PATH = "hdfs://namenode:9000/data/openaq_hourly/"
CHECKPOINT_PATH = "hdfs://namenode:9000/checkpoints/openaq_hourly/"

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
    StructField("event_id", StringType(), True),
])


def main():
    spark = (
        SparkSession.builder
        .appName("OpenAQHourly_Streaming")
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
        .withColumn("year", spark_year(col("event_time")))
        .withColumn("month", spark_month(col("event_time")))
        .withColumn("day", dayofmonth(col("event_time")))
        .withColumn("hour", hour(col("event_time")))
        .withColumn("spark_processed_at", current_timestamp())
    )

    query = (
        final_df.writeStream
        .outputMode("append")
        .format("parquet")
        .option("path", HDFS_OUTPUT_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .partitionBy("year", "month", "day", "hour")
        .trigger(processingTime="30 seconds")
        .queryName("openaq_hourly_to_hdfs")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
