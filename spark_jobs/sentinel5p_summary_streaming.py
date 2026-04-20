"""
Sentinel-5P summary - Spark Structured Streaming job
Kafka topic:
  sentinel5p-summary
Output:
  hdfs://namenode:9000/data/sentinel5p_summary/
Checkpoint:
  hdfs://namenode:9000/checkpoints/sentinel5p_summary/
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json,
    col,
    coalesce,
    to_timestamp,
    year as spark_year,
    month as spark_month,
    dayofmonth,
    current_timestamp,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    ArrayType,
)

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "sentinel5p-summary"
HDFS_OUTPUT_PATH = "hdfs://namenode:9000/data/sentinel5p_summary/"
CHECKPOINT_PATH = "hdfs://namenode:9000/checkpoints/sentinel5p_summary/"

SENTINEL5P_SCHEMA = StructType([
    StructField("product", StringType(), True),
    StructField("collection", StringType(), True),
    StructField("content_start", StringType(), True),
    StructField("content_end", StringType(), True),
    StructField("bbox", ArrayType(DoubleType()), True),
    StructField("file_name", StringType(), True),
    StructField("unit", StringType(), True),
    StructField("ingest_time", StringType(), True),
    StructField("window_mode", StringType(), True),
    StructField("window_start_utc", StringType(), True),
    StructField("window_end_utc", StringType(), True),
    StructField("window_now_utc", StringType(), True),
    StructField("event_id", StringType(), True),
    StructField("source", StringType(), True),
])


def main():
    spark = (
        SparkSession.builder
        .appName("Sentinel5PSummary_Streaming")
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
        .select(from_json(col("json_str"), SENTINEL5P_SCHEMA).alias("data"))
        .select("data.*")
    )

    final_df = (
        parsed_df
        .withColumn("content_start_ts", to_timestamp(col("content_start")))
        .withColumn("content_end_ts", to_timestamp(col("content_end")))
        .withColumn("ingest_time", to_timestamp(col("ingest_time")))
        .withColumn("window_start_utc", to_timestamp(col("window_start_utc")))
        .withColumn("window_end_utc", to_timestamp(col("window_end_utc")))
        .withColumn("window_now_utc", to_timestamp(col("window_now_utc")))
        .withColumn("event_time", coalesce(col("content_start_ts"), col("window_end_utc")))
        .withColumn("year", spark_year(col("event_time")))
        .withColumn("month", spark_month(col("event_time")))
        .withColumn("day", dayofmonth(col("event_time")))
        .withColumn("spark_processed_at", current_timestamp())
    )

    query = (
        final_df.writeStream
        .outputMode("append")
        .format("parquet")
        .option("path", HDFS_OUTPUT_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .partitionBy("product", "year", "month", "day")
        .trigger(processingTime="30 seconds")
        .queryName("sentinel5p_summary_to_hdfs")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
