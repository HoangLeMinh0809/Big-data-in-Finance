"""
MODIS MAIAC summary - Spark Structured Streaming job
Kafka topic:
  maiac-summary
Output:
  hdfs://namenode:9000/data/maiac_summary/
Checkpoint:
  hdfs://namenode:9000/checkpoints/maiac_summary/
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json,
    col,
    coalesce,
    to_timestamp,
    to_date,
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
KAFKA_TOPIC = "maiac-summary"
HDFS_OUTPUT_PATH = "hdfs://namenode:9000/data/maiac_summary/"
CHECKPOINT_PATH = "hdfs://namenode:9000/checkpoints/maiac_summary/"

MAIAC_SCHEMA = StructType([
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
])


def main():
    spark = (
        SparkSession.builder
        .appName("MAIACSummary_Streaming")
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
        .partitionBy("short_name", "year", "month", "day", "tile")
        .trigger(processingTime="30 seconds")
        .queryName("maiac_summary_to_hdfs")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
