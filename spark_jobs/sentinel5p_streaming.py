import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    from_json,
    lit,
    month as spark_month,
    to_date,
    to_timestamp,
    year as spark_year,
)
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sentinel5p-summary")
HDFS_OUTPUT_PATH = os.getenv("HDFS_OUTPUT_PATH", "hdfs://namenode:9000/data/sentinel5p_summary")
CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", "/opt/checkpoints/sentinel5p_summary")

S5P_SCHEMA = StructType(
    [
        StructField("product", StringType(), True),
        StructField("collection", StringType(), True),
        StructField("content_start", StringType(), True),
        StructField("content_end", StringType(), True),
        StructField("bbox", ArrayType(DoubleType()), True),
        StructField("file_name", StringType(), True),
        StructField(
            "stats",
            StructType(
                [
                    StructField("min", DoubleType(), True),
                    StructField("max", DoubleType(), True),
                    StructField("mean", DoubleType(), True),
                    StructField("valid_pct", DoubleType(), True),
                    # hotspots is optional
                    StructField(
                        "hotspots",
                        ArrayType(
                            StructType(
                                [
                                    StructField("lat", DoubleType(), True),
                                    StructField("lon", DoubleType(), True),
                                    StructField("value", DoubleType(), True),
                                ]
                            )
                        ),
                        True,
                    ),
                ]
            ),
            True,
        ),
        StructField("unit", StringType(), True),
        StructField("ingest_time", StringType(), True),
        StructField("event_id", StringType(), True),
        StructField("source", StringType(), True),
    ]
)


def main():
    spark = (
        SparkSession.builder.appName("Sentinel5P_Summary_Streaming")
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_PATH)
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    print("=" * 60)
    print("SPARK STRUCTURED STREAMING - sentinel5p-summary")
    print("=" * 60)

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed_df = (
        kafka_df.selectExpr("CAST(value AS STRING) AS json_str")
        .select(from_json(col("json_str"), S5P_SCHEMA).alias("data"))
        .select("data.*")
    )

    # Partition by content_start (UTC) if present; fallback to ingest_time
    final_df = (
        parsed_df
        .withColumn(
            "event_time",
            to_timestamp(col("content_start")),
        )
        .withColumn(
            "event_time",
            to_timestamp(col("ingest_time")).when(col("event_time").isNull(), to_timestamp(col("ingest_time"))).otherwise(col("event_time")),
        )
        .withColumn("event_date", to_date(col("event_time")))
        .withColumn("year", spark_year(col("event_date")))
        .withColumn("month", spark_month(col("event_date")))
        .withColumn("spark_processed_at", current_timestamp())
    )

    query = (
        final_df.writeStream.outputMode("append")
        .format("parquet")
        .option("path", HDFS_OUTPUT_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .partitionBy("year", "month")
        .trigger(processingTime="30 seconds")
        .queryName("sentinel5p_summary_to_hdfs")
        .start()
    )

    print(f"Streaming query started: {query.name}")
    print(f"  Kafka topic: {KAFKA_TOPIC}")
    print(f"  HDFS output: {HDFS_OUTPUT_PATH}")
    print(f"  Checkpoint:  {CHECKPOINT_PATH}")
    print("Waiting for data...")

    query.awaitTermination()


if __name__ == "__main__":
    main()
