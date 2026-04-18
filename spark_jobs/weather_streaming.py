"""
=============================================================================
Weather History — Spark Structured Streaming Job
=============================================================================
Chuc nang:
  1. Doc stream tu Kafka topic "weather_history"
  2. Parse JSON message theo schema output cua ingest_weather
  3. Cast mot so truong thoi gian/phong van
  4. Them cot partition: year, month (dua tren query_date)
  5. Ghi Parquet ra HDFS tai /data/weather_history/
  6. Su dung checkpoint de dam bao exactly-once

Luu y: Day la pass-through pipeline, khong co business transform phuc tap.
=============================================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
	from_json,
	col,
	year as spark_year,
	month as spark_month,
	to_date,
	to_timestamp,
	current_timestamp,
)
from pyspark.sql.types import (
	StructType,
	StructField,
	StringType,
	DoubleType,
	LongType,
	IntegerType,
)

# =============================================================================
# Cau hinh
# =============================================================================
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
KAFKA_TOPIC = "weather_history"
HDFS_OUTPUT_PATH = "hdfs://namenode:9000/data/weather_history/"
CHECKPOINT_PATH = "hdfs://namenode:9000/checkpoints/weather_history/"

# =============================================================================
# Schema cho JSON message tu Kafka
# Phai khop voi output cua ingest_weather.py
# =============================================================================
WEATHER_SCHEMA = StructType([
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
])


def main():
	# =========================================================================
	# 1. Tao Spark Session
	# =========================================================================
	spark = (
		SparkSession.builder
		.appName("WeatherHistory_Streaming")
		.config("spark.sql.streaming.checkpointLocation", CHECKPOINT_PATH)
		.config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
		.getOrCreate()
	)

	spark.sparkContext.setLogLevel("WARN")
	print("=" * 60)
	print("SPARK STRUCTURED STREAMING - weather_history")
	print("=" * 60)

	# =========================================================================
	# 2. Doc stream tu Kafka
	# =========================================================================
	kafka_df = (
		spark
		.readStream
		.format("kafka")
		.option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
		.option("subscribe", KAFKA_TOPIC)
		.option("startingOffsets", "earliest")
		.option("failOnDataLoss", "false")
		.option("maxOffsetsPerTrigger", 10000)
		.load()
	)

	# =========================================================================
	# 3. Parse JSON tu Kafka value
	# =========================================================================
	parsed_df = (
		kafka_df
		.selectExpr("CAST(key AS STRING) AS kafka_key", "CAST(value AS STRING) AS json_str")
		.select(
			col("kafka_key"),
			from_json(col("json_str"), WEATHER_SCHEMA).alias("data"),
		)
		.select("data.*")
	)

	# =========================================================================
	# 4. Chuan hoa thoi gian va them cot partition
	# =========================================================================
	final_df = (
		parsed_df
		.withColumn("query_date", to_date(col("query_date"), "yyyy-MM-dd"))
		.withColumn("event_time", to_timestamp(col("time"), "yyyy-MM-dd HH:mm"))
		.withColumn("ingest_time", to_timestamp(col("ingest_time")))
		.withColumn("year", spark_year(col("query_date")))
		.withColumn("month", spark_month(col("query_date")))
		.withColumn("spark_processed_at", current_timestamp())
	)

	# =========================================================================
	# 5. Ghi ra HDFS duoi dang Parquet
	# =========================================================================
	query = (
		final_df
		.writeStream
		.outputMode("append")
		.format("parquet")
		.option("path", HDFS_OUTPUT_PATH)
		.option("checkpointLocation", CHECKPOINT_PATH)
		.partitionBy("year", "month")
		.trigger(processingTime="30 seconds")
		.queryName("weather_history_to_hdfs")
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
