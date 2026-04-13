#!/bin/bash
# =============================================================================
# Submit Spark Structured Streaming job
# Chạy trên host — exec vào spark-master container
# =============================================================================

set -e

JOB_TYPE="${1:-stock}"

if [ "$JOB_TYPE" = "stock" ]; then
  APP_NAME="StockPricesDaily_Streaming"
  JOB_FILE="/opt/spark-jobs/stock_prices_streaming.py"
  JOB_ARGS=""
  HDFS_DATA_DIR="/data/stock_prices_daily"
  HDFS_CHECKPOINT_DIR="/checkpoints/stock_prices_daily"
elif [ "$JOB_TYPE" = "weather" ]; then
  APP_NAME="WeatherHistory_Streaming"
  JOB_FILE="/opt/spark-jobs/weather_streaming.py"
  JOB_ARGS=""
  HDFS_DATA_DIR="/data/weather_history"
  HDFS_CHECKPOINT_DIR="/checkpoints/weather_history"
elif [ "$JOB_TYPE" = "openaq" ]; then
  APP_NAME="OpenAQHourly_Streaming"
  JOB_FILE="/opt/spark-jobs/openaq_hourly_streaming.py"
  JOB_ARGS=""
  HDFS_DATA_DIR="/data/openaq_hourly"
  HDFS_CHECKPOINT_DIR="/checkpoints/openaq_hourly"
elif [ "$JOB_TYPE" = "cassandra-weather" ]; then
  APP_NAME="IcebergToCassandra_Weather"
  JOB_FILE="/opt/spark-jobs/iceberg_to_cassandra.py"
  JOB_ARGS="weather"
  HDFS_DATA_DIR="/data/iceberg_to_cassandra"
  HDFS_CHECKPOINT_DIR="/checkpoints/iceberg_to_cassandra"
elif [ "$JOB_TYPE" = "cassandra-openaq" ]; then
  APP_NAME="IcebergToCassandra_OpenAQ"
  JOB_FILE="/opt/spark-jobs/iceberg_to_cassandra.py"
  JOB_ARGS="openaq"
  HDFS_DATA_DIR="/data/iceberg_to_cassandra"
  HDFS_CHECKPOINT_DIR="/checkpoints/iceberg_to_cassandra"
else
  echo "Usage: $0 [stock|weather|openaq|cassandra-weather|cassandra-openaq]"
  exit 1
fi

echo "=== Tạo thư mục output trên HDFS ==="
docker exec namenode hdfs dfs -mkdir -p "$HDFS_DATA_DIR"
docker exec namenode hdfs dfs -mkdir -p "$HDFS_CHECKPOINT_DIR"
docker exec namenode hdfs dfs -mkdir -p /warehouse/iceberg
docker exec namenode hdfs dfs -chmod -R 777 /data
docker exec namenode hdfs dfs -chmod -R 777 /checkpoints
docker exec namenode hdfs dfs -chmod -R 777 /warehouse

echo ""
echo "=== Submit Spark Streaming Job: $APP_NAME ==="
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --name "$APP_NAME" \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-client:3.2.1,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 \
  --conf "spark.hadoop.fs.defaultFS=hdfs://namenode:9000" \
  --conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
  --conf "spark.sql.catalog.ais=org.apache.iceberg.spark.SparkCatalog" \
  --conf "spark.sql.catalog.ais.type=hadoop" \
  --conf "spark.sql.catalog.ais.warehouse=hdfs://namenode:9000/warehouse/iceberg" \
  --conf "spark.cassandra.connection.host=cassandra" \
  --conf "spark.cassandra.connection.port=9042" \
  --conf "spark.sql.adaptive.enabled=true" \
  --conf "spark.driver.memory=1g" \
  --conf "spark.executor.memory=1g" \
  "$JOB_FILE" $JOB_ARGS
