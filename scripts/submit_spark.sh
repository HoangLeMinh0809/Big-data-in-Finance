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
  HDFS_DATA_DIR="/data/stock_prices_daily"
  HDFS_CHECKPOINT_DIR="/checkpoints/stock_prices_daily"
elif [ "$JOB_TYPE" = "weather" ]; then
  APP_NAME="WeatherHistory_Streaming"
  JOB_FILE="/opt/spark-jobs/weather_streaming.py"
  HDFS_DATA_DIR="/data/weather_history"
  HDFS_CHECKPOINT_DIR="/checkpoints/weather_history"
else
  echo "Usage: $0 [stock|weather]"
  exit 1
fi

echo "=== Tạo thư mục output trên HDFS ==="
docker exec namenode hdfs dfs -mkdir -p "$HDFS_DATA_DIR"
docker exec namenode hdfs dfs -mkdir -p "$HDFS_CHECKPOINT_DIR"
docker exec namenode hdfs dfs -chmod -R 777 /data
docker exec namenode hdfs dfs -chmod -R 777 /checkpoints

echo ""
echo "=== Submit Spark Streaming Job: $APP_NAME ==="
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --name "$APP_NAME" \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-client:3.2.1 \
  --conf "spark.hadoop.fs.defaultFS=hdfs://namenode:9000" \
  --conf "spark.sql.adaptive.enabled=true" \
  --conf "spark.driver.memory=1g" \
  --conf "spark.executor.memory=1g" \
  "$JOB_FILE"
