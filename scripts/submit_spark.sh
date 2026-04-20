#!/bin/bash
# =============================================================================
# Submit AIS Spark jobs (streaming + batch load)
# =============================================================================

set -euo pipefail

JOB_TYPE="${1:-weather}"
DETACH="${DETACH:-false}"

KAFKA_HADOOP_PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-client:3.2.1"
ICEBERG_PACKAGES="${KAFKA_HADOOP_PACKAGES},org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2"
CASSANDRA_PACKAGES="${ICEBERG_PACKAGES},com.datastax.spark:spark-cassandra-connector_2.12:3.5.1"

APP_NAME=""
JOB_FILE=""
JOB_ARGS=()
HDFS_DATA_DIR=""
HDFS_CHECKPOINT_DIR=""
PACKAGES="${ICEBERG_PACKAGES}"

case "$JOB_TYPE" in
  weather)
    APP_NAME="WeatherHistory_Streaming"
    JOB_FILE="/opt/spark-jobs/weather_streaming.py"
    HDFS_DATA_DIR="/data/weather_history"
    HDFS_CHECKPOINT_DIR="/checkpoints/weather_history"
    PACKAGES="${ICEBERG_PACKAGES}"
    ;;
  openaq)
    APP_NAME="OpenAQHourly_Streaming"
    JOB_FILE="/opt/spark-jobs/openaq_hourly_streaming.py"
    HDFS_DATA_DIR="/data/openaq_hourly"
    HDFS_CHECKPOINT_DIR="/checkpoints/openaq_hourly"
    PACKAGES="${ICEBERG_PACKAGES}"
    ;;
  sentinel5p)
    APP_NAME="Sentinel5PSummary_Streaming"
    JOB_FILE="/opt/spark-jobs/sentinel5p_summary_streaming.py"
    HDFS_DATA_DIR="/data/sentinel5p_summary"
    HDFS_CHECKPOINT_DIR="/checkpoints/sentinel5p_summary"
    ;;
  maiac)
    APP_NAME="MAIACSummary_Streaming"
    JOB_FILE="/opt/spark-jobs/maiac_summary_streaming.py"
    HDFS_DATA_DIR="/data/maiac_summary"
    HDFS_CHECKPOINT_DIR="/checkpoints/maiac_summary"
    ;;
  cassandra-weather)
    APP_NAME="IcebergToCassandra_Weather"
    JOB_FILE="/opt/spark-jobs/iceberg_to_cassandra.py"
    JOB_ARGS=("weather")
    HDFS_DATA_DIR="/data/iceberg_to_cassandra"
    HDFS_CHECKPOINT_DIR="/checkpoints/iceberg_to_cassandra"
    PACKAGES="${CASSANDRA_PACKAGES}"
    ;;
  cassandra-openaq)
    APP_NAME="IcebergToCassandra_OpenAQ"
    JOB_FILE="/opt/spark-jobs/iceberg_to_cassandra.py"
    JOB_ARGS=("openaq")
    HDFS_DATA_DIR="/data/iceberg_to_cassandra"
    HDFS_CHECKPOINT_DIR="/checkpoints/iceberg_to_cassandra"
    PACKAGES="${CASSANDRA_PACKAGES}"
    ;;
  *)
    echo "Usage: $0 [weather|openaq|sentinel5p|maiac|cassandra-weather|cassandra-openaq]"
    exit 1
    ;;
esac

echo "=== Create HDFS output paths ==="
docker exec namenode hdfs dfs -mkdir -p "$HDFS_DATA_DIR"
docker exec namenode hdfs dfs -mkdir -p "$HDFS_CHECKPOINT_DIR"
docker exec namenode hdfs dfs -mkdir -p /warehouse/iceberg
docker exec namenode hdfs dfs -chmod -R 777 /data
docker exec namenode hdfs dfs -chmod -R 777 /checkpoints
docker exec namenode hdfs dfs -chmod -R 777 /warehouse

echo
echo "=== Submit Spark Job: $APP_NAME ==="

DOCKER_EXEC_ARGS=()
if [ "$DETACH" = "true" ]; then
  DOCKER_EXEC_ARGS+=("-d")
fi

docker exec "${DOCKER_EXEC_ARGS[@]}" spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --name "$APP_NAME" \
  --packages "$PACKAGES" \
  --conf "spark.hadoop.fs.defaultFS=hdfs://namenode:9000" \
  --conf "spark.sql.adaptive.enabled=true" \
  --conf "spark.driver.memory=1g" \
  --conf "spark.executor.memory=1g" \
  --conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" \
  --conf "spark.sql.catalog.ais=org.apache.iceberg.spark.SparkCatalog" \
  --conf "spark.sql.catalog.ais.type=hadoop" \
  --conf "spark.sql.catalog.ais.warehouse=hdfs://namenode:9000/warehouse/iceberg" \
  --conf "spark.cassandra.connection.host=cassandra" \
  --conf "spark.cassandra.connection.port=9042" \
  "$JOB_FILE" "${JOB_ARGS[@]}"

if [ "$DETACH" = "true" ]; then
  echo "Submitted in detached mode: $APP_NAME"
fi
