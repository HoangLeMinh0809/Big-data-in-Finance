#!/bin/bash
set -e

echo "=== Ensure HDFS folders ==="
docker exec namenode hdfs dfs -mkdir -p /data/openaq_hourly
docker exec namenode hdfs dfs -mkdir -p /checkpoints/openaq_hourly
docker exec namenode hdfs dfs -chmod -R 777 /data
docker exec namenode hdfs dfs -chmod -R 777 /checkpoints

echo "=== Submit Spark Streaming Job (OpenAQ) ==="
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --name "OpenAQHourly_Streaming" \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-client:3.2.1 \
  --conf "spark.hadoop.fs.defaultFS=hdfs://namenode:9000" \
  --conf "spark.sql.adaptive.enabled=true" \
  --conf "spark.driver.memory=1g" \
  --conf "spark.executor.memory=1g" \
  /opt/spark-jobs/openaq_hourly_streaming.py
