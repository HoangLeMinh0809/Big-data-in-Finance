#!/bin/bash
# =============================================================================
# End-to-end pipeline health check
# =============================================================================

set -e

PIPELINE="${1:-weather}"

if [ "$PIPELINE" = "openaq" ]; then
  TOPIC="openaq-hourly"
  HDFS_DATA_DIR="/data/openaq_hourly"
  HDFS_CHECKPOINT_DIR="/checkpoints/openaq_hourly"
  APP_NAME="OpenAQHourly_Streaming"
elif [ "$PIPELINE" = "weather" ]; then
  TOPIC="weather_history"
  HDFS_DATA_DIR="/data/weather_history"
  HDFS_CHECKPOINT_DIR="/checkpoints/weather_history"
  APP_NAME="WeatherHistory_Streaming"
elif [ "$PIPELINE" = "sentinel5p" ]; then
  TOPIC="sentinel5p-summary"
  HDFS_DATA_DIR="/data/sentinel5p_summary"
  HDFS_CHECKPOINT_DIR="/checkpoints/sentinel5p_summary"
  APP_NAME="Sentinel5PSummary_Streaming"
elif [ "$PIPELINE" = "maiac" ]; then
  TOPIC="maiac-summary"
  HDFS_DATA_DIR="/data/maiac_summary"
  HDFS_CHECKPOINT_DIR="/checkpoints/maiac_summary"
  APP_NAME="MAIACSummary_Streaming"
else
  echo "Usage: $0 [openaq|weather|sentinel5p|maiac]"
  exit 1
fi

echo "============================================"
echo "  PIPELINE HEALTH CHECK"
echo "  Pipeline: $PIPELINE"
echo "============================================"

# 1. Check Kafka topic
echo ""
echo "[1/5] Kafka - topic and message count"
echo "--- Topics ---"
docker exec kafka kafka-topics --list --bootstrap-server kafka:9092

echo ""
echo "--- Message count trong $TOPIC ---"
docker exec kafka kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list kafka:9092 \
  --topic "$TOPIC" \
  --time -1 2>/dev/null || echo "Topic not found or has no messages"

# 2. Xem sample messages
echo ""
echo "[2/5] Kafka - first 3 messages (timeout 5s)"
docker exec kafka kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic "$TOPIC" \
  --from-beginning \
  --max-messages 3 \
  --timeout-ms 5000 2>/dev/null || echo "Cannot read messages (topic may be empty)"

# 3. Check HDFS output
echo ""
echo "[3/5] HDFS - output directory"
docker exec namenode hdfs dfs -ls -R "$HDFS_DATA_DIR/" 2>/dev/null \
  || echo "No data found in HDFS (Spark may not be running yet)"

# 4. Check HDFS checkpoint
echo ""
echo "[4/5] HDFS - checkpoint directory"
docker exec namenode hdfs dfs -ls "$HDFS_CHECKPOINT_DIR/" 2>/dev/null \
  || echo "No checkpoint found (Spark may not be running yet)"

# 5. Check Spark app status
echo ""
echo "[5/5] Spark - active application"
SPARK_APPS_JSON="$(curl -fsS http://localhost:8080/json 2>/dev/null || true)"
if [ -z "$SPARK_APPS_JSON" ]; then
  echo "Cannot query Spark Master API at http://localhost:8080/json"
elif echo "$SPARK_APPS_JSON" | grep -q "\"name\":\"$APP_NAME\""; then
  echo "Spark app is active: $APP_NAME"
else
  echo "Spark app not found in active list: $APP_NAME"
fi

echo ""
echo "============================================"
echo "  CHECK COMPLETED"
echo "============================================"
