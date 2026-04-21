#!/bin/bash
# =============================================================================
# End-to-end pipeline health check
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LAG_CHECK_SCRIPT="${SCRIPT_DIR}/airflow/check_kafka_lag.sh"
SPARK_MASTER_API="${SPARK_MASTER_API:-http://spark-master:8080/json}"

spark_app_registered() {
  local app_name="$1"

  docker exec -i -e APP_NAME="$app_name" spark-master python3 - <<'PY'
import json
import os
import urllib.request

app_name = os.environ.get("APP_NAME", "")
try:
  raw = urllib.request.urlopen("http://localhost:8080/json", timeout=10).read().decode("utf-8")
  payload = json.loads(raw)
except Exception:
  raise SystemExit(1)

for app in payload.get("activeapps", []):
  if app.get("name") == app_name and app.get("state") in {"RUNNING", "WAITING"}:
    raise SystemExit(0)

raise SystemExit(1)
PY
}

PIPELINE="${1:-weather}"

if [ "$PIPELINE" = "openaq" ]; then
  TOPIC="openaq-hourly"
  ICEBERG_DATA_DIR="/warehouse/iceberg/air_quality/openaq_hourly_bronze"
  HDFS_CHECKPOINT_DIR="/checkpoints/openaq_hourly"
  APP_NAME="OpenAQHourly_Streaming"
  GROUP_ID="ais-stream-openaq"
elif [ "$PIPELINE" = "weather" ]; then
  TOPIC="weather_history"
  ICEBERG_DATA_DIR="/warehouse/iceberg/weather/weather_history_bronze"
  HDFS_CHECKPOINT_DIR="/checkpoints/weather_history"
  APP_NAME="WeatherHistory_Streaming"
  GROUP_ID="ais-stream-weather"
elif [ "$PIPELINE" = "sentinel5p" ]; then
  TOPIC="sentinel5p-summary"
  ICEBERG_DATA_DIR="/warehouse/iceberg/satellite/sentinel5p_summary_bronze"
  HDFS_CHECKPOINT_DIR="/checkpoints/sentinel5p_summary"
  APP_NAME="Sentinel5PSummary_Streaming"
  GROUP_ID="ais-stream-sentinel5p"
elif [ "$PIPELINE" = "maiac" ]; then
  TOPIC="maiac-summary"
  ICEBERG_DATA_DIR="/warehouse/iceberg/satellite/maiac_summary_bronze"
  HDFS_CHECKPOINT_DIR="/checkpoints/maiac_summary"
  APP_NAME="MAIACSummary_Streaming"
  GROUP_ID="ais-stream-maiac"
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
echo "[1/6] Kafka - topic and message count"
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
echo "[2/6] Kafka - first 3 messages (timeout 5s)"
docker exec kafka kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic "$TOPIC" \
  --from-beginning \
  --max-messages 3 \
  --timeout-ms 5000 2>/dev/null || echo "Cannot read messages (topic may be empty)"

# 3. Check Iceberg data path
echo ""
echo "[3/6] HDFS - Iceberg table directory"
docker exec namenode hdfs dfs -ls -R "$ICEBERG_DATA_DIR/" 2>/dev/null \
  || echo "No Iceberg data found yet (stream may not have committed files)"

# 4. Check HDFS checkpoint
echo ""
echo "[4/6] HDFS - checkpoint directory"
docker exec namenode hdfs dfs -ls "$HDFS_CHECKPOINT_DIR/" 2>/dev/null \
  || echo "No checkpoint found (Spark may not be running yet)"

# 5. Check Spark app status
echo ""
echo "[5/6] Spark - active application"
if spark_app_registered "$APP_NAME"; then
  echo "Spark app is active: $APP_NAME"
else
  echo "Spark app not found in active list: $APP_NAME"
fi

# 6. Kafka lag for stream consumer group
echo ""
echo "[6/6] Kafka - consumer lag"
if [ ! -f "$LAG_CHECK_SCRIPT" ]; then
  echo "Lag check script not found or not executable: $LAG_CHECK_SCRIPT"
  echo "Skipping lag check"
else
  bash "$LAG_CHECK_SCRIPT" "$GROUP_ID" "$TOPIC" 50000 \
    || echo "Lag check failed or consumer group not ready"
fi

echo ""
echo "============================================"
echo "  CHECK COMPLETED"
echo "============================================"
