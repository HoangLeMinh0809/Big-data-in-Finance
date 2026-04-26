#!/bin/bash
set -euo pipefail

JOB_TYPE="${1:-}"
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
  if app.get("name") == app_name and app.get("state") == "RUNNING":
    raise SystemExit(0)

raise SystemExit(1)
PY
}

if [ -z "$JOB_TYPE" ]; then
  echo "Usage: $0 <weather|openaq|sentinel5p|maiac>" >&2
  exit 1
fi

case "$JOB_TYPE" in
  weather)
    APP_NAME="WeatherHistory_Streaming"
    ;;
  openaq)
    APP_NAME="OpenAQHourly_Streaming"
    ;;
  sentinel5p)
    APP_NAME="Sentinel5PSummary_Streaming"
    ;;
  maiac)
    APP_NAME="MAIACSummary_Streaming"
    ;;
  *)
    echo "Unsupported stream job type: $JOB_TYPE" >&2
    exit 1
    ;;
esac

if spark_app_registered "$APP_NAME"; then
  echo "[OK] Spark streaming app already running: ${APP_NAME}"
  exit 0
fi

echo "[INFO] Spark app missing, starting: ${APP_NAME}"
DETACH=true STOP_AFTER_BATCH=false bash /opt/ais/scripts/submit_spark.sh "$JOB_TYPE"

for i in $(seq 1 12); do
  if spark_app_registered "$APP_NAME"; then
    echo "[OK] Spark streaming app started: ${APP_NAME}"
    exit 0
  fi
  echo "[WAIT] Waiting for Spark app registration (${i}/12): ${APP_NAME}"
  sleep 5
done

echo "[ERROR] Spark streaming app did not appear in Spark master: ${APP_NAME}" >&2
exit 1
