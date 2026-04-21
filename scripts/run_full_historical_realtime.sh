#!/bin/bash
# =============================================================================
# bootstrap_ais.sh
# One-command AIS bootstrap:
# - Start Spark streaming sinks FIRST
# - Bootstrap streams with earliest offsets
# - Backfill all sources AFTER sinks are active
# - Keep Weather + OpenAQ in realtime mode
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "$ROOT_DIR"

LOOKBACK_DAYS="${LOOKBACK_DAYS:-7}"
REALTIME_LOOKBACK_MINUTES="${REALTIME_LOOKBACK_MINUTES:-10}"
REALTIME_POLL_SECONDS="${REALTIME_POLL_SECONDS:-600}"
RESET_STREAM_CHECKPOINTS="${RESET_STREAM_CHECKPOINTS:-true}"
STREAM_PROCESSING_TIME="${STREAM_PROCESSING_TIME:-30 seconds}"
BOOTSTRAP_STARTING_OFFSETS="${BOOTSTRAP_STARTING_OFFSETS:-earliest}"

unset STOP_AFTER_BATCH || true

spark_app_registered() {
  local app_name="$1"

  docker exec -i -e APP_NAME="$app_name" spark-master python3 - <<'PY'
import json
import os
import sys
import urllib.request

app_name = os.environ.get("APP_NAME", "")
try:
    raw = urllib.request.urlopen("http://localhost:8080/json", timeout=10).read().decode("utf-8")
    payload = json.loads(raw)
except Exception:
    sys.exit(1)

for app in payload.get("activeapps", []):
    if app.get("name") == app_name and app.get("state") in {"RUNNING", "WAITING"}:
        sys.exit(0)

sys.exit(1)
PY
}

spark_app_ids_by_name() {
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
    raise SystemExit(0)

for app in payload.get("activeapps", []):
    if app.get("name") == app_name:
        app_id = app.get("id")
        if app_id:
            print(app_id)
PY
}

kill_spark_app_by_name() {
  local app_name="$1"
  local ids

  ids="$(spark_app_ids_by_name "$app_name" || true)"
  if [ -z "$ids" ]; then
    return 0
  fi

  echo "[INFO] Killing Spark app(s) for ${app_name}: ${ids}"
  while IFS= read -r app_id; do
    [ -z "$app_id" ] && continue
    docker exec spark-master /opt/spark/bin/spark-class \
      org.apache.spark.deploy.Client kill spark://spark-master:7077 "$app_id" || true
  done <<< "$ids"

  local elapsed=0
  local timeout_sec=60
  while spark_app_registered "$app_name"; do
    if [ "$elapsed" -ge "$timeout_sec" ]; then
      echo "[WARN] Spark app still visible after kill timeout: ${app_name}"
      break
    fi
    sleep 3
    elapsed=$((elapsed + 3))
  done
}

ensure_spark_app_active() {
  local app_name="$1"
  local job_type="$2"
  local starting_offsets="${3:-earliest}"
  local timeout_sec="${4:-180}"
  local attempts="${5:-3}"
  local attempt=1

  if spark_app_registered "$app_name"; then
    echo "[OK] Spark app active: ${app_name}"
    return 0
  fi

  while [ "$attempt" -le "$attempts" ]; do
    local elapsed=0

    echo "[WARN] Spark app not active, submitting (attempt ${attempt}/${attempts}): ${app_name}"
    KAFKA_STARTING_OFFSETS="$starting_offsets" \
    DETACH=true \
    STOP_AFTER_BATCH=false \
    PROCESSING_TIME="$STREAM_PROCESSING_TIME" \
      bash scripts/submit_spark.sh "$job_type"

    while [ "$elapsed" -lt "$timeout_sec" ]; do
      if spark_app_registered "$app_name"; then
        echo "[OK] Spark app became active: ${app_name}"
        return 0
      fi
      sleep 5
      elapsed=$((elapsed + 5))
    done

    attempt=$((attempt + 1))
  done

  echo "[ERROR] Spark app still not active after ${attempts} attempts: ${app_name}"
  return 1
}

wait_for_healthy() {
  local container_name="$1"
  local timeout_sec="${2:-300}"
  local elapsed=0

  while true; do
    local status
    status="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$container_name" 2>/dev/null || true)"

    if [ "$status" = "healthy" ] || [ "$status" = "running" ]; then
      echo "[OK] $container_name status=$status"
      return 0
    fi

    if [ "$elapsed" -ge "$timeout_sec" ]; then
      echo "[ERROR] Timeout waiting for $container_name (last status=$status)"
      return 1
    fi

    echo "[WAIT] $container_name status=$status (${elapsed}s/${timeout_sec}s)"
    sleep 5
    elapsed=$((elapsed + 5))
  done
}

wait_for_hdfs_parquet() {
  local hdfs_path="$1"
  local timeout_sec="${2:-300}"
  local elapsed=0

  while true; do
    if docker exec namenode bash -lc "hdfs dfs -ls -R '$hdfs_path' 2>/dev/null | grep -q '\.parquet$'"; then
      echo "[OK] Found parquet under ${hdfs_path}"
      return 0
    fi

    if [ "$elapsed" -ge "$timeout_sec" ]; then
      echo "[ERROR] No parquet found under ${hdfs_path} after ${timeout_sec}s"
      docker exec namenode hdfs dfs -ls -R "$hdfs_path" || true
      return 1
    fi

    echo "[WAIT] No parquet yet under ${hdfs_path} (${elapsed}s/${timeout_sec}s)"
    sleep 10
    elapsed=$((elapsed + 10))
  done
}

echo "=== [1/9] Start core infrastructure ==="
docker compose up -d --build zookeeper kafka namenode datanode spark-master spark-worker cassandra

wait_for_healthy kafka 300
wait_for_healthy namenode 300
wait_for_healthy datanode 300
wait_for_healthy spark-master 300

if [ "$RESET_STREAM_CHECKPOINTS" = "true" ]; then
  echo "=== [2/9] Stop old Spark streams + reset checkpoints ==="
  kill_spark_app_by_name "WeatherHistory_Streaming"
  kill_spark_app_by_name "OpenAQHourly_Streaming"
  kill_spark_app_by_name "Sentinel5PSummary_Streaming"
  kill_spark_app_by_name "MAIACSummary_Streaming"

  docker exec namenode hdfs dfs -rm -r -f /checkpoints/weather_history || true
  docker exec namenode hdfs dfs -rm -r -f /checkpoints/openaq_hourly || true
  docker exec namenode hdfs dfs -rm -r -f /checkpoints/sentinel5p_summary || true
  docker exec namenode hdfs dfs -rm -r -f /checkpoints/maiac_summary || true
fi

echo "=== [3/9] Create Kafka topics ==="
bash "$SCRIPT_DIR/create_topics.sh"

echo "=== [4/9] Ensure Iceberg catalog/tables ==="
bash scripts/submit_spark.sh ensure-iceberg

echo "=== [5/9] Start Spark streaming sinks FIRST (bootstrap with earliest) ==="
ensure_spark_app_active "WeatherHistory_Streaming" "weather" "$BOOTSTRAP_STARTING_OFFSETS"
ensure_spark_app_active "OpenAQHourly_Streaming" "openaq" "$BOOTSTRAP_STARTING_OFFSETS"
ensure_spark_app_active "Sentinel5PSummary_Streaming" "sentinel5p" "$BOOTSTRAP_STARTING_OFFSETS"
ensure_spark_app_active "MAIACSummary_Streaming" "maiac" "$BOOTSTRAP_STARTING_OFFSETS"

echo "=== [6/9] Start Airflow + Monitoring ==="
docker compose up airflow-init
docker compose up -d airflow-webserver airflow-scheduler airflow-triggerer monitoring-ui || \
docker start airflow-webserver airflow-scheduler airflow-triggerer monitoring-ui

echo "=== [7/9] Historical backfill: Sentinel-5P ==="
docker compose run --rm \
  --no-deps \
  -e WINDOW_MODE=batch \
  -e BATCH_LOOKBACK_DAYS="${LOOKBACK_DAYS}" \
  sentinel5p-ingest

wait_for_hdfs_parquet "/warehouse/iceberg/satellite/sentinel5p_summary_bronze" 300

echo "=== [8/9] Historical backfill: MAIAC ==="
docker compose run --rm \
  --no-deps \
  -e WINDOW_MODE=batch \
  -e BATCH_LOOKBACK_DAYS="${LOOKBACK_DAYS}" \
  maiac-ingest

wait_for_hdfs_parquet "/warehouse/iceberg/satellite/maiac_summary_bronze" 300

echo "=== [9/9] Historical backfill: Weather ==="
docker compose run --rm \
  --no-deps \
  -e WINDOW_MODE=batch \
  -e BATCH_LOOKBACK_DAYS="${LOOKBACK_DAYS}" \
  ingest

wait_for_hdfs_parquet "/warehouse/iceberg/weather/weather_history_bronze" 300

echo "=== [10/9] Historical backfill: OpenAQ ==="
docker compose run --rm \
  --no-deps \
  -e WINDOW_MODE=batch \
  -e BATCH_LOOKBACK_DAYS="${LOOKBACK_DAYS}" \
  openaq-ingest

wait_for_hdfs_parquet "/warehouse/iceberg/air_quality/openaq_hourly_bronze" 300

echo "=== [11/9] Start realtime loops for Weather + OpenAQ ==="
WEATHER_WINDOW_MODE=realtime \
WEATHER_REALTIME_CONTINUOUS=true \
WEATHER_REALTIME_LOOKBACK_MINUTES="${REALTIME_LOOKBACK_MINUTES}" \
WEATHER_REALTIME_POLL_SECONDS="${REALTIME_POLL_SECONDS}" \
OPENAQ_WINDOW_MODE=realtime \
OPENAQ_REALTIME_CONTINUOUS=true \
OPENAQ_REALTIME_LOOKBACK_MINUTES="${REALTIME_LOOKBACK_MINUTES}" \
OPENAQ_REALTIME_POLL_SECONDS="${REALTIME_POLL_SECONDS}" \
  docker compose -p atmospheric_intelligence_sys---ais up -d --no-recreate ingest openaq-ingest

echo
echo "DONE. Pipeline status checks:"
echo "  bash scripts/check_pipeline.sh weather"
echo "  bash scripts/check_pipeline.sh openaq"
echo "  bash scripts/check_pipeline.sh sentinel5p"
echo "  bash scripts/check_pipeline.sh maiac"
echo
echo "UIs:"
echo "  NameNode:  http://localhost:9870"
echo "  Spark:     http://localhost:8080"
echo "  Airflow:   http://localhost:8088"
echo "  Monitor:   http://localhost:8501"