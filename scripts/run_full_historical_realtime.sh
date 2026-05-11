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
ENABLE_AIRFLOW="${ENABLE_AIRFLOW:-false}"
ENABLE_MONITORING="${ENABLE_MONITORING:-false}"

# ERA5 (optional in this bootstrap)
ENABLE_ERA5="${ENABLE_ERA5:-false}"
ERA5_DATASET_TYPE="${ERA5_DATASET_TYPE:-surface}"
ERA5_START_DATE="${ERA5_START_DATE:-}"
ERA5_END_DATE="${ERA5_END_DATE:-}"

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
  if app.get("name") == app_name and app.get("state") == "RUNNING":
        sys.exit(0)

sys.exit(1)
PY
}

spark_app_state() {
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
    print(app.get("state", "UNKNOWN"))
    raise SystemExit(0)
PY
}

spark_app_present() {
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
  if app.get("name") == app_name:
    sys.exit(0)

sys.exit(1)
PY
}

print_spark_cluster_snapshot() {
  docker exec -i spark-master python3 - <<'PY'
import json
import urllib.request

try:
  raw = urllib.request.urlopen("http://localhost:8080/json", timeout=10).read().decode("utf-8")
  payload = json.loads(raw)
except Exception as exc:
  print(f"[WARN] Unable to query Spark master state: {exc}")
  raise SystemExit(0)

print("[INFO] Spark active apps:")
for app in payload.get("activeapps", []):
  print(f"  - {app.get('name')} ({app.get('id')}): state={app.get('state')} cores={app.get('cores')}")

print("[INFO] Spark workers:")
for worker in payload.get("workers", []):
  print(
    "  - {wid}: coresUsed={used}/{total} memUsedMB={mused}/{mtotal}".format(
      wid=worker.get("id"),
      used=worker.get("coresused"),
      total=worker.get("cores"),
      mused=worker.get("memoryused"),
      mtotal=worker.get("memory"),
    )
  )
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

  # Fallback for client-mode detached drivers that may survive app-id kill.
  docker exec spark-master sh -lc "pkill -f -- '--name ${app_name}'" >/dev/null 2>&1 || true

  local elapsed=0
  local timeout_sec=60
  while spark_app_present "$app_name"; do
    if [ "$elapsed" -ge "$timeout_sec" ]; then
      echo "[WARN] Spark app still visible after kill timeout: ${app_name}"
      break
    fi
    sleep 3
    elapsed=$((elapsed + 3))
  done
}

ensure_exclusive_stream_resources() {
  local target_app="$1"
  local stream_apps=(
    "WeatherHistory_Streaming"
    "OpenAQHourly_Streaming"
    "Sentinel5PSummary_Streaming"
    "MAIACSummary_Streaming"
  )

  for app in "${stream_apps[@]}"; do
    if [ "$app" != "$target_app" ]; then
      kill_spark_app_by_name "$app"
    fi
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
    local submitted_this_attempt=false

    if spark_app_present "$app_name"; then
      local existing_state
      existing_state="$(spark_app_state "$app_name" || true)"
      echo "[WARN] Spark app already present (state=${existing_state:-UNKNOWN}), waiting instead of duplicate submit: ${app_name}"
    else
      echo "[WARN] Spark app not active, submitting (attempt ${attempt}/${attempts}): ${app_name}"
      KAFKA_STARTING_OFFSETS="$starting_offsets" \
      DETACH=true \
      STOP_AFTER_BATCH=false \
      PROCESSING_TIME="$STREAM_PROCESSING_TIME" \
        bash scripts/submit_spark.sh "$job_type"
      submitted_this_attempt=true
    fi

    while [ "$elapsed" -lt "$timeout_sec" ]; do
      if spark_app_registered "$app_name"; then
        echo "[OK] Spark app became active: ${app_name}"
        return 0
      fi

      local state
      state="$(spark_app_state "$app_name" || true)"
      if [ "$state" = "WAITING" ]; then
        echo "[WAIT] Spark app is WAITING for resources: ${app_name}"
      fi

      sleep 5
      elapsed=$((elapsed + 5))
    done

    if spark_app_present "$app_name"; then
      echo "[WARN] Spark app did not reach RUNNING; terminating stale instance before retry: ${app_name}"
      kill_spark_app_by_name "$app_name"
    elif [ "$submitted_this_attempt" = "false" ]; then
      echo "[WARN] Spark app disappeared while waiting; will retry submit: ${app_name}"
    fi

    echo "[WARN] Spark app state snapshot after timeout for ${app_name}:"
    print_spark_cluster_snapshot

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
      print_container_diagnostics "$container_name"
      return 1
    fi

    echo "[WAIT] $container_name status=$status (${elapsed}s/${timeout_sec}s)"
    sleep 5
    elapsed=$((elapsed + 5))
  done
}

print_container_diagnostics() {
  local container_name="$1"

  echo "[DEBUG] ${container_name} health log:"
  docker inspect -f '{{range .State.Health.Log}}{{println .ExitCode ":" .Output}}{{end}}' "$container_name" 2>/dev/null || true

  echo "[DEBUG] ${container_name} container logs (tail):"
  docker logs --tail 120 "$container_name" 2>&1 || true
}

wait_for_hdfs_parquet() {
  local hdfs_path="$1"
  local timeout_sec="${2:-300}"
  local elapsed=0

  while true; do
    # Use awk instead of grep -q to avoid pipefail false negatives on early pipe close.
    if docker exec namenode hdfs dfs -ls -R "$hdfs_path" 2>/dev/null | awk '/\.parquet$/ { found=1 } END { exit(found ? 0 : 1) }'; then
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
  kill_spark_app_by_name "ERA5Files_Streaming"

  docker exec namenode hdfs dfs -rm -r -f /checkpoints/weather_history || true
  docker exec namenode hdfs dfs -rm -r -f /checkpoints/openaq_hourly || true
  docker exec namenode hdfs dfs -rm -r -f /checkpoints/sentinel5p_summary || true
  docker exec namenode hdfs dfs -rm -r -f /checkpoints/maiac_summary || true
  docker exec namenode hdfs dfs -rm -r -f /checkpoints/era5_files || true
fi

echo "=== [3/9] Create Kafka topics ==="
bash "$SCRIPT_DIR/create_topics.sh"

echo "=== [4/9] Ensure Iceberg catalog/tables ==="
bash scripts/submit_spark.sh ensure-iceberg

if [ "$ENABLE_ERA5" = "true" ]; then
  echo "=== [5/9] Historical backfill: ERA5 metadata (download + Kafka + Iceberg) ==="

  if [ -z "$ERA5_START_DATE" ] || [ -z "$ERA5_END_DATE" ]; then
    echo "[ERROR] ENABLE_ERA5=true but ERA5_START_DATE/ERA5_END_DATE are empty"
    exit 1
  fi

  # Start metadata consumer (writes to Iceberg bronze)
  ensure_exclusive_stream_resources "ERA5Files_Streaming"
  ensure_spark_app_active "ERA5Files_Streaming" "era5-files" "$BOOTSTRAP_STARTING_OFFSETS"

  # Run ingest (downloads NetCDF -> HDFS raw + sends Kafka events)
  ERA5_START_DATE="$ERA5_START_DATE" \
  ERA5_END_DATE="$ERA5_END_DATE" \
  ERA5_DATASET_TYPE="$ERA5_DATASET_TYPE" \
    bash scripts/submit_spark.sh era5-ingest

  # One-shot metadata sink flush
  STOP_AFTER_BATCH=true \
  KAFKA_STARTING_OFFSETS=earliest \
    bash scripts/submit_spark.sh era5-files

  kill_spark_app_by_name "ERA5Files_Streaming"

  echo "[OK] ERA5 metadata backfill done"
else
  echo "=== [5/9] Skip ERA5 (ENABLE_ERA5=${ENABLE_ERA5}) ==="
fi

echo "=== [5/9] Prepare Spark streaming consumers (on-demand per source) ==="

echo "=== [6/9] Optional services (Monitoring/Airflow) ==="
if [ "$ENABLE_MONITORING" = "true" ]; then
  docker compose up -d monitoring-ui || docker start monitoring-ui
else
  echo "[INFO] Skip Monitoring UI startup (ENABLE_MONITORING=${ENABLE_MONITORING})"
fi

if [ "$ENABLE_AIRFLOW" = "true" ]; then
  echo "[INFO] ENABLE_AIRFLOW=true -> starting Airflow services"
  docker compose up airflow-init
  docker compose up -d airflow-webserver airflow-scheduler airflow-triggerer || \
  docker start airflow-webserver airflow-scheduler airflow-triggerer
else
  echo "[INFO] Skip Airflow startup (ENABLE_AIRFLOW=${ENABLE_AIRFLOW})"
fi

echo "=== [7/9] Historical backfill: Sentinel-5P ==="
ensure_exclusive_stream_resources "Sentinel5PSummary_Streaming"
ensure_spark_app_active "Sentinel5PSummary_Streaming" "sentinel5p" "$BOOTSTRAP_STARTING_OFFSETS"

docker compose run --rm \
  --no-deps \
  -e WINDOW_MODE=batch \
  -e BATCH_LOOKBACK_DAYS="${LOOKBACK_DAYS}" \
  sentinel5p-ingest

wait_for_hdfs_parquet "/warehouse/iceberg/satellite/sentinel5p_summary_bronze" 300
kill_spark_app_by_name "Sentinel5PSummary_Streaming"

echo "=== [8/9] Historical backfill: MAIAC ==="
ensure_exclusive_stream_resources "MAIACSummary_Streaming"
ensure_spark_app_active "MAIACSummary_Streaming" "maiac" "$BOOTSTRAP_STARTING_OFFSETS"

docker compose run --rm \
  --no-deps \
  -e WINDOW_MODE=batch \
  -e BATCH_LOOKBACK_DAYS="${LOOKBACK_DAYS}" \
  maiac-ingest

wait_for_hdfs_parquet "/warehouse/iceberg/satellite/maiac_summary_bronze" 300
kill_spark_app_by_name "MAIACSummary_Streaming"

echo "=== [9/9] Historical backfill: Weather ==="
ensure_exclusive_stream_resources "WeatherHistory_Streaming"
ensure_spark_app_active "WeatherHistory_Streaming" "weather" "$BOOTSTRAP_STARTING_OFFSETS"

docker compose run --rm \
  --no-deps \
  -e WINDOW_MODE=batch \
  -e BATCH_LOOKBACK_DAYS="${LOOKBACK_DAYS}" \
  ingest

wait_for_hdfs_parquet "/warehouse/iceberg/weather/weather_history_bronze" 300

echo "=== [10/9] Historical backfill: OpenAQ ==="
ensure_exclusive_stream_resources "OpenAQHourly_Streaming"
ensure_spark_app_active "OpenAQHourly_Streaming" "openaq" "$BOOTSTRAP_STARTING_OFFSETS"

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
if [ "$ENABLE_AIRFLOW" = "true" ]; then
  echo "  Airflow:   http://localhost:8088"
fi
if [ "$ENABLE_MONITORING" = "true" ]; then
  echo "  Monitor:   http://localhost:8501"
fi
