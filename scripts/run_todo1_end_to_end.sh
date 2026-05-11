#!/bin/bash
# =============================================================================
# TODO_1 end-to-end runner
#
# Flow:
#   1) Start infrastructure + monitoring UI
#   2) Create Kafka topics and Iceberg tables
#   3) Backfill bronze sources into Kafka and catch them up to Iceberg
#   4) Full-refresh Hanoi silver tables
#   5) Full-refresh Hanoi PM2.5 gold master + training dataset
#
# Usage:
#   START_DATE=2026-03-01 END_DATE=2026-03-31 bash scripts/run_todo1_end_to_end.sh
#
# Optional:
#   LOOKBACK_DAYS=30                         # used when START_DATE is empty
#   RUN_ERA5=true ERA5_START_DATE=... ERA5_END_DATE=...
#   RUN_SENTINEL5P=true|false
#   RUN_MAIAC=true|false
#   RUN_WEATHER=true|false
#   RUN_OPENAQ=true|false
#   RESET_BRONZE_CHECKPOINTS=true|false      # default false to avoid replay duplicates
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "$ROOT_DIR"

today_utc() {
  if date -u +%Y-%m-%d >/dev/null 2>&1; then
    date -u +%Y-%m-%d
    return 0
  fi
  powershell.exe -NoProfile -Command "(Get-Date).ToUniversalTime().ToString('yyyy-MM-dd')" | tr -d '\r'
}

days_before_utc() {
  local base_date="$1"
  local days="$2"

  if date -u -d "${base_date} - ${days} days" +%Y-%m-%d >/dev/null 2>&1; then
    date -u -d "${base_date} - ${days} days" +%Y-%m-%d
    return 0
  fi

  if date -u -v-"${days}"d -j -f "%Y-%m-%d" "${base_date}" +%Y-%m-%d >/dev/null 2>&1; then
    date -u -v-"${days}"d -j -f "%Y-%m-%d" "${base_date}" +%Y-%m-%d
    return 0
  fi

  powershell.exe -NoProfile -Command "([datetime]::Parse('${base_date}')).AddDays(-${days}).ToString('yyyy-MM-dd')" | tr -d '\r'
}

LOOKBACK_DAYS="${LOOKBACK_DAYS:-30}"
END_DATE="${END_DATE:-$(today_utc)}"
START_DATE="${START_DATE:-$(days_before_utc "$END_DATE" "$LOOKBACK_DAYS")}"

WINDOW_START_UTC="${WINDOW_START_UTC:-${START_DATE}T00:00:00Z}"
WINDOW_END_UTC="${WINDOW_END_UTC:-${END_DATE}T23:59:59Z}"

FULL_REFRESH_SILVER_GOLD="${FULL_REFRESH_SILVER_GOLD:-1}"
RESET_BRONZE_CHECKPOINTS="${RESET_BRONZE_CHECKPOINTS:-false}"
KAFKA_STARTING_OFFSETS="${KAFKA_STARTING_OFFSETS:-earliest}"
STOP_REALTIME_AFTER_BACKFILL="${STOP_REALTIME_AFTER_BACKFILL:-true}"

RUN_WEATHER="${RUN_WEATHER:-true}"
RUN_OPENAQ="${RUN_OPENAQ:-true}"
RUN_SENTINEL5P="${RUN_SENTINEL5P:-true}"
RUN_MAIAC="${RUN_MAIAC:-true}"
RUN_ERA5="${RUN_ERA5:-auto}"

ERA5_DATASET_TYPE="${ERA5_DATASET_TYPE:-surface}"
ERA5_START_DATE="${ERA5_START_DATE:-$START_DATE}"
ERA5_END_DATE="${ERA5_END_DATE:-$END_DATE}"

MAIAC_LOCAL_FALLBACK_PATH="${MAIAC_LOCAL_FALLBACK_PATH:-/opt/maiac_data}"
MAIAC_RELAXED_QA="${MAIAC_RELAXED_QA:-0}"
DATASET_VERSION="${DATASET_VERSION:-hanoi_pm25_v1_${START_DATE}_${END_DATE}}"
FEATURE_SET_NAME="${FEATURE_SET_NAME:-hanoi_pm25_core_v1}"

wait_for_healthy() {
  local container_name="$1"
  local timeout_sec="${2:-300}"
  local elapsed=0

  while true; do
    local status
    status="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$container_name" 2>/dev/null || true)"
    if [ "$status" = "healthy" ] || [ "$status" = "running" ]; then
      echo "[OK] ${container_name} status=${status}"
      return 0
    fi
    if [ "$elapsed" -ge "$timeout_sec" ]; then
      echo "[ERROR] Timeout waiting for ${container_name}; last status=${status}"
      docker logs --tail 120 "$container_name" 2>&1 || true
      return 1
    fi
    echo "[WAIT] ${container_name} status=${status} (${elapsed}s/${timeout_sec}s)"
    sleep 5
    elapsed=$((elapsed + 5))
  done
}

run_ingest_service() {
  local service="$1"
  local label="$2"
  shift 2

  echo "=== Backfill ${label} -> Kafka ==="
  docker compose run --rm --no-deps \
    -e WINDOW_MODE=batch \
    -e BATCH_LOOKBACK_DAYS="$LOOKBACK_DAYS" \
    -e WINDOW_START_UTC="$WINDOW_START_UTC" \
    -e WINDOW_END_UTC="$WINDOW_END_UTC" \
    "$@" \
    "$service"
}

catch_up_bronze() {
  local job_type="$1"
  local label="$2"

  echo "=== Kafka -> Iceberg bronze catch-up: ${label} ==="
  STOP_AFTER_BATCH=true \
  KAFKA_STARTING_OFFSETS="$KAFKA_STARTING_OFFSETS" \
    bash scripts/submit_spark.sh "$job_type"
}

run_silver_gold_job() {
  local job_type="$1"
  local label="$2"

  echo "=== ${label} ==="
  START_DATE="$START_DATE" \
  END_DATE="$END_DATE" \
  FULL_REFRESH="$FULL_REFRESH_SILVER_GOLD" \
  DATASET_VERSION="$DATASET_VERSION" \
  FEATURE_SET_NAME="$FEATURE_SET_NAME" \
  MAIAC_LOCAL_FALLBACK_PATH="$MAIAC_LOCAL_FALLBACK_PATH" \
  MAIAC_RELAXED_QA="$MAIAC_RELAXED_QA" \
    bash scripts/submit_spark.sh "$job_type"
}

maybe_reset_bronze_checkpoints() {
  if [ "$RESET_BRONZE_CHECKPOINTS" != "true" ]; then
    echo "[INFO] Keep existing bronze checkpoints (RESET_BRONZE_CHECKPOINTS=${RESET_BRONZE_CHECKPOINTS})"
    return 0
  fi

  echo "=== Reset bronze stream checkpoints ==="
  docker exec namenode hdfs dfs -rm -r -f /checkpoints/weather_history || true
  docker exec namenode hdfs dfs -rm -r -f /checkpoints/openaq_hourly || true
  docker exec namenode hdfs dfs -rm -r -f /checkpoints/sentinel5p_summary || true
  docker exec namenode hdfs dfs -rm -r -f /checkpoints/maiac_summary || true
  docker exec namenode hdfs dfs -rm -r -f /checkpoints/era5_files || true
}

should_run_era5() {
  if [ "$RUN_ERA5" = "true" ]; then
    return 0
  fi
  if [ "$RUN_ERA5" = "false" ]; then
    return 1
  fi
  if [ -n "${CDSAPI_URL:-}" ] || [ -n "${CDSAPI_KEY:-}" ] || [ -f "${HOME:-}/.cdsapirc" ]; then
    return 0
  fi
  return 1
}

echo "=== TODO_1 end-to-end run ==="
echo "Date window: ${START_DATE} -> ${END_DATE}"
echo "UTC window:  ${WINDOW_START_UTC} -> ${WINDOW_END_UTC}"
echo "Dataset:     ${DATASET_VERSION}"
echo

echo "=== [1/8] Start infrastructure + monitoring UI ==="
docker compose up -d --build \
  zookeeper kafka namenode datanode spark-master spark-worker cassandra monitoring-ui

docker compose build ingest openaq-ingest sentinel5p-ingest maiac-ingest

wait_for_healthy kafka 300
wait_for_healthy namenode 300
wait_for_healthy datanode 300
wait_for_healthy spark-master 300

echo "=== [2/8] Create Kafka topics and Iceberg tables ==="
bash scripts/create_topics.sh
bash scripts/submit_spark.sh ensure-iceberg
maybe_reset_bronze_checkpoints

echo "=== [3/8] Backfill bronze source events ==="
if [ "$RUN_WEATHER" = "true" ]; then
  run_ingest_service "ingest" "WeatherAPI/local weather" -e SOURCE_MODE="${WEATHER_SOURCE_MODE:-auto}"
  catch_up_bronze "weather" "WeatherAPI"
fi

if [ "$RUN_OPENAQ" = "true" ]; then
  run_ingest_service "openaq-ingest" "OpenAQ hourly"
  catch_up_bronze "openaq" "OpenAQ"
fi

if [ "$RUN_SENTINEL5P" = "true" ]; then
  run_ingest_service "sentinel5p-ingest" "Sentinel-5P metadata"
  catch_up_bronze "sentinel5p" "Sentinel-5P"
fi

if [ "$RUN_MAIAC" = "true" ]; then
  run_ingest_service "maiac-ingest" "MAIAC metadata"
  catch_up_bronze "maiac" "MAIAC"
fi

if should_run_era5; then
  echo "=== Backfill ERA5 raw NetCDF metadata ==="
  ERA5_START_DATE="$ERA5_START_DATE" \
  ERA5_END_DATE="$ERA5_END_DATE" \
  ERA5_DATASET_TYPE="$ERA5_DATASET_TYPE" \
    bash scripts/submit_spark.sh era5-ingest
  catch_up_bronze "era5-files" "ERA5 files"
else
  echo "[WARN] Skip ERA5 ingest (RUN_ERA5=${RUN_ERA5}); set RUN_ERA5=true plus CDS credentials to include it."
fi

echo "=== [4/8] Build Hanoi silver tables ==="
run_silver_gold_job "hanoi-openaq-silver" "OpenAQ station/hourly silver"
run_silver_gold_job "hanoi-weather-silver" "WeatherAPI proxy silver"
run_silver_gold_job "era5-surface-hanoi-silver" "ERA5 surface silver"
run_silver_gold_job "sentinel5p-hanoi-silver" "Sentinel-5P daily silver"
run_silver_gold_job "maiac-hanoi-silver" "MAIAC daily AOD silver"

echo "=== [5/8] Build Hanoi gold datasets ==="
run_silver_gold_job "hanoi-master-features-gold" "PM2.5 master feature gold"
run_silver_gold_job "hanoi-training-dataset-gold" "PM2.5 training dataset gold"

echo "=== [6/8] Optional baseline training ==="
if [ "${RUN_BASELINE_TRAINING:-false}" = "true" ]; then
  DATASET_VERSION="$DATASET_VERSION" \
  FEATURE_SET_NAME="$FEATURE_SET_NAME" \
    bash scripts/submit_spark.sh hanoi-train-baseline
else
  echo "[INFO] Skip baseline model training (RUN_BASELINE_TRAINING=false)"
fi

echo "=== [7/8] Stop realtime ingest services if requested ==="
if [ "$STOP_REALTIME_AFTER_BACKFILL" = "true" ]; then
  docker compose stop ingest openaq-ingest sentinel5p-ingest maiac-ingest >/dev/null 2>&1 || true
fi

echo "=== [8/8] Done ==="
echo "Monitoring UI: http://localhost:8501"
echo "Spark UI:      http://localhost:8080"
echo "NameNode UI:   http://localhost:9870"
echo
echo "Useful checks:"
echo "  bash scripts/check_pipeline.sh weather"
echo "  bash scripts/check_pipeline.sh openaq"
echo "  bash scripts/check_pipeline.sh sentinel5p"
echo "  bash scripts/check_pipeline.sh maiac"
echo "  bash scripts/submit_spark.sh ensure-iceberg"
