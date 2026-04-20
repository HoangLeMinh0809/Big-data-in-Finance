#!/bin/bash
# =============================================================================
# One-command AIS bootstrap:
# - Backfill 30 days for all sources into HDFS
# - Keep Weather + OpenAQ in realtime mode
# - MAIAC and Sentinel-5P run as batch metadata pipelines
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "$ROOT_DIR"

LOOKBACK_DAYS="${LOOKBACK_DAYS:-30}"
REALTIME_LOOKBACK_MINUTES="${REALTIME_LOOKBACK_MINUTES:-10}"
REALTIME_POLL_SECONDS="${REALTIME_POLL_SECONDS:-600}"

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

echo "=== [1/7] Start core infrastructure ==="
docker compose up -d --build zookeeper kafka namenode datanode spark-master spark-worker cassandra

wait_for_healthy kafka 300
wait_for_healthy namenode 300
wait_for_healthy spark-master 300

echo "=== [2/7] Create Kafka topics ==="
bash "$SCRIPT_DIR/create_topics.sh"

echo "=== [3/7] Start Spark streaming sinks (detached) ==="
DETACH=true bash scripts/submit_spark.sh weather
DETACH=true bash scripts/submit_spark.sh openaq
DETACH=true bash scripts/submit_spark.sh sentinel5p
DETACH=true bash scripts/submit_spark.sh maiac

echo "=== [4/7] Historical backfill: Weather (last ${LOOKBACK_DAYS} days) ==="
docker compose run --rm \
  -e WINDOW_MODE=batch \
  -e BATCH_LOOKBACK_DAYS="${LOOKBACK_DAYS}" \
  ingest

echo "=== [5/7] Historical backfill: OpenAQ, Sentinel-5P, MAIAC ==="
docker compose run --rm \
  -e WINDOW_MODE=batch \
  -e BATCH_LOOKBACK_DAYS="${LOOKBACK_DAYS}" \
  openaq-ingest

docker compose run --rm \
  -e WINDOW_MODE=batch \
  -e BATCH_LOOKBACK_DAYS="${LOOKBACK_DAYS}" \
  sentinel5p-ingest

docker compose run --rm \
  -e WINDOW_MODE=batch \
  -e BATCH_LOOKBACK_DAYS="${LOOKBACK_DAYS}" \
  maiac-ingest

echo "=== [6/7] Start realtime loops for Weather + OpenAQ ==="
WEATHER_WINDOW_MODE=realtime \
WEATHER_REALTIME_CONTINUOUS=true \
WEATHER_REALTIME_LOOKBACK_MINUTES="${REALTIME_LOOKBACK_MINUTES}" \
WEATHER_REALTIME_POLL_SECONDS="${REALTIME_POLL_SECONDS}" \
OPENAQ_WINDOW_MODE=realtime \
OPENAQ_REALTIME_CONTINUOUS=true \
OPENAQ_REALTIME_LOOKBACK_MINUTES="${REALTIME_LOOKBACK_MINUTES}" \
OPENAQ_REALTIME_POLL_SECONDS="${REALTIME_POLL_SECONDS}" \
  docker compose up -d ingest openaq-ingest

echo "=== [7/7] Start Airflow + Monitoring ==="
docker compose up -d airflow-postgres airflow-init airflow-webserver airflow-scheduler airflow-triggerer monitoring-ui

echo ""
echo "DONE. Pipeline status checks:"
echo "  bash scripts/check_pipeline.sh weather"
echo "  bash scripts/check_pipeline.sh openaq"
echo "  bash scripts/check_pipeline.sh sentinel5p"
echo "  bash scripts/check_pipeline.sh maiac"
echo ""
echo "UIs:"
echo "  NameNode:  http://localhost:9870"
echo "  Spark:     http://localhost:8080"
echo "  Airflow:   http://localhost:8088"
echo "  Monitor:   http://localhost:8501"
