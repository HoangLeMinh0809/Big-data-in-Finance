#!/bin/bash
# =============================================================================
# Start AIS Infrastructure + Monitoring UI only (NO automatic backfill)
# User can trigger backfill via Monitoring UI button
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "$ROOT_DIR"

print_container_diagnostics() {
  local container_name="$1"

  echo "[DEBUG] ${container_name} health log:"
  docker inspect -f '{{range .State.Health.Log}}{{println .ExitCode ":" .Output}}{{end}}' "$container_name" 2>/dev/null || true

  echo "[DEBUG] ${container_name} container logs (tail):"
  docker logs --tail 120 "$container_name" 2>&1 || true
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

echo "=== [1/6] Start core infrastructure ==="
docker compose up -d --build zookeeper kafka namenode datanode spark-master spark-worker cassandra

wait_for_healthy kafka 300
wait_for_healthy namenode 300
wait_for_healthy spark-master 300

echo "=== [2/6] Create Kafka topics ==="
bash "$SCRIPT_DIR/create_topics.sh"

echo "=== [3/7] Ensure Iceberg catalog/tables ==="
bash scripts/submit_spark.sh ensure-iceberg

echo "=== [4/7] Start Spark streaming sinks (detached) ==="
DETACH=true STOP_AFTER_BATCH=false bash scripts/submit_spark.sh weather
DETACH=true STOP_AFTER_BATCH=false bash scripts/submit_spark.sh openaq
DETACH=true STOP_AFTER_BATCH=false bash scripts/submit_spark.sh sentinel5p
DETACH=true STOP_AFTER_BATCH=false bash scripts/submit_spark.sh maiac

echo "=== [5/7] Ensure Cassandra schema ==="
docker exec cassandra cqlsh -e "CREATE KEYSPACE IF NOT EXISTS ais_serving WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};" || true
docker exec cassandra cqlsh -e "CREATE TABLE IF NOT EXISTS ais_serving.weather_hourly_by_province_day (province text, day text, event_time timestamp, event_id text, query_date text, location_name text, lat double, lon double, temp_c double, temp_f double, humidity int, wind_kph double, wind_degree int, wind_dir text, precip_mm double, condition_text text, source text, ingest_time text, PRIMARY KEY ((province, day), event_time)) WITH CLUSTERING ORDER BY (event_time DESC);" || true
docker exec cassandra cqlsh -e "CREATE TABLE IF NOT EXISTS ais_serving.openaq_hourly_by_city_parameter_day (city text, parameter text, day text, event_time timestamp, event_id text, location_id bigint, location_name text, provider text, sensor_id bigint, unit text, value double, min double, max double, sd double, coverage_pct double, source text, ingest_time text, PRIMARY KEY ((city, parameter, day), event_time)) WITH CLUSTERING ORDER BY (event_time DESC);" || true

echo "=== [6/7] Run Airflow init then start services ==="
docker compose up airflow-init
docker compose up -d airflow-webserver airflow-scheduler airflow-triggerer || docker start airflow-webserver airflow-scheduler airflow-triggerer

echo "=== [7/7] Start Monitoring UI (with ingest trigger support) ==="
docker compose up -d monitoring-ui

echo ""
echo "✓ AIS Infrastructure started successfully!"
echo ""
echo "Dashboard UIs:"
echo "  NameNode:   http://localhost:9870"
echo "  Spark:      http://localhost:8080"
echo "  Airflow:    http://localhost:8088"
echo "  Monitoring: http://localhost:8501"
echo ""
echo "To backfill data from Monitoring UI:"
echo "  1. Open http://localhost:8501"
echo "  2. Click 'Start 7-Day Backfill DAG' (single button)"
echo "  3. Or use API: curl -X POST 'http://localhost:8501/api/airflow/start-backfill'"
echo ""
echo "To manually backfill (7 days for all sources):"
echo "  bash scripts/backfill_all_sources.sh"
echo ""
echo "To check pipeline status:"
echo "  bash scripts/check_pipeline.sh weather"
echo "  bash scripts/check_pipeline.sh openaq"
echo "  bash scripts/check_pipeline.sh sentinel5p"
echo "  bash scripts/check_pipeline.sh maiac"
