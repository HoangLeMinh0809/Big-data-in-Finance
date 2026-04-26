#!/bin/bash
# =============================================================================
# Backfill all sources with 7 days of historical data
# Use this if you want to manually backfill instead of clicking UI
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "$ROOT_DIR"

LOOKBACK_DAYS="${LOOKBACK_DAYS:-7}"
REFRESH_CASSANDRA="${REFRESH_CASSANDRA:-true}"

echo "=== Backfill all sources (${LOOKBACK_DAYS} days) ==="
echo ""

echo "[1/4] Sentinel-5P backfill..."
docker compose run --rm \
  --no-deps \
  -e WINDOW_MODE=batch \
  -e BATCH_LOOKBACK_DAYS="${LOOKBACK_DAYS}" \
  sentinel5p-ingest

echo "[1/4] Sentinel-5P processing -> Iceberg..."
KAFKA_STARTING_OFFSETS=earliest STOP_AFTER_BATCH=true bash scripts/submit_spark.sh sentinel5p

echo ""
echo "[2/4] MAIAC backfill..."
docker compose run --rm \
  --no-deps \
  -e WINDOW_MODE=batch \
  -e BATCH_LOOKBACK_DAYS="${LOOKBACK_DAYS}" \
  maiac-ingest

echo "[2/4] MAIAC processing -> Iceberg..."
KAFKA_STARTING_OFFSETS=earliest STOP_AFTER_BATCH=true bash scripts/submit_spark.sh maiac

echo ""
echo "[3/4] Weather backfill..."
docker compose run --rm \
  --no-deps \
  -e WINDOW_MODE=batch \
  -e BATCH_LOOKBACK_DAYS="${LOOKBACK_DAYS}" \
  ingest

echo "[3/4] Weather processing -> Iceberg..."
KAFKA_STARTING_OFFSETS=earliest STOP_AFTER_BATCH=true bash scripts/submit_spark.sh weather

echo ""
echo "[4/4] OpenAQ backfill..."
docker compose run --rm \
  --no-deps \
  -e WINDOW_MODE=batch \
  -e BATCH_LOOKBACK_DAYS="${LOOKBACK_DAYS}" \
  openaq-ingest

echo "[4/4] OpenAQ processing -> Iceberg..."
KAFKA_STARTING_OFFSETS=earliest STOP_AFTER_BATCH=true bash scripts/submit_spark.sh openaq

if [ "$REFRESH_CASSANDRA" = "true" ]; then
  echo ""
  echo "[Serving] Refresh Cassandra tables from Iceberg..."
  docker exec cassandra cqlsh -e "CREATE KEYSPACE IF NOT EXISTS ais_serving WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};" || true
  docker exec cassandra cqlsh -e "CREATE TABLE IF NOT EXISTS ais_serving.weather_hourly_by_province_day (province text, day text, event_time timestamp, event_id text, query_date text, location_name text, lat double, lon double, temp_c double, temp_f double, humidity int, wind_kph double, wind_degree int, wind_dir text, precip_mm double, condition_text text, source text, ingest_time text, PRIMARY KEY ((province, day), event_time)) WITH CLUSTERING ORDER BY (event_time DESC);" || true
  docker exec cassandra cqlsh -e "CREATE TABLE IF NOT EXISTS ais_serving.openaq_hourly_by_city_parameter_day (city text, parameter text, day text, event_time timestamp, event_id text, location_id bigint, location_name text, provider text, sensor_id bigint, unit text, value double, min double, max double, sd double, coverage_pct double, source text, ingest_time text, PRIMARY KEY ((city, parameter, day), event_time)) WITH CLUSTERING ORDER BY (event_time DESC);" || true
  bash scripts/submit_spark.sh cassandra-weather
  bash scripts/submit_spark.sh cassandra-openaq
fi

echo ""
echo "✓ Backfill completed!"
echo ""
echo "Check results:"
echo "  bash scripts/check_pipeline.sh weather"
echo "  bash scripts/check_pipeline.sh openaq"
echo "  bash scripts/check_pipeline.sh sentinel5p"
echo "  bash scripts/check_pipeline.sh maiac"
