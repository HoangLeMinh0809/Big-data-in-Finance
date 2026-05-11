#!/usr/bin/env bash
set -euo pipefail

export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL="*"

NAMENODE_CONTAINER="${NAMENODE_CONTAINER:-namenode}"
KAFKA_CONTAINER="${KAFKA_CONTAINER:-kafka}"

run_hdfs() {
  docker exec "$NAMENODE_CONTAINER" hdfs dfs "$@"
}

path_exists() {
  run_hdfs -test -e "$1" >/dev/null 2>&1
}

dir_exists() {
  run_hdfs -test -d "$1" >/dev/null 2>&1
}

file_count() {
  local path="$1"
  local pattern="$2"

  if ! path_exists "$path"; then
    echo "0"
    return 0
  fi

  run_hdfs -find "$path" -name "$pattern" 2>/dev/null | wc -l | tr -d ' '
}

print_du() {
  local path="$1"

  if path_exists "$path"; then
    run_hdfs -du -h -s "$path" 2>/dev/null || true
  else
    echo "missing  $path"
  fi
}

print_recent_files() {
  local path="$1"
  local limit="${2:-8}"

  if ! path_exists "$path"; then
    echo "  missing"
    return 0
  fi

  run_hdfs -ls -R "$path" 2>/dev/null \
    | awk '$1 ~ /^-/ {print $6, $7, $5, $8}' \
    | sort \
    | tail -n "$limit" \
    | sed 's/^/  /' \
    || true
}

print_dir_overview() {
  local path="$1"

  if ! dir_exists "$path"; then
    echo "  missing"
    return 0
  fi

  run_hdfs -ls "$path" 2>/dev/null | sed 's/^/  /' || true
}

print_table() {
  local topic="$1"
  local table_path="$2"
  local checkpoint_path="$3"

  echo
  echo "=== Topic: ${topic} ==="
  echo "table_path:      ${table_path}"
  echo "checkpoint_path: ${checkpoint_path}"

  if ! dir_exists "$table_path"; then
    echo "table_exists: no"
    return 0
  fi

  echo "table_exists: yes"
  echo
  echo "[size]"
  print_du "$table_path" | sed 's/^/  table      /'
  print_du "${table_path}/data" | sed 's/^/  data       /'
  print_du "${table_path}/metadata" | sed 's/^/  metadata   /'
  print_du "$checkpoint_path" | sed 's/^/  checkpoint /'

  echo
  echo "[count]"
  run_hdfs -count -h "$table_path" 2>/dev/null | sed 's/^/  table      /' || true
  if dir_exists "${table_path}/data"; then
    run_hdfs -count -h "${table_path}/data" 2>/dev/null | sed 's/^/  data       /' || true
  fi
  echo "  parquet_files=$(file_count "$table_path" "*.parquet")"
  echo "  metadata_json=$(file_count "$table_path" "*.metadata.json")"
  echo "  manifest_files=$(file_count "$table_path" "*.avro")"

  echo
  echo "[top-level]"
  print_dir_overview "$table_path"

  echo
  echo "[data overview]"
  print_dir_overview "${table_path}/data"

  echo
  echo "[recent files]"
  print_recent_files "$table_path" 10
}

echo "============================================"
echo "  HDFS Topic / Iceberg Inventory"
echo "============================================"
echo "namenode_container: ${NAMENODE_CONTAINER}"
echo

echo "=== HDFS root ==="
run_hdfs -ls / || true
echo

echo "=== HDFS main path sizes ==="
for path in /warehouse /warehouse/iceberg /checkpoints /logs /tmp; do
  print_du "$path"
done

echo
echo "=== Kafka topics (if Kafka container is running) ==="
if docker exec "$KAFKA_CONTAINER" kafka-topics --bootstrap-server kafka:9092 --list >/tmp/ais_kafka_topics.txt 2>/dev/null; then
  cat /tmp/ais_kafka_topics.txt | sed 's/^/  /'
else
  echo "  unable to read Kafka topics from container: ${KAFKA_CONTAINER}"
fi

print_table "weather_history" \
  "/warehouse/iceberg/weather/weather_history_bronze" \
  "/checkpoints/weather_history"

print_table "openaq-hourly" \
  "/warehouse/iceberg/air_quality/openaq_hourly_bronze" \
  "/checkpoints/openaq_hourly"

print_table "sentinel5p-summary" \
  "/warehouse/iceberg/satellite/sentinel5p_summary_bronze" \
  "/checkpoints/sentinel5p_summary"

print_table "maiac-summary" \
  "/warehouse/iceberg/satellite/maiac_summary_bronze" \
  "/checkpoints/maiac_summary"

print_table "era5-files" \
  "/warehouse/iceberg/weather/era5_files_bronze" \
  "/checkpoints/era5_files"

echo
echo "=== Other Iceberg directories ==="
if dir_exists /warehouse/iceberg; then
  run_hdfs -ls -R /warehouse/iceberg 2>/dev/null \
    | awk '$1 ~ /^d/ {print $8}' \
    | sed 's/^/  /' \
    || true
else
  echo "  /warehouse/iceberg missing"
fi

echo
echo "DONE"
