#!/usr/bin/env bash
set -euo pipefail

export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL="*"

NAMENODE_CONTAINER="${NAMENODE_CONTAINER:-namenode}"

run_hdfs() {
    docker exec "$NAMENODE_CONTAINER" hdfs dfs "$@"
}

run_hdfs_admin() {
    docker exec "$NAMENODE_CONTAINER" hdfs dfsadmin "$@"
}

echo "=== Check HDFS safemode ==="
SAFE_MODE_OUTPUT="$(run_hdfs_admin -safemode get 2>/dev/null || true)"
echo "$SAFE_MODE_OUTPUT"

if echo "$SAFE_MODE_OUTPUT" | grep -qi "ON"; then
    echo "=== Leave HDFS safemode ==="
    run_hdfs_admin -safemode leave || true
fi

echo "=== Create AIS HDFS layout ==="

DIRS=(
    "/warehouse"
    "/warehouse/iceberg"
    "/warehouse/iceberg/weather"
    "/warehouse/iceberg/air_quality"
    "/warehouse/iceberg/satellite"

    "/checkpoints"
    "/checkpoints/weather_history"
    "/checkpoints/openaq_hourly"
    "/checkpoints/sentinel5p_summary"
    "/checkpoints/maiac_summary"

    "/tmp"
    "/tmp/spark"

    "/logs"
    "/logs/spark"
    "/logs/ingest"
)

for dir in "${DIRS[@]}"; do
    echo "[MKDIR] $dir"
    run_hdfs -mkdir -p "$dir"
done

echo "=== Set HDFS permissions for local development ==="

# Local/demo mode: để rộng quyền cho Spark/Airflow/Ingest dễ ghi.
run_hdfs -chmod -R 777 /warehouse
run_hdfs -chmod -R 777 /checkpoints
run_hdfs -chmod -R 777 /tmp/spark
run_hdfs -chmod -R 777 /logs

echo "=== Current AIS HDFS layout ==="
run_hdfs -ls -R /warehouse || true
run_hdfs -ls -R /checkpoints || true
run_hdfs -ls -R /logs || true

echo "=== HDFS layout initialized successfully ==="