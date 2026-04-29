#!/usr/bin/env bash
set -euo pipefail

export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL="*"

NAMENODE_CONTAINER="${NAMENODE_CONTAINER:-namenode}"

run_hdfs() {
    docker exec "$NAMENODE_CONTAINER" hdfs dfs "$@"
}

echo "=== HDFS root ==="
run_hdfs -ls /

echo
echo "=== Iceberg warehouse ==="
run_hdfs -ls -R /warehouse/iceberg || true

echo
echo "=== Spark checkpoints ==="
run_hdfs -ls -R /checkpoints || true

echo
echo "=== AIS logs ==="
run_hdfs -ls -R /logs || true

echo
echo "=== Iceberg table health check ==="

TABLES=(
    "/warehouse/iceberg/weather/weather_history_bronze"
    "/warehouse/iceberg/air_quality/openaq_hourly_bronze"
    "/warehouse/iceberg/satellite/sentinel5p_summary_bronze"
    "/warehouse/iceberg/satellite/maiac_summary_bronze"
)

for table in "${TABLES[@]}"; do
    echo
    echo "[TABLE] $table"

    if run_hdfs -test -d "$table"; then
        echo "  exists: yes"

        if run_hdfs -test -d "$table/data"; then
            echo "  data dir: yes"
            run_hdfs -find "$table/data" -name "*.parquet" | head -5 || true
        else
            echo "  data dir: no"
        fi

        if run_hdfs -test -d "$table/metadata"; then
            echo "  metadata dir: yes"
            run_hdfs -find "$table/metadata" -name "*.metadata.json" | tail -3 || true
        else
            echo "  metadata dir: no"
        fi
    else
        echo "  exists: no"
    fi
done