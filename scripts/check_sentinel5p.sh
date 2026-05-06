#!/bin/bash
set -euo pipefail

SPARK_SQL="/opt/spark/bin/spark-sql --master spark://spark-master:7077 --packages 'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1' --conf spark.sql.catalog.ais=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.ais.type=hadoop --conf spark.sql.catalog.ais.warehouse=hdfs://namenode:9000/warehouse/iceberg --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"

echo "=== SHOW TABLES ==="
eval "$SPARK_SQL -e \"SHOW TABLES IN ais.satellite\"" || true

echo "=== SNAPSHOTS ==="
eval "$SPARK_SQL -e \"CALL ais.system.snapshots('ais.satellite.sentinel5p_hanoi_daily_silver')\"" || true

echo "=== BRONZE COUNT ==="
eval "$SPARK_SQL -e \"SELECT COUNT(*) FROM ais.satellite.sentinel5p_summary_bronze\"" || true
