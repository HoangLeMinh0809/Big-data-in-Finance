#!/bin/bash
set -euo pipefail

echo "=== Waiting for HDFS parquet /warehouse/iceberg/satellite/sentinel5p_summary_bronze ==="
elapsed=0
timeout=300

while ! docker exec namenode hdfs dfs -ls -R /warehouse/iceberg/satellite/sentinel5p_summary_bronze 2>/dev/null | grep -E '\.parquet$' >/dev/null 2>&1; do
  if [ "$elapsed" -ge "$timeout" ]; then
    echo "[ERROR] No parquet found under /warehouse/iceberg/satellite/sentinel5p_summary_bronze after ${timeout}s"
    docker exec namenode hdfs dfs -ls -R /warehouse/iceberg/satellite/sentinel5p_summary_bronze || true
    exit 1
  fi
  echo "[WAIT] No parquet yet (${elapsed}s/${timeout}s)"
  sleep 10
  elapsed=$((elapsed + 10))
done

echo "[OK] Found parquet under /warehouse/iceberg/satellite/sentinel5p_summary_bronze"

echo "=== Killing Spark app Sentinel5PSummary_Streaming ==="

docker exec -i -e APP_NAME=Sentinel5PSummary_Streaming spark-master python3 - <<'PY'
import json, urllib.request, os, sys
app_name=os.environ.get('APP_NAME','')
try:
  raw=urllib.request.urlopen('http://localhost:8080/json', timeout=10).read().decode('utf-8')
  payload=json.loads(raw)
except Exception as exc:
  print('[WARN] Unable to query Spark master:', exc)
  sys.exit(0)
for app in payload.get('activeapps',[]):
  if app.get('name')==app_name:
    app_id=app.get('id')
    if app_id:
      print(f'Killing Spark app {app_id}')
      os.system(f'/opt/spark/bin/spark-class org.apache.spark.deploy.Client kill spark://spark-master:7077 {app_id}')
PY

echo "=== Step 7 complete ==="
