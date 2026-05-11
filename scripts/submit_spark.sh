#!/bin/bash
# =============================================================================
# submit_spark.sh
# Submit AIS Spark jobs (streaming + batch load)
#
# KAFKA_STARTING_OFFSETS:
#   - "latest"  : Start from newest messages only (default)
#   - "earliest": Catch all historical Kafka messages (recommended for initial runs)
#
# Example:
#   KAFKA_STARTING_OFFSETS=earliest bash scripts/submit_spark.sh sentinel5p
#   bash scripts/submit_spark.sh era5-ingest   # download ERA5 + publish metadata to Kafka
#   bash scripts/submit_spark.sh era5-files    # Spark consumer: Kafka era5-files -> Iceberg
# =============================================================================

set -euo pipefail
export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL="*"

# Load .env file to get credentials and configuration
if [ -f ".env" ]; then
  set +u  # Temporarily disable strict mode for variable substitution
  set -a
  source .env
  set +a
  set -u  # Re-enable strict mode
fi

JOB_TYPE="${1:-weather}"
DETACH="${DETACH:-false}"
STOP_AFTER_BATCH="${STOP_AFTER_BATCH:-false}"
PROCESSING_TIME="${PROCESSING_TIME:-}"
KAFKA_STARTING_OFFSETS="${KAFKA_STARTING_OFFSETS:-latest}"
START_DATE="${START_DATE:-}"
END_DATE="${END_DATE:-}"
ERA5_START_DATE="${ERA5_START_DATE:-}"
ERA5_END_DATE="${ERA5_END_DATE:-}"
FULL_REFRESH="${FULL_REFRESH:-0}"
MAIAC_LOCAL_FALLBACK_PATH="${MAIAC_LOCAL_FALLBACK_PATH:-/opt/maiac_data}"
MAIAC_RELAXED_QA="${MAIAC_RELAXED_QA:-0}"
SPARK_JARS_IVY="${SPARK_JARS_IVY:-/root/.ivy2}"
COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-atmospheric_intelligence_sys---ais}"

KAFKA_HADOOP_PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.apache.hadoop:hadoop-client:3.3.4"
ICEBERG_PACKAGES="${KAFKA_HADOOP_PACKAGES},org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1"
CASSANDRA_PACKAGES="${ICEBERG_PACKAGES},com.datastax.spark:spark-cassandra-connector_2.12:3.5.1"

APP_NAME=""
JOB_FILE=""
JOB_ARGS=()
STREAM_ARGS=()
HDFS_DATA_DIR=""
HDFS_CHECKPOINT_DIR=""
KAFKA_TOPIC=""
ICEBERG_TABLE=""
CHECKPOINT_PATH=""
PACKAGES="${ICEBERG_PACKAGES}"
SPARK_CORES_MAX="${SPARK_CORES_MAX:-}"
SPARK_EXECUTOR_CORES="${SPARK_EXECUTOR_CORES:-}"

wait_for_hdfs_writable() {
  local timeout_sec="${1:-300}"
  local elapsed=0

  while true; do
    local safemode_output
    safemode_output="$(docker exec namenode hdfs dfsadmin -safemode get 2>/dev/null || true)"

    if echo "$safemode_output" | grep -q "Safe mode is OFF"; then
      if docker exec namenode hdfs dfs -ls / >/dev/null 2>&1; then
        echo "[OK] HDFS RPC reachable and safemode is OFF"
        return 0
      fi
    fi

    if [ "$elapsed" -ge "$timeout_sec" ]; then
      echo "[ERROR] HDFS is not writable after ${timeout_sec}s"
      docker exec namenode hdfs dfsadmin -safemode get || true
      return 1
    fi

    echo "[WAIT] HDFS not writable yet (${elapsed}s/${timeout_sec}s)"
    sleep 5
    elapsed=$((elapsed + 5))
  done
}

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

case "$JOB_TYPE" in
  weather-ingest)
    JOB_TYPE_KIND="ingest"
    APP_NAME="WeatherHistory_Ingest"
    INGEST_SERVICE="ingest"
    INGEST_SCRIPT="ingest_weather.py"
    KAFKA_TOPIC="weather_history"
    INGEST_LOOKBACK_DAYS="${WEATHER_BATCH_LOOKBACK_DAYS:-7}"
    KAFKA_TOPIC="weather_history"
    ;;
  openaq-ingest)
    JOB_TYPE_KIND="ingest"
    APP_NAME="OpenAQHourly_Ingest"
    INGEST_SERVICE="openaq-ingest"
    INGEST_SCRIPT="openaq_ingest.py"
    KAFKA_TOPIC="openaq-hourly"
    INGEST_LOOKBACK_DAYS="${OPENAQ_BATCH_LOOKBACK_DAYS:-7}"
    KAFKA_TOPIC="openaq-hourly"
    ;;
  sentinel5p-ingest)
    JOB_TYPE_KIND="ingest"
    APP_NAME="Sentinel5PSummary_Ingest"
    INGEST_SERVICE="sentinel5p-ingest"
    INGEST_SCRIPT="sentinel5p_ingest.py"
    KAFKA_TOPIC="sentinel5p-summary"
    INGEST_LOOKBACK_DAYS="${LOOKBACK_DAYS:-7}"
    KAFKA_TOPIC="sentinel5p-summary"
    SENTINEL5P_LOCAL_METADATA_PATH="/app/data/crawling/outputs/sentinel5p_vietnam_last_3d.json"
    ;;
  maiac-ingest)
    JOB_TYPE_KIND="ingest"
    APP_NAME="MAIACSummary_Ingest"
    INGEST_SERVICE="maiac-ingest"
    INGEST_SCRIPT="maiac_ingest.py"
    KAFKA_TOPIC="maiac-summary"
    INGEST_LOOKBACK_DAYS="${MAIAC_BATCH_LOOKBACK_DAYS:-${LOOKBACK_DAYS:-30}}"
    KAFKA_TOPIC="maiac-summary"
    ;;
  era5-ingest)
    JOB_TYPE_KIND="ingest"
    APP_NAME="ERA5Files_Ingest"
    INGEST_SERVICE="ingest"
    INGEST_SCRIPT="era5_ingest.py"
    KAFKA_TOPIC="era5-files"
    INGEST_LOOKBACK_DAYS="${ERA5_BATCH_LOOKBACK_DAYS:-${LOOKBACK_DAYS:-7}}"
    KAFKA_TOPIC="era5-files"
    ;;
  weather)
    JOB_TYPE_KIND="spark"
    APP_NAME="WeatherHistory_Streaming"
    JOB_FILE="/opt/spark-jobs/weather_streaming.py"
    HDFS_DATA_DIR="/warehouse/iceberg/weather/weather_history_bronze"
    HDFS_CHECKPOINT_DIR="/checkpoints/weather_history"
    KAFKA_TOPIC="weather_history"
    ICEBERG_TABLE="ais.weather.weather_history_bronze"
    CHECKPOINT_PATH="hdfs://namenode:9000/checkpoints/weather_history/"
    PACKAGES="${ICEBERG_PACKAGES}"
    ;;
  era5-files)
    JOB_TYPE_KIND="spark"
    APP_NAME="ERA5Files_Streaming"
    JOB_FILE="/opt/spark-jobs/era5_files_streaming.py"
    HDFS_DATA_DIR="/warehouse/iceberg/weather/era5_files_bronze"
    HDFS_CHECKPOINT_DIR="/checkpoints/era5_files"
    KAFKA_TOPIC="era5-files"
    ICEBERG_TABLE="ais.weather.era5_files_bronze"
    CHECKPOINT_PATH="hdfs://namenode:9000/checkpoints/era5_files/"
    PACKAGES="${ICEBERG_PACKAGES}"
    ;;
  openaq)
    JOB_TYPE_KIND="spark"
    APP_NAME="OpenAQHourly_Streaming"
    JOB_FILE="/opt/spark-jobs/openaq_hourly_streaming.py"
    HDFS_DATA_DIR="/warehouse/iceberg/air_quality/openaq_hourly_bronze"
    HDFS_CHECKPOINT_DIR="/checkpoints/openaq_hourly"
    KAFKA_TOPIC="openaq-hourly"
    ICEBERG_TABLE="ais.air_quality.openaq_hourly_bronze"
    CHECKPOINT_PATH="hdfs://namenode:9000/checkpoints/openaq_hourly/"
    PACKAGES="${ICEBERG_PACKAGES}"
    ;;
  sentinel5p)
    JOB_TYPE_KIND="spark"
    APP_NAME="Sentinel5PSummary_Streaming"
    JOB_FILE="/opt/spark-jobs/sentinel5p_summary_streaming.py"
    HDFS_DATA_DIR="/warehouse/iceberg/satellite/sentinel5p_summary_bronze"
    HDFS_CHECKPOINT_DIR="/checkpoints/sentinel5p_summary"
    KAFKA_TOPIC="sentinel5p-summary"
    ICEBERG_TABLE="ais.satellite.sentinel5p_summary_bronze"
    CHECKPOINT_PATH="hdfs://namenode:9000/checkpoints/sentinel5p_summary/"
    PACKAGES="${ICEBERG_PACKAGES}"
    ;;
  maiac)
    JOB_TYPE_KIND="spark"
    APP_NAME="MAIACSummary_Streaming"
    JOB_FILE="/opt/spark-jobs/maiac_summary_streaming.py"
    HDFS_DATA_DIR="/warehouse/iceberg/satellite/maiac_summary_bronze"
    HDFS_CHECKPOINT_DIR="/checkpoints/maiac_summary"
    KAFKA_TOPIC="maiac-summary"
    ICEBERG_TABLE="ais.satellite.maiac_summary_bronze"
    CHECKPOINT_PATH="hdfs://namenode:9000/checkpoints/maiac_summary/"
    PACKAGES="${ICEBERG_PACKAGES}"
    ;;
  hanoi-openaq-silver)
    JOB_TYPE_KIND="spark"
    APP_NAME="HanoiOpenAQSilver"
    JOB_FILE="/opt/spark-jobs/hanoi_openaq_silver.py"
    JOB_ARGS=("--full-refresh" "$FULL_REFRESH")
    HDFS_DATA_DIR="/warehouse/iceberg/air_quality/openaq_hanoi_hourly_silver"
    HDFS_CHECKPOINT_DIR="/checkpoints/hanoi_openaq_silver"
    ICEBERG_TABLE="ais.air_quality.openaq_hanoi_hourly_silver"
    CHECKPOINT_PATH="hdfs://namenode:9000/checkpoints/hanoi_openaq_silver/"
    PACKAGES="${ICEBERG_PACKAGES}"
    ;;
  hanoi-weather-silver)
    JOB_TYPE_KIND="spark"
    APP_NAME="HanoiWeatherSurfaceProxySilver"
    JOB_FILE="/opt/spark-jobs/hanoi_weather_surface_proxy_silver.py"
    JOB_ARGS=("--full-refresh" "$FULL_REFRESH")
    HDFS_DATA_DIR="/warehouse/iceberg/weather/weather_hanoi_surface_proxy_silver"
    HDFS_CHECKPOINT_DIR="/checkpoints/hanoi_weather_surface_proxy_silver"
    ICEBERG_TABLE="ais.weather.weather_hanoi_surface_proxy_silver"
    CHECKPOINT_PATH="hdfs://namenode:9000/checkpoints/hanoi_weather_surface_proxy_silver/"
    PACKAGES="${ICEBERG_PACKAGES}"
    ;;
  era5-surface-hanoi-silver)
    JOB_TYPE_KIND="spark"
    APP_NAME="ERA5SurfaceHanoiSilver"
    JOB_FILE="/opt/spark-jobs/era5_surface_hanoi_silver.py"
    JOB_ARGS=("--full-refresh" "$FULL_REFRESH")
    HDFS_DATA_DIR="/warehouse/iceberg/weather/era5_surface_hanoi_hourly_silver"
    HDFS_CHECKPOINT_DIR="/checkpoints/era5_surface_hanoi_silver"
    ICEBERG_TABLE="ais.weather.era5_surface_hanoi_hourly_silver"
    CHECKPOINT_PATH="hdfs://namenode:9000/checkpoints/era5_surface_hanoi_silver/"
    PACKAGES="${ICEBERG_PACKAGES}"
    ;;
  sentinel5p-hanoi-silver)
    JOB_TYPE_KIND="spark"
    APP_NAME="Sentinel5PHanoiSilver"
    JOB_FILE="/opt/spark-jobs/sentinel5p_hanoi_silver.py"
    JOB_ARGS=("--full-refresh" "$FULL_REFRESH")
    HDFS_DATA_DIR="/warehouse/iceberg/satellite/sentinel5p_hanoi_daily_silver"
    HDFS_CHECKPOINT_DIR="/checkpoints/sentinel5p_hanoi_silver"
    ICEBERG_TABLE="ais.satellite.sentinel5p_hanoi_daily_silver"
    CHECKPOINT_PATH="hdfs://namenode:9000/checkpoints/sentinel5p_hanoi_silver/"
    PACKAGES="${ICEBERG_PACKAGES}"
    ;;
  maiac-hanoi-silver)
    JOB_TYPE_KIND="spark"
    APP_NAME="MAIACHanoiSilver"
    JOB_FILE="/opt/spark-jobs/maiac_hanoi_silver.py"
    JOB_ARGS=("--full-refresh" "$FULL_REFRESH" "--local-fallback-path" "$MAIAC_LOCAL_FALLBACK_PATH" "--relaxed-qa" "$MAIAC_RELAXED_QA")
    HDFS_DATA_DIR="/warehouse/iceberg/satellite/maiac_hanoi_daily_silver"
    HDFS_CHECKPOINT_DIR="/checkpoints/maiac_hanoi_daily_silver"
    ICEBERG_TABLE="ais.satellite.maiac_hanoi_daily_silver"
    CHECKPOINT_PATH="hdfs://namenode:9000/checkpoints/maiac_hanoi_daily_silver/"
    PACKAGES="${ICEBERG_PACKAGES}"
    ;;
  hanoi-master-features-gold)
    JOB_TYPE_KIND="spark"
    APP_NAME="HanoiPM25MasterFeaturesGold"
    JOB_FILE="/opt/spark-jobs/hanoi_pm25_master_features_gold.py"
    JOB_ARGS=("--full-refresh" "$FULL_REFRESH")
    HDFS_DATA_DIR="/warehouse/iceberg/features/hanoi_pm25_master_hourly_gold"
    HDFS_CHECKPOINT_DIR="/checkpoints/hanoi_pm25_master_features_gold"
    ICEBERG_TABLE="ais.features.hanoi_pm25_master_hourly_gold"
    CHECKPOINT_PATH="hdfs://namenode:9000/checkpoints/hanoi_pm25_master_features_gold/"
    PACKAGES="${ICEBERG_PACKAGES}"
    ;;
  hanoi-training-dataset-gold)
    JOB_TYPE_KIND="spark"
    APP_NAME="HanoiPM25TrainingDatasetGold"
    JOB_FILE="/opt/spark-jobs/hanoi_pm25_training_dataset_gold.py"
    JOB_ARGS=("--full-refresh" "$FULL_REFRESH")
    HDFS_DATA_DIR="/warehouse/iceberg/features/hanoi_pm25_training_dataset_gold"
    HDFS_CHECKPOINT_DIR="/checkpoints/hanoi_pm25_training_dataset_gold"
    ICEBERG_TABLE="ais.features.hanoi_pm25_training_dataset_gold"
    CHECKPOINT_PATH="hdfs://namenode:9000/checkpoints/hanoi_pm25_training_dataset_gold/"
    PACKAGES="${ICEBERG_PACKAGES}"
    ;;
  hanoi-train-baseline)
    JOB_TYPE_KIND="spark"
    APP_NAME="TrainHanoiPM25Baseline"
    JOB_FILE="/opt/ml/train_hanoi_pm25.py"
    JOB_ARGS=("--dataset-version" "${DATASET_VERSION:-hanoi_pm25_v1}" "--feature-set-name" "${FEATURE_SET_NAME:-hanoi_pm25_core_v1}" "--model-type" "${MODEL_TYPE:-lightgbm}" "--output-dir" "${MODEL_OUTPUT_DIR:-/opt/models/hanoi_pm25}")
    HDFS_DATA_DIR="/warehouse/iceberg/models/hanoi_pm25_model_runs_gold"
    HDFS_CHECKPOINT_DIR="/checkpoints/hanoi_train_baseline"
    ICEBERG_TABLE="ais.models.hanoi_pm25_model_runs_gold"
    CHECKPOINT_PATH="hdfs://namenode:9000/checkpoints/hanoi_train_baseline/"
    PACKAGES="${ICEBERG_PACKAGES}"
    ;;
  cassandra-weather)
    JOB_TYPE_KIND="spark"
    APP_NAME="IcebergToCassandra_Weather"
    JOB_FILE="/opt/spark-jobs/iceberg_to_cassandra.py"
    JOB_ARGS=("weather")
    HDFS_DATA_DIR="/data/iceberg_to_cassandra"
    HDFS_CHECKPOINT_DIR="/checkpoints/iceberg_to_cassandra"
    PACKAGES="${CASSANDRA_PACKAGES}"
    ;;
  cassandra-openaq)
    JOB_TYPE_KIND="spark"
    APP_NAME="IcebergToCassandra_OpenAQ"
    JOB_FILE="/opt/spark-jobs/iceberg_to_cassandra.py"
    JOB_ARGS=("openaq")
    HDFS_DATA_DIR="/data/iceberg_to_cassandra"
    HDFS_CHECKPOINT_DIR="/checkpoints/iceberg_to_cassandra"
    PACKAGES="${CASSANDRA_PACKAGES}"
    ;;
  ensure-iceberg)
    JOB_TYPE_KIND="spark"
    APP_NAME="AIS_EnsureIcebergTables"
    JOB_FILE="/opt/spark-jobs/ensure_iceberg_tables.py"
    HDFS_DATA_DIR="/warehouse/iceberg"
    HDFS_CHECKPOINT_DIR="/checkpoints"
    CHECKPOINT_PATH="hdfs://namenode:9000/checkpoints/ensure_iceberg/"
    PACKAGES="${ICEBERG_PACKAGES}"
    ;;
  maintenance-iceberg)
    JOB_TYPE_KIND="spark"
    APP_NAME="AIS_IcebergMaintenance"
    JOB_FILE="/opt/spark-jobs/iceberg_maintenance.py"
    JOB_ARGS=("--retention-hours" "${RETENTION_HOURS:-168}")
    HDFS_DATA_DIR="/warehouse/iceberg"
    HDFS_CHECKPOINT_DIR="/checkpoints"
    CHECKPOINT_PATH="hdfs://namenode:9000/checkpoints/iceberg_maintenance/"
    PACKAGES="${ICEBERG_PACKAGES}"
    ;;
  reconcile-serving)
    JOB_TYPE_KIND="spark"
    APP_NAME="AIS_ReconcileServing"
    JOB_FILE="/opt/spark-jobs/reconcile_iceberg_cassandra.py"
    JOB_ARGS=("--lookback-hours" "${RECONCILE_LOOKBACK_HOURS:-24}" "--tolerance" "${RECONCILE_TOLERANCE:-0.95}")
    HDFS_DATA_DIR="/warehouse/iceberg"
    HDFS_CHECKPOINT_DIR="/checkpoints"
    CHECKPOINT_PATH="hdfs://namenode:9000/checkpoints/reconcile_serving/"
    PACKAGES="${CASSANDRA_PACKAGES}"
    ;;
  *)
    echo "Usage: $0 [weather|openaq|sentinel5p|maiac|era5-files|weather-ingest|openaq-ingest|sentinel5p-ingest|maiac-ingest|era5-ingest|hanoi-openaq-silver|hanoi-weather-silver|era5-surface-hanoi-silver|sentinel5p-hanoi-silver|maiac-hanoi-silver|hanoi-master-features-gold|hanoi-training-dataset-gold|hanoi-train-baseline|cassandra-weather|cassandra-openaq|ensure-iceberg|maintenance-iceberg|reconcile-serving]"
    exit 1
    ;;
esac

case "$JOB_TYPE" in
  hanoi-openaq-silver|hanoi-weather-silver|era5-surface-hanoi-silver|sentinel5p-hanoi-silver|maiac-hanoi-silver|hanoi-master-features-gold|hanoi-training-dataset-gold)
    if [ -n "$START_DATE" ]; then
      JOB_ARGS+=("--start-date" "$START_DATE")
    fi
    if [ -n "$END_DATE" ]; then
      JOB_ARGS+=("--end-date" "$END_DATE")
    fi
    ;;
esac

if [ "${JOB_TYPE_KIND:-spark}" = "ingest" ]; then
  echo "=== Submit Ingest Job: $APP_NAME ==="
  docker compose -p "$COMPOSE_PROJECT_NAME" run --rm --no-deps \
    -e WINDOW_MODE=batch \
    -e BATCH_LOOKBACK_DAYS="$INGEST_LOOKBACK_DAYS" \
    -e LOOKBACK_DAYS="$INGEST_LOOKBACK_DAYS" \
    -e WINDOW_START_UTC="${WINDOW_START_UTC:-}" \
    -e WINDOW_END_UTC="${WINDOW_END_UTC:-}" \
    -e KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}" \
    -e KAFKA_TOPIC="$KAFKA_TOPIC" \
    -e KAFKA_CONNECT_MAX_RETRIES=36 \
    -e KAFKA_CONNECT_RETRY_DELAY=5 \
    -e CDS_URL="${CDS_URL:-}" \
    -e CDS_KEY="${CDS_KEY:-}" \
    -e ERA5_START_DATE="${ERA5_START_DATE:-}" \
    -e ERA5_END_DATE="${ERA5_END_DATE:-}" \
    -e ERA5_DATASET_TYPE="${ERA5_DATASET_TYPE:-surface}" \
    -e ERA5_OUTPUT_BASE_PATH="${ERA5_OUTPUT_BASE_PATH:-}" \
    -e ERA5_SKIP_EXISTING="${ERA5_SKIP_EXISTING:-true}" \
    -e SENTINEL5P_LOCAL_METADATA_PATH="${SENTINEL5P_LOCAL_METADATA_PATH:-}" \
    "$INGEST_SERVICE" \
    python3 "$INGEST_SCRIPT"
  exit 0
fi

case "$JOB_TYPE" in
  weather|openaq|sentinel5p|maiac|era5-files)
    # Keep each streaming app lightweight so multiple consumers can run on a small local cluster.
    SPARK_CORES_MAX="${SPARK_CORES_MAX:-1}"
    SPARK_EXECUTOR_CORES="${SPARK_EXECUTOR_CORES:-1}"
    if [ "$STOP_AFTER_BATCH" = "true" ]; then
      STREAM_ARGS+=("--stop-after-batch" "1")
    fi
    if [ -n "$PROCESSING_TIME" ]; then
      STREAM_ARGS+=("--processing-time" "$PROCESSING_TIME")
    fi
    ;;
esac

echo "=== Create HDFS output paths ==="
wait_for_hdfs_writable 300
docker exec namenode hdfs dfs -mkdir -p "$HDFS_DATA_DIR"
docker exec namenode hdfs dfs -mkdir -p "$HDFS_CHECKPOINT_DIR"
docker exec namenode hdfs dfs -mkdir -p /warehouse/iceberg
docker exec namenode hdfs dfs -chmod 777 "$HDFS_DATA_DIR"
docker exec namenode hdfs dfs -chmod 777 "$HDFS_CHECKPOINT_DIR"
docker exec namenode hdfs dfs -chmod 777 /warehouse/iceberg

echo
echo "=== Submit Spark Job: $APP_NAME ==="

if [ "$DETACH" = "true" ]; then
  if spark_app_registered "$APP_NAME"; then
    echo "[WARN] Spark app already active: ${APP_NAME}; skip duplicate submit"
    exit 0
  fi
fi

docker exec spark-master sh -lc "mkdir -p '$SPARK_JARS_IVY' && find '$SPARK_JARS_IVY' -type f -name '*.part' -delete" >/dev/null 2>&1 || true

DOCKER_EXEC_ARGS=()
if [ "$DETACH" = "true" ]; then
  DOCKER_EXEC_ARGS+=("-d")
fi
DOCKER_EXEC_ARGS+=("-e" "KAFKA_STARTING_OFFSETS=${KAFKA_STARTING_OFFSETS}")
DOCKER_EXEC_ARGS+=("-e" "KAFKA_TOPIC=${KAFKA_TOPIC:-}")
DOCKER_EXEC_ARGS+=("-e" "ICEBERG_TABLE=${ICEBERG_TABLE:-}")
DOCKER_EXEC_ARGS+=("-e" "CHECKPOINT_PATH=${CHECKPOINT_PATH:-}")
DOCKER_EXEC_ARGS+=("-e" "S5P_QA_THRESHOLD=${S5P_QA_THRESHOLD:-}")
DOCKER_EXEC_ARGS+=("-e" "S5P_NO2_QA_THRESHOLD=${S5P_NO2_QA_THRESHOLD:-}")
DOCKER_EXEC_ARGS+=("-e" "S5P_CO_QA_THRESHOLD=${S5P_CO_QA_THRESHOLD:-}")
DOCKER_EXEC_ARGS+=("-e" "S5P_SO2_QA_THRESHOLD=${S5P_SO2_QA_THRESHOLD:-}")
DOCKER_EXEC_ARGS+=("-e" "S5P_O3_QA_THRESHOLD=${S5P_O3_QA_THRESHOLD:-}")
DOCKER_EXEC_ARGS+=("-e" "S5P_AER_AI_QA_THRESHOLD=${S5P_AER_AI_QA_THRESHOLD:-}")

SPARK_EXTRA_CONF=()
if [ -n "$SPARK_CORES_MAX" ]; then
  SPARK_EXTRA_CONF+=(--conf "spark.cores.max=${SPARK_CORES_MAX}")
fi
if [ -n "$SPARK_EXECUTOR_CORES" ]; then
  SPARK_EXTRA_CONF+=(--conf "spark.executor.cores=${SPARK_EXECUTOR_CORES}")
fi

docker exec "${DOCKER_EXEC_ARGS[@]}" spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --name "$APP_NAME" \
  --conf "spark.jars.ivy=${SPARK_JARS_IVY}" \
  --repositories "https://repo.maven.apache.org/maven2,https://repo1.maven.org/maven2,https://repos.spark-packages.org" \
  --packages "$PACKAGES" \
  --conf "spark.sql.streaming.checkpointLocation=${CHECKPOINT_PATH}" \
  --conf "spark.hadoop.fs.defaultFS=hdfs://namenode:9000" \
  --conf "spark.yarn.maxAppAttempts=1" \
  "$JOB_FILE" \
  "${JOB_ARGS[@]}" \
  "${STREAM_ARGS[@]}"

if [ "$DETACH" = "true" ]; then
  echo "Submitted in detached mode: $APP_NAME"
fi
