from __future__ import annotations

SPARK_COMMON_CONF = " \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-client:3.2.1,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2 \
  --conf \"spark.hadoop.fs.defaultFS=hdfs://namenode:9000\" \
  --conf \"spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions\" \
  --conf \"spark.sql.catalog.ais=org.apache.iceberg.spark.SparkCatalog\" \
  --conf \"spark.sql.catalog.ais.type=hadoop\" \
  --conf \"spark.sql.catalog.ais.warehouse=hdfs://namenode:9000/warehouse/iceberg\" \
  --conf \"spark.sql.adaptive.enabled=true\" \
  --conf \"spark.driver.memory=1g\" \
  --conf \"spark.executor.memory=1g\""

CASSANDRA_CONF = " \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-client:3.2.1,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 \
    --conf \"spark.hadoop.fs.defaultFS=hdfs://namenode:9000\" \
    --conf \"spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions\" \
    --conf \"spark.sql.catalog.ais=org.apache.iceberg.spark.SparkCatalog\" \
    --conf \"spark.sql.catalog.ais.type=hadoop\" \
    --conf \"spark.sql.catalog.ais.warehouse=hdfs://namenode:9000/warehouse/iceberg\" \
    --conf \"spark.cassandra.connection.host=cassandra\" \
    --conf \"spark.cassandra.connection.port=9042\" \
    --conf \"spark.sql.adaptive.enabled=true\" \
    --conf \"spark.driver.memory=1g\" \
    --conf \"spark.executor.memory=1g\""

LOOKBACK_DAYS_TEMPLATE = "{{ dag_run.conf.get('lookback_days', 7) if dag_run and dag_run.conf else 7 }}"
MAIAC_LOOKBACK_DAYS_TEMPLATE = "{{ dag_run.conf.get('maiac_lookback_days', dag_run.conf.get('lookback_days', 30)) if dag_run and dag_run.conf else 30 }}"
COMPOSE_PROJECT_NAME_TEMPLATE = "${COMPOSE_PROJECT_NAME:-atmospheric_intelligence_sys---ais}"


def spark_submit_command(
    app_name: str,
    job_file: str,
    *,
    extra_args: str = "",
    starting_offsets: str | None = None,
    with_cassandra: bool = False,
    detached: bool = False,
) -> str:
    conf = CASSANDRA_CONF if with_cassandra else SPARK_COMMON_CONF
    detach_flag = "-d " if detached else ""
    cleaned_args = extra_args.strip()
    suffix = f" {cleaned_args}" if cleaned_args else ""

    env_prefix = f"KAFKA_STARTING_OFFSETS={starting_offsets} " if starting_offsets else ""

    return (
        "set -euo pipefail\n"
        "cd /opt/ais\n"
        f"{env_prefix}docker exec {detach_flag}spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --name \"{app_name}\"{conf} {job_file}{suffix}"
    )


def spark_cassandra_command(dataset: str) -> str:
    return spark_submit_command(
        app_name=f"IcebergToCassandra_{dataset.capitalize()}",
        job_file="/opt/spark-jobs/iceberg_to_cassandra.py",
        extra_args=dataset,
        with_cassandra=True,
    )


def ensure_topics_command() -> str:
    return (
        "set -euo pipefail\n"
        "cd /opt/ais\n"
        "bash ./scripts/create_topics.sh "
    )


def ensure_iceberg_tables_command() -> str:
    return spark_submit_command(
        app_name="AIS_EnsureIcebergTables",
        job_file="/opt/spark-jobs/ensure_iceberg_tables.py",
    )


def ensure_cassandra_schema_command() -> str:
    return (
        "set -euo pipefail\n"
        "docker exec cassandra cqlsh -e \"CREATE KEYSPACE IF NOT EXISTS ais_serving WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};\"\n"
        "docker exec cassandra cqlsh -e \"CREATE TABLE IF NOT EXISTS ais_serving.weather_hourly_by_province_day (province text, day text, event_time timestamp, event_id text, query_date text, location_name text, lat double, lon double, temp_c double, temp_f double, humidity int, wind_kph double, wind_degree int, wind_dir text, precip_mm double, condition_text text, source text, ingest_time text, PRIMARY KEY ((province, day), event_time)) WITH CLUSTERING ORDER BY (event_time DESC);\"\n"
        "docker exec cassandra cqlsh -e \"CREATE TABLE IF NOT EXISTS ais_serving.openaq_hourly_by_city_parameter_day (city text, parameter text, day text, event_time timestamp, event_id text, location_id bigint, location_name text, provider text, sensor_id bigint, unit text, value double, min double, max double, sd double, coverage_pct double, source text, ingest_time text, PRIMARY KEY ((city, parameter, day), event_time)) WITH CLUSTERING ORDER BY (event_time DESC);\""
    )


def compose_ingest_command(
    service: str,
    script_name: str,
    *,
    lookback_days_template: str = LOOKBACK_DAYS_TEMPLATE,
) -> str:
    return (
        "set -euo pipefail\n"
        "cd /opt/ais\n"
        "for i in $(seq 1 36); do\n"
        "  if docker exec kafka kafka-topics --bootstrap-server kafka:9092 --list >/dev/null 2>&1; then\n"
        "    echo 'Kafka is ready'\n"
        "    break\n"
        "  fi\n"
        "  if [ \"$i\" -eq 36 ]; then\n"
        "    echo 'Kafka is not ready after 180 seconds' >&2\n"
        "    exit 1\n"
        "  fi\n"
        "  echo \"Waiting for Kafka... attempt $i/36\"\n"
        "  sleep 5\n"
        "done\n"
        f"docker compose -p {COMPOSE_PROJECT_NAME_TEMPLATE} run --rm --no-deps -e WINDOW_MODE=batch -e BATCH_LOOKBACK_DAYS={lookback_days_template} -e KAFKA_CONNECT_MAX_RETRIES=36 -e KAFKA_CONNECT_RETRY_DELAY=5 {service} python -u {script_name}"
    )


def ensure_streaming_job_command(job_type: str) -> str:
    return (
        "set -euo pipefail\n"
        "cd /opt/ais\n"
        f"bash scripts/airflow/ensure_stream_job.sh {job_type}"
    )


def kafka_lag_check_command(group_id: str, topic: str, max_lag: int = 50000) -> str:
    return (
        "set -euo pipefail\n"
        "cd /opt/ais\n"
        f"bash scripts/airflow/check_kafka_lag.sh {group_id} {topic} {max_lag}"
    )


def reconcile_serving_command(lookback_hours: int = 24, tolerance: float = 0.95) -> str:
    return spark_submit_command(
        app_name="AIS_ReconcileServing",
        job_file="/opt/spark-jobs/reconcile_iceberg_cassandra.py",
        extra_args=f"--lookback-hours {lookback_hours} --tolerance {tolerance}",
        with_cassandra=True,
    )


def iceberg_maintenance_command(retention_hours: int = 168) -> str:
    return spark_submit_command(
        app_name="AIS_IcebergMaintenance",
        job_file="/opt/spark-jobs/iceberg_maintenance.py",
        extra_args=f"--retention-hours {retention_hours}",
    )
