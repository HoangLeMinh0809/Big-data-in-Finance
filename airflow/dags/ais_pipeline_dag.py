from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

DAG_ID = "ais_streaming_pipeline"

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

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


def spark_submit_command(app_name: str, job_file: str) -> str:
    return (
        "set -euo pipefail\n"
        f"docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --name \"{app_name}\"{SPARK_COMMON_CONF} {job_file}"
    )


def spark_cassandra_command(dataset: str) -> str:
    app_name = f"IcebergToCassandra_{dataset.capitalize()}"
    return (
        "set -euo pipefail\n"
        f"docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --name \"{app_name}\"{CASSANDRA_CONF} /opt/spark-jobs/iceberg_to_cassandra.py {dataset}"
    )


with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 4, 13),
    schedule=None,
    catchup=False,
    tags=["ais", "iceberg", "streaming"],
    description="Orchestrate weather and openaq ingest -> Spark Iceberg sink",
) as dag:
    ensure_weather_topic = BashOperator(
        task_id="ensure_weather_topic",
        bash_command=(
            "set -euo pipefail\n"
            "docker exec kafka kafka-topics --create --bootstrap-server kafka:9092 --replication-factor 1 --partitions 3 --topic weather-history --if-not-exists"
        ),
    )

    ensure_openaq_topic = BashOperator(
        task_id="ensure_openaq_topic",
        bash_command=(
            "set -euo pipefail\n"
            "docker exec kafka kafka-topics --create --bootstrap-server kafka:9092 --replication-factor 1 --partitions 3 --topic openaq-hourly --if-not-exists"
        ),
    )

    run_weather_ingest = BashOperator(
        task_id="run_weather_ingest",
        bash_command="set -euo pipefail\ndocker exec ingest python -u ingest_weather.py",
    )

    run_openaq_ingest = BashOperator(
        task_id="run_openaq_ingest",
        bash_command="set -euo pipefail\ndocker exec ingest python -u openaq_ingest.py",
    )

    run_weather_spark = BashOperator(
        task_id="run_weather_spark",
        bash_command=spark_submit_command("WeatherHistory_Streaming", "/opt/spark-jobs/weather_streaming.py"),
    )

    run_openaq_spark = BashOperator(
        task_id="run_openaq_spark",
        bash_command=spark_submit_command("OpenAQHourly_Streaming", "/opt/spark-jobs/openaq_hourly_streaming.py"),
    )

    ensure_cassandra_schema = BashOperator(
        task_id="ensure_cassandra_schema",
        bash_command=(
            "set -euo pipefail\n"
            "docker exec cassandra cqlsh -e \"CREATE KEYSPACE IF NOT EXISTS ais_serving WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};\"\n"
            "docker exec cassandra cqlsh -e \"CREATE TABLE IF NOT EXISTS ais_serving.weather_hourly_by_province_day (province text, day text, event_time timestamp, event_id text, query_date text, location_name text, lat double, lon double, temp_c double, temp_f double, humidity int, wind_kph double, wind_degree int, wind_dir text, precip_mm double, condition_text text, source text, ingest_time text, PRIMARY KEY ((province, day), event_time)) WITH CLUSTERING ORDER BY (event_time DESC);\"\n"
            "docker exec cassandra cqlsh -e \"CREATE TABLE IF NOT EXISTS ais_serving.openaq_hourly_by_city_parameter_day (city text, parameter text, day text, event_time timestamp, event_id text, location_id bigint, location_name text, provider text, sensor_id bigint, unit text, value double, min double, max double, sd double, coverage_pct double, source text, ingest_time text, PRIMARY KEY ((city, parameter, day), event_time)) WITH CLUSTERING ORDER BY (event_time DESC);\""
        ),
    )

    load_weather_cassandra = BashOperator(
        task_id="load_weather_cassandra",
        bash_command=spark_cassandra_command("weather"),
    )

    load_openaq_cassandra = BashOperator(
        task_id="load_openaq_cassandra",
        bash_command=spark_cassandra_command("openaq"),
    )

    pipeline_done = BashOperator(
        task_id="pipeline_done",
        bash_command=(
            "set -euo pipefail\n"
            "echo 'AIS pipeline finished: weather-history and openaq-hourly loaded into Iceberg and Cassandra'"
        ),
    )

    [ensure_weather_topic, ensure_openaq_topic] >> [run_weather_ingest, run_openaq_ingest] >> [run_weather_spark, run_openaq_spark] >> ensure_cassandra_schema >> [load_weather_cassandra, load_openaq_cassandra] >> pipeline_done
