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


def spark_submit_command(app_name: str, job_file: str) -> str:
    return (
        "set -euo pipefail\n"
        f"docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --name \"{app_name}\"{SPARK_COMMON_CONF} {job_file}"
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

    pipeline_done = BashOperator(
        task_id="pipeline_done",
        bash_command=(
            "set -euo pipefail\n"
            "echo 'AIS pipeline finished: weather-history and openaq-hourly loaded into Iceberg'"
        ),
    )

    [ensure_weather_topic, ensure_openaq_topic] >> [run_weather_ingest, run_openaq_ingest] >> [run_weather_spark, run_openaq_spark] >> pipeline_done
