from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

from ais_dag_utils import (
    ensure_streaming_job_command,
    kafka_lag_check_command,
)

DAG_ID = "ais_streaming_supervision"

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "do_xcom_push": False,
}

with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 4, 13),
    schedule=timedelta(minutes=15),
    catchup=False,
    max_active_runs=1,
    tags=["ais", "streaming", "supervision", "airflow"],
    description="Ensure long-running Spark streaming jobs are healthy and Kafka lag stays bounded",
) as dag:
    ensure_weather_stream = BashOperator(
        task_id="ensure_weather_stream",
        bash_command=ensure_streaming_job_command("weather"),
    )

    ensure_openaq_stream = BashOperator(
        task_id="ensure_openaq_stream",
        bash_command=ensure_streaming_job_command("openaq"),
    )

    ensure_sentinel5p_stream = BashOperator(
        task_id="ensure_sentinel5p_stream",
        bash_command=ensure_streaming_job_command("sentinel5p"),
    )

    ensure_maiac_stream = BashOperator(
        task_id="ensure_maiac_stream",
        bash_command=ensure_streaming_job_command("maiac"),
    )

    check_weather_kafka_lag = BashOperator(
        task_id="check_weather_kafka_lag",
        bash_command=kafka_lag_check_command("ais-stream-weather", "weather_history", 50000),
    )

    check_openaq_kafka_lag = BashOperator(
        task_id="check_openaq_kafka_lag",
        bash_command=kafka_lag_check_command("ais-stream-openaq", "openaq-hourly", 50000),
    )

    check_sentinel5p_kafka_lag = BashOperator(
        task_id="check_sentinel5p_kafka_lag",
        bash_command=kafka_lag_check_command("ais-stream-sentinel5p", "sentinel5p-summary", 50000),
    )

    check_maiac_kafka_lag = BashOperator(
        task_id="check_maiac_kafka_lag",
        bash_command=kafka_lag_check_command("ais-stream-maiac", "maiac-summary", 50000),
    )

    supervision_done = BashOperator(
        task_id="supervision_done",
        bash_command=(
            "set -euo pipefail\n"
            "echo 'Streaming supervision checks passed: stream jobs up, lag checks healthy.'"
        ),
    )

    ensure_weather_stream >> check_weather_kafka_lag
    ensure_openaq_stream >> check_openaq_kafka_lag
    ensure_sentinel5p_stream >> check_sentinel5p_kafka_lag
    ensure_maiac_stream >> check_maiac_kafka_lag

    [
        check_weather_kafka_lag,
        check_openaq_kafka_lag,
        check_sentinel5p_kafka_lag,
        check_maiac_kafka_lag,
    ] >> supervision_done
