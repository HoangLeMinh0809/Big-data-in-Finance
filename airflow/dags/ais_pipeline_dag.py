from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

from ais_dag_utils import (
    LOOKBACK_DAYS_TEMPLATE,
    MAIAC_LOOKBACK_DAYS_TEMPLATE,
    compose_ingest_command,
    ensure_cassandra_schema_command,
    ensure_iceberg_tables_command,
    ensure_topics_command,
    spark_cassandra_command,
    spark_submit_command,
)

DAG_ID = "ais_batch_orchestration"

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
    schedule=timedelta(days=7),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    tags=["ais", "bootstrap", "historical", "airflow"],
    description=(
        "Historical bootstrap/backfill orchestration: ingest to Kafka, one-shot Spark "
        "loads to Iceberg, then serving refresh to Cassandra"
    ),
) as dag:
    ensure_kafka_topics = BashOperator(
        task_id="ensure_kafka_topics",
        bash_command=ensure_topics_command(),
    )

    ensure_iceberg_tables = BashOperator(
        task_id="ensure_iceberg_tables",
        bash_command=ensure_iceberg_tables_command(),
    )

    ensure_cassandra_schema = BashOperator(
        task_id="ensure_cassandra_schema",
        bash_command=ensure_cassandra_schema_command(),
    )

    run_weather_ingest = BashOperator(
        task_id="run_weather_ingest",
        bash_command=compose_ingest_command(
            "ingest",
            "ingest_weather.py",
            lookback_days_template=LOOKBACK_DAYS_TEMPLATE,
        ),
    )

    run_openaq_ingest = BashOperator(
        task_id="run_openaq_ingest",
        bash_command=compose_ingest_command(
            "openaq-ingest",
            "openaq_ingest.py",
            lookback_days_template=LOOKBACK_DAYS_TEMPLATE,
        ),
    )

    run_sentinel5p_ingest = BashOperator(
        task_id="run_sentinel5p_ingest",
        bash_command=compose_ingest_command(
            "sentinel5p-ingest",
            "sentinel5p_ingest.py",
            lookback_days_template=LOOKBACK_DAYS_TEMPLATE,
        ),
    )

    run_maiac_ingest = BashOperator(
        task_id="run_maiac_ingest",
        bash_command=compose_ingest_command(
            "maiac-ingest",
            "maiac_ingest.py",
            lookback_days_template=MAIAC_LOOKBACK_DAYS_TEMPLATE,
        ),
    )

    process_weather_to_iceberg = BashOperator(
        task_id="process_weather_to_iceberg",
        bash_command=spark_submit_command(
            "WeatherHistory_Bootstrap",
            "/opt/spark-jobs/weather_streaming.py",
            starting_offsets="earliest",
        ),
    )

    process_openaq_to_iceberg = BashOperator(
        task_id="process_openaq_to_iceberg",
        bash_command=spark_submit_command(
            "OpenAQHourly_Bootstrap",
            "/opt/spark-jobs/openaq_hourly_streaming.py",
            starting_offsets="earliest",
        ),
    )

    process_sentinel5p_to_iceberg = BashOperator(
        task_id="process_sentinel5p_to_iceberg",
        bash_command=spark_submit_command(
            "Sentinel5PSummary_Bootstrap",
            "/opt/spark-jobs/sentinel5p_summary_streaming.py",
            starting_offsets="earliest",
        ),
    )

    process_maiac_to_iceberg = BashOperator(
        task_id="process_maiac_to_iceberg",
        bash_command=spark_submit_command(
            "MAIACSummary_Bootstrap",
            "/opt/spark-jobs/maiac_summary_streaming.py",
            starting_offsets="earliest",
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

    bootstrap_done = BashOperator(
        task_id="bootstrap_done",
        bash_command=(
            "set -euo pipefail\n"
            "echo 'Historical bootstrap complete: Iceberg is the historical source of truth; Cassandra is refreshed for serving.'"
        ),
    )

    ensure_kafka_topics >> [
        run_weather_ingest,
        run_openaq_ingest,
        run_sentinel5p_ingest,
        run_maiac_ingest,
    ]

    ensure_iceberg_tables >> [
        process_weather_to_iceberg,
        process_openaq_to_iceberg,
        process_sentinel5p_to_iceberg,
        process_maiac_to_iceberg,
    ]

    run_weather_ingest >> process_weather_to_iceberg
    run_openaq_ingest >> process_openaq_to_iceberg
    run_sentinel5p_ingest >> process_sentinel5p_to_iceberg
    run_maiac_ingest >> process_maiac_to_iceberg

    [process_weather_to_iceberg, process_openaq_to_iceberg] >> ensure_cassandra_schema
    ensure_cassandra_schema >> [load_weather_cassandra, load_openaq_cassandra]

    [
        load_weather_cassandra,
        load_openaq_cassandra,
        process_sentinel5p_to_iceberg,
        process_maiac_to_iceberg,
    ] >> bootstrap_done
