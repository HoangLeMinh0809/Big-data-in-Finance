from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

from ais_dag_utils import (
    MAIAC_LOOKBACK_DAYS_TEMPLATE,
    compose_ingest_command,
    ensure_iceberg_tables_command,
    ensure_topics_command,
    spark_submit_command,
)

DAG_ID = "ais_maiac_backfill"

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "do_xcom_push": False,
}

with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 4, 13),
    schedule=timedelta(days=1),
    catchup=False,
    max_active_runs=1,
    tags=["ais", "maiac", "backfill", "batch"],
    description="Periodic delayed-batch MAIAC backfill into Iceberg",
) as dag:
    ensure_kafka_topics = BashOperator(
        task_id="ensure_kafka_topics",
        bash_command=ensure_topics_command(),
    )

    ensure_iceberg_tables = BashOperator(
        task_id="ensure_iceberg_tables",
        bash_command=ensure_iceberg_tables_command(),
    )

    run_maiac_ingest = BashOperator(
        task_id="run_maiac_ingest",
        bash_command=compose_ingest_command(
            "maiac-ingest",
            "maiac_ingest.py",
            lookback_days_template=MAIAC_LOOKBACK_DAYS_TEMPLATE,
        ),
    )

    process_maiac_to_iceberg = BashOperator(
        task_id="process_maiac_to_iceberg",
        bash_command=spark_submit_command(
            "MAIACSummary_Backfill",
            "/opt/spark-jobs/maiac_summary_streaming.py",
            extra_args="--stop-after-batch 1",
            starting_offsets="earliest",
        ),
    )

    refresh_maiac_serving = BashOperator(
        task_id="refresh_maiac_serving",
        bash_command=(
            "set -euo pipefail\n"
            "echo 'MAIAC serving refresh skipped: no Cassandra serving table defined for MAIAC in current codebase.'"
        ),
    )

    maiac_backfill_done = BashOperator(
        task_id="maiac_backfill_done",
        bash_command=(
            "set -euo pipefail\n"
            "echo 'MAIAC delayed backfill completed and persisted in Iceberg.'"
        ),
    )

    [ensure_kafka_topics, ensure_iceberg_tables] >> run_maiac_ingest
    [ensure_iceberg_tables, run_maiac_ingest] >> process_maiac_to_iceberg
    process_maiac_to_iceberg >> refresh_maiac_serving >> maiac_backfill_done
