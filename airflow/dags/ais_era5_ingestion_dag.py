from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

from ais_dag_utils import ensure_iceberg_tables_command, ensure_topics_command


DAG_ID = "ais_era5_ingestion"

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "do_xcom_push": False,
}

START_DATE_TEMPLATE = "{{ dag_run.conf.get('start_date', ds) if dag_run and dag_run.conf else ds }}"
END_DATE_TEMPLATE = "{{ dag_run.conf.get('end_date', ds) if dag_run and dag_run.conf else ds }}"
FULL_REFRESH_TEMPLATE = "{{ dag_run.conf.get('full_refresh', 0) if dag_run and dag_run.conf else 0 }}"


def submit_command(job_type: str, *, extra_env: str = "") -> str:
    env = extra_env.strip()
    env_prefix = f"{env} " if env else ""
    return (
        "set -euo pipefail\n"
        "cd /opt/ais\n"
        f"{env_prefix}bash scripts/submit_spark.sh {job_type}"
    )


with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 4, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    tags=["ais", "era5", "hanoi", "silver"],
    description="ERA5 surface ingest: raw NetCDF metadata to Iceberg bronze, then Hanoi surface silver.",
) as dag:
    ensure_kafka_topic_era5_files = BashOperator(
        task_id="ensure_kafka_topic_era5_files",
        bash_command=ensure_topics_command(),
    )

    ensure_iceberg_tables = BashOperator(
        task_id="ensure_iceberg_tables",
        bash_command=ensure_iceberg_tables_command(),
    )

    download_era5_surface = BashOperator(
        task_id="download_era5_surface",
        bash_command=submit_command(
            "era5-ingest",
            extra_env=(
                f"ERA5_START_DATE={START_DATE_TEMPLATE} "
                f"ERA5_END_DATE={END_DATE_TEMPLATE} "
                "ERA5_DATASET_TYPE=surface "
                "KAFKA_TOPIC=era5-files"
            ),
        ),
    )

    process_era5_files_to_iceberg = BashOperator(
        task_id="process_era5_files_to_iceberg",
        bash_command=submit_command(
            "era5-files",
            extra_env="STOP_AFTER_BATCH=true KAFKA_STARTING_OFFSETS=earliest KAFKA_TOPIC=era5-files",
        ),
    )

    process_era5_surface_hanoi_silver = BashOperator(
        task_id="process_era5_surface_hanoi_silver",
        bash_command=submit_command(
            "era5-surface-hanoi-silver",
            extra_env=(
                f"START_DATE={START_DATE_TEMPLATE} "
                f"END_DATE={END_DATE_TEMPLATE} "
                f"FULL_REFRESH={FULL_REFRESH_TEMPLATE}"
            ),
        ),
    )

    [ensure_kafka_topic_era5_files, ensure_iceberg_tables] >> download_era5_surface
    download_era5_surface >> process_era5_files_to_iceberg >> process_era5_surface_hanoi_silver
