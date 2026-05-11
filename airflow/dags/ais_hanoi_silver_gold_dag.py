from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

from ais_dag_utils import ensure_iceberg_tables_command


DAG_ID = "ais_hanoi_silver_gold"

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "do_xcom_push": False,
}

START_DATE_TEMPLATE = "{{ dag_run.conf.get('start_date', ds) if dag_run and dag_run.conf else ds }}"
END_DATE_TEMPLATE = "{{ dag_run.conf.get('end_date', ds) if dag_run and dag_run.conf else ds }}"
FULL_REFRESH_TEMPLATE = "{{ dag_run.conf.get('full_refresh', 0) if dag_run and dag_run.conf else 0 }}"
DATASET_VERSION_TEMPLATE = "{{ dag_run.conf.get('dataset_version', 'hanoi_pm25_v1') if dag_run and dag_run.conf else 'hanoi_pm25_v1' }}"


def submit_command(job_type: str, *, include_dataset_version: bool = False) -> str:
    dataset_env = f" DATASET_VERSION={DATASET_VERSION_TEMPLATE}" if include_dataset_version else ""
    return (
        "set -euo pipefail\n"
        "cd /opt/ais\n"
        f"START_DATE={START_DATE_TEMPLATE} "
        f"END_DATE={END_DATE_TEMPLATE} "
        f"FULL_REFRESH={FULL_REFRESH_TEMPLATE}"
        f"{dataset_env} "
        f"bash scripts/submit_spark.sh {job_type}"
    )


with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 4, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    tags=["ais", "hanoi", "silver", "gold", "pm25"],
    description="Build Hanoi PM2.5 silver tables and gold feature/training datasets.",
) as dag:
    ensure_iceberg_tables = BashOperator(
        task_id="ensure_iceberg_tables",
        bash_command=ensure_iceberg_tables_command(),
    )

    hanoi_openaq_silver = BashOperator(
        task_id="hanoi_openaq_silver",
        bash_command=submit_command("hanoi-openaq-silver"),
    )

    hanoi_weather_surface_proxy_silver = BashOperator(
        task_id="hanoi_weather_surface_proxy_silver",
        bash_command=submit_command("hanoi-weather-silver"),
    )

    era5_surface_hanoi_silver = BashOperator(
        task_id="era5_surface_hanoi_silver",
        bash_command=submit_command("era5-surface-hanoi-silver"),
    )

    sentinel5p_hanoi_silver = BashOperator(
        task_id="sentinel5p_hanoi_silver",
        bash_command=submit_command("sentinel5p-hanoi-silver"),
    )

    maiac_hanoi_silver = BashOperator(
        task_id="maiac_hanoi_silver",
        bash_command=submit_command("maiac-hanoi-silver"),
    )

    hanoi_pm25_master_features_gold = BashOperator(
        task_id="hanoi_pm25_master_features_gold",
        bash_command=submit_command("hanoi-master-features-gold"),
    )

    hanoi_pm25_training_dataset_gold = BashOperator(
        task_id="hanoi_pm25_training_dataset_gold",
        bash_command=submit_command("hanoi-training-dataset-gold", include_dataset_version=True),
    )

    ensure_iceberg_tables >> [
        hanoi_openaq_silver,
        hanoi_weather_surface_proxy_silver,
        era5_surface_hanoi_silver,
        sentinel5p_hanoi_silver,
        maiac_hanoi_silver,
    ]

    [
        hanoi_openaq_silver,
        hanoi_weather_surface_proxy_silver,
        era5_surface_hanoi_silver,
        sentinel5p_hanoi_silver,
        maiac_hanoi_silver,
    ] >> hanoi_pm25_master_features_gold

    hanoi_pm25_master_features_gold >> hanoi_pm25_training_dataset_gold
