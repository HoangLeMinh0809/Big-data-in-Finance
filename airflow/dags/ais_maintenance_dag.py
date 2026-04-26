from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

from ais_dag_utils import (
    ensure_iceberg_tables_command,
    iceberg_maintenance_command,
    reconcile_serving_command,
)

DAG_ID = "ais_maintenance"

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
    tags=["ais", "maintenance", "validation", "reconciliation"],
    description="Iceberg maintenance and serving-layer reconciliation checks",
) as dag:
    ensure_iceberg_tables = BashOperator(
        task_id="ensure_iceberg_tables",
        bash_command=ensure_iceberg_tables_command(),
    )

    run_iceberg_maintenance = BashOperator(
        task_id="run_iceberg_maintenance",
        bash_command=iceberg_maintenance_command(retention_hours=168),
    )

    reconcile_serving = BashOperator(
        task_id="reconcile_serving",
        bash_command=reconcile_serving_command(lookback_hours=24, tolerance=0.90),
    )

    maintenance_done = BashOperator(
        task_id="maintenance_done",
        bash_command=(
            "set -euo pipefail\n"
            "echo 'Maintenance completed: Iceberg optimized and serving reconciliation validated.'"
        ),
    )

    ensure_iceberg_tables >> run_iceberg_maintenance >> reconcile_serving >> maintenance_done
