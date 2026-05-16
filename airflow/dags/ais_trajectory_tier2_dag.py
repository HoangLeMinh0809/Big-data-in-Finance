from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

from ais_dag_utils import ensure_iceberg_tables_command


DAG_ID = "ais_trajectory_tier2"

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
DATASET_VERSION_TEMPLATE = "{{ dag_run.conf.get('dataset_version', 'hanoi_pm25_v1') if dag_run and dag_run.conf else 'hanoi_pm25_v1' }}"
FEATURE_SET_NAME_TEMPLATE = "{{ dag_run.conf.get('feature_set_name', 'hanoi_pm25_core_v1') if dag_run and dag_run.conf else 'hanoi_pm25_core_v1' }}"
MODEL_TYPE_TEMPLATE = "{{ dag_run.conf.get('model_type', 'lightgbm') if dag_run and dag_run.conf else 'lightgbm' }}"


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
    tags=["ais", "trajectory", "tier2", "hanoi", "pm25"],
    description="Tier-2 Hanoi trajectory pipeline: ERA5 pressure ingest -> ARL -> HYSPLIT -> sampling -> gold datasets -> training.",
) as dag:
    ensure_iceberg_tables = BashOperator(
        task_id="ensure_iceberg_tables",
        bash_command=ensure_iceberg_tables_command(),
    )

    era5_pressure_ingest = BashOperator(
        task_id="era5_pressure_ingest",
        bash_command=submit_command(
            "era5-ingest",
            extra_env=(
                f"ERA5_START_DATE={START_DATE_TEMPLATE} "
                f"ERA5_END_DATE={END_DATE_TEMPLATE} "
                "ERA5_DATASET_TYPE=pressure_levels "
                "KAFKA_TOPIC=era5-files"
            ),
        ),
    )

    # Convert pressure-level ERA5 -> ARL (writes Iceberg bronze metadata)
    era5_pressure_to_arl = BashOperator(
        task_id="era5_pressure_to_arl",
        bash_command=submit_command(
            "era5-pressure-arl",
            extra_env=f"START_DATE={START_DATE_TEMPLATE} END_DATE={END_DATE_TEMPLATE} FULL_REFRESH={FULL_REFRESH_TEMPLATE}",
        ),
    )

    hysplit_trajectory_run = BashOperator(
        task_id="hysplit_trajectory_run",
        bash_command=submit_command(
            "hysplit-run",
            extra_env=f"START_DATE={START_DATE_TEMPLATE} END_DATE={END_DATE_TEMPLATE} FULL_REFRESH={FULL_REFRESH_TEMPLATE}",
        ),
    )

    hysplit_trajectory_parse = BashOperator(
        task_id="hysplit_trajectory_parse",
        bash_command=submit_command(
            "hysplit-parse",
            extra_env=f"START_DATE={START_DATE_TEMPLATE} END_DATE={END_DATE_TEMPLATE} FULL_REFRESH={FULL_REFRESH_TEMPLATE}",
        ),
    )

    hysplit_trajectory_cluster = BashOperator(
        task_id="hysplit_trajectory_cluster",
        bash_command=submit_command(
            "hysplit-cluster",
            extra_env=f"START_DATE={START_DATE_TEMPLATE} END_DATE={END_DATE_TEMPLATE} FULL_REFRESH={FULL_REFRESH_TEMPLATE}",
        ),
    )

    openaq_spatial_gradient = BashOperator(
        task_id="openaq_spatial_gradient",
        bash_command=submit_command(
            "openaq-gradient",
            extra_env=f"START_DATE={START_DATE_TEMPLATE} END_DATE={END_DATE_TEMPLATE} FULL_REFRESH={FULL_REFRESH_TEMPLATE}",
        ),
    )

    sentinel5p_grid_silver = BashOperator(
        task_id="sentinel5p_grid_silver",
        bash_command=submit_command(
            "s5p-grid-silver",
            extra_env=f"START_DATE={START_DATE_TEMPLATE} END_DATE={END_DATE_TEMPLATE} FULL_REFRESH={FULL_REFRESH_TEMPLATE}",
        ),
    )

    trajectory_path_sampling = BashOperator(
        task_id="trajectory_path_sampling",
        bash_command=submit_command(
            "traj-path-sampling",
            extra_env=f"START_DATE={START_DATE_TEMPLATE} END_DATE={END_DATE_TEMPLATE} FULL_REFRESH={FULL_REFRESH_TEMPLATE}",
        ),
    )

    trajectory_hourly_features = BashOperator(
        task_id="trajectory_hourly_features",
        bash_command=submit_command(
            "traj-hourly-features",
            extra_env=f"START_DATE={START_DATE_TEMPLATE} END_DATE={END_DATE_TEMPLATE} FULL_REFRESH={FULL_REFRESH_TEMPLATE}",
        ),
    )

    hanoi_pm25_master_features_gold = BashOperator(
        task_id="hanoi_pm25_master_features_gold",
        bash_command=submit_command(
            "hanoi-master-features-gold",
            extra_env=f"START_DATE={START_DATE_TEMPLATE} END_DATE={END_DATE_TEMPLATE} FULL_REFRESH={FULL_REFRESH_TEMPLATE}",
        ),
    )

    hanoi_pm25_training_dataset_gold = BashOperator(
        task_id="hanoi_pm25_training_dataset_gold",
        bash_command=submit_command(
            "hanoi-training-dataset-gold",
            extra_env=(
                f"START_DATE={START_DATE_TEMPLATE} "
                f"END_DATE={END_DATE_TEMPLATE} "
                f"FULL_REFRESH={FULL_REFRESH_TEMPLATE} "
                f"DATASET_VERSION={DATASET_VERSION_TEMPLATE} "
                f"FEATURE_SET_NAME={FEATURE_SET_NAME_TEMPLATE}"
            ),
        ),
    )

    train_hanoi_pm25 = BashOperator(
        task_id="train_hanoi_pm25",
        bash_command=submit_command(
            "hanoi-train-baseline",
            extra_env=(
                f"DATASET_VERSION={DATASET_VERSION_TEMPLATE} "
                f"FEATURE_SET_NAME={FEATURE_SET_NAME_TEMPLATE} "
                f"MODEL_TYPE={MODEL_TYPE_TEMPLATE}"
            ),
        ),
    )

    # Flow
    ensure_iceberg_tables >> era5_pressure_ingest >> era5_pressure_to_arl
    era5_pressure_to_arl >> hysplit_trajectory_run >> hysplit_trajectory_parse >> hysplit_trajectory_cluster

    # Feature engineering branch
    hysplit_trajectory_cluster >> trajectory_path_sampling
    sentinel5p_grid_silver >> trajectory_path_sampling

    trajectory_path_sampling >> trajectory_hourly_features
    openaq_spatial_gradient >> hanoi_pm25_master_features_gold
    trajectory_hourly_features >> hanoi_pm25_master_features_gold

    hanoi_pm25_master_features_gold >> hanoi_pm25_training_dataset_gold >> train_hanoi_pm25

    # Independent inputs
    ensure_iceberg_tables >> [openaq_spatial_gradient, sentinel5p_grid_silver]
