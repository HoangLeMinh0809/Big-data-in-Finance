from __future__ import annotations

import os
from copy import deepcopy
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

try:
    import yaml
except ModuleNotFoundError:  # pragma: no cover - Spark images may not have PyYAML yet.
    yaml = None


ICEBERG_CATALOG = os.getenv("ICEBERG_CATALOG", "ais")
ICEBERG_WAREHOUSE = os.getenv("ICEBERG_WAREHOUSE", "hdfs://namenode:9000/warehouse/iceberg")

DEFAULT_CONFIG: dict[str, Any] = {
    "hanoi": {
        "bbox": {"west": 105.25, "east": 106.10, "south": 20.55, "north": 21.40},
        "center": {"lat": 21.0285, "lon": 105.8542},
    },
    "pm25_qc": {"min_value": 0.0, "max_value": 1000.0, "min_coverage_pct": 50.0},
    "era5": {
        "region": {"west": 95.0, "east": 115.0, "south": 5.0, "north": 35.0},
        "raw_base_path": "hdfs://namenode:9000/raw/era5",
        "surface_variables": [
            "10m_u_component_of_wind",
            "10m_v_component_of_wind",
            "boundary_layer_height",
            "surface_pressure",
            "2m_temperature",
            "2m_dewpoint_temperature",
            "total_precipitation",
            "mean_sea_level_pressure",
        ],
    },
    "sentinel5p": {
        "raw_base_path": "hdfs://namenode:9000/raw/sentinel5p",
        "products": ["NO2", "CO", "SO2", "O3", "AER_AI"],
    },
    "maiac": {
        "raw_base_path": "hdfs://namenode:9000/raw/maiac",
        "local_fallback_path": "crawler/maiac_data",
        "scale_factor": 0.001,
        "bands": ["AOD_047", "AOD_055"],
    },
    "gold": {
        "horizons_hours": [6, 12, 24],
        "lag_hours": [1, 3, 6, 12, 24],
        "rolling_hours": [3, 6, 24],
    },
}

TABLES = {
    "openaq_bronze": f"{ICEBERG_CATALOG}.air_quality.openaq_hourly_bronze",
    "weather_bronze": f"{ICEBERG_CATALOG}.weather.weather_history_bronze",
    "sentinel5p_bronze": f"{ICEBERG_CATALOG}.satellite.sentinel5p_summary_bronze",
    "maiac_bronze": f"{ICEBERG_CATALOG}.satellite.maiac_summary_bronze",
    "era5_files_bronze": f"{ICEBERG_CATALOG}.weather.era5_files_bronze",
    "openaq_station_silver": f"{ICEBERG_CATALOG}.air_quality.openaq_hanoi_station_hourly_silver",
    "openaq_hourly_silver": f"{ICEBERG_CATALOG}.air_quality.openaq_hanoi_hourly_silver",
    "weather_proxy_silver": f"{ICEBERG_CATALOG}.weather.weather_hanoi_surface_proxy_silver",
    "era5_surface_silver": f"{ICEBERG_CATALOG}.weather.era5_surface_hanoi_hourly_silver",
    "sentinel5p_silver": f"{ICEBERG_CATALOG}.satellite.sentinel5p_hanoi_daily_silver",
    "maiac_silver": f"{ICEBERG_CATALOG}.satellite.maiac_hanoi_daily_silver",
    "master_gold": f"{ICEBERG_CATALOG}.features.hanoi_pm25_master_hourly_gold",
    "training_gold": f"{ICEBERG_CATALOG}.features.hanoi_pm25_training_dataset_gold",
    "model_runs_gold": f"{ICEBERG_CATALOG}.models.hanoi_pm25_model_runs_gold",
}


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _candidate_config_paths() -> list[Path]:
    explicit = os.getenv("HANOI_PIPELINE_CONFIG", "").strip()
    paths = []
    if explicit:
        paths.append(Path(explicit))
    paths.extend(
        [
            Path("config/hanoi_pipeline.yaml"),
            Path("/opt/config/hanoi_pipeline.yaml"),
            Path("/opt/ais/config/hanoi_pipeline.yaml"),
            Path(__file__).resolve().parents[1] / "config" / "hanoi_pipeline.yaml",
        ]
    )
    return paths


def load_config() -> dict[str, Any]:
    cfg = DEFAULT_CONFIG
    if yaml is None:
        return _apply_env_overrides(cfg)

    for path in _candidate_config_paths():
        if path.exists():
            with path.open("r", encoding="utf-8") as f:
                loaded = yaml.safe_load(f) or {}
            if not isinstance(loaded, dict):
                raise ValueError(f"Invalid YAML root in {path}")
            cfg = _deep_merge(cfg, loaded)
            break
    return _apply_env_overrides(cfg)


def _apply_env_overrides(cfg: dict[str, Any]) -> dict[str, Any]:
    cfg = deepcopy(cfg)
    bbox = cfg["hanoi"]["bbox"]
    center = cfg["hanoi"]["center"]

    bbox["west"] = float(os.getenv("HANOI_BBOX_WEST", bbox["west"]))
    bbox["east"] = float(os.getenv("HANOI_BBOX_EAST", bbox["east"]))
    bbox["south"] = float(os.getenv("HANOI_BBOX_SOUTH", bbox["south"]))
    bbox["north"] = float(os.getenv("HANOI_BBOX_NORTH", bbox["north"]))
    center["lat"] = float(os.getenv("HANOI_CENTER_LAT", center["lat"]))
    center["lon"] = float(os.getenv("HANOI_CENTER_LON", center["lon"]))
    return cfg


def get_hanoi_bbox() -> dict[str, float]:
    bbox = load_config()["hanoi"]["bbox"]
    return {k: float(v) for k, v in bbox.items()}


def get_hanoi_center() -> dict[str, float]:
    center = load_config()["hanoi"]["center"]
    return {k: float(v) for k, v in center.items()}


def get_pm25_qc() -> dict[str, float]:
    qc = load_config()["pm25_qc"]
    return {k: float(v) for k, v in qc.items()}


def get_era5_region() -> dict[str, float]:
    region = load_config()["era5"]["region"]
    return {k: float(v) for k, v in region.items()}


def get_era5_raw_base_path() -> str:
    return str(load_config()["era5"]["raw_base_path"]).rstrip("/")


def get_era5_surface_variables() -> list[str]:
    return [str(v) for v in load_config()["era5"]["surface_variables"]]


def get_sentinel5p_raw_base_path() -> str:
    return str(load_config()["sentinel5p"]["raw_base_path"]).rstrip("/")


def get_sentinel5p_products() -> list[str]:
    return [str(v) for v in load_config()["sentinel5p"]["products"]]


def get_maiac_raw_base_path() -> str:
    return str(load_config()["maiac"]["raw_base_path"]).rstrip("/")


def get_maiac_local_fallback_path() -> str:
    return str(load_config()["maiac"]["local_fallback_path"])


def get_maiac_scale_factor() -> float:
    return float(load_config()["maiac"]["scale_factor"])


def get_gold_horizons_hours() -> list[int]:
    return [int(v) for v in load_config()["gold"]["horizons_hours"]]


def get_gold_lag_hours() -> list[int]:
    return [int(v) for v in load_config()["gold"]["lag_hours"]]


def get_gold_rolling_hours() -> list[int]:
    return [int(v) for v in load_config()["gold"]["rolling_hours"]]


def get_table_names() -> dict[str, str]:
    return TABLES.copy()


def filter_hanoi_bbox(df: DataFrame, lat_col: str, lon_col: str) -> DataFrame:
    bbox = get_hanoi_bbox()
    return df.filter(
        F.col(lat_col).between(bbox["south"], bbox["north"])
        & F.col(lon_col).between(bbox["west"], bbox["east"])
    )
