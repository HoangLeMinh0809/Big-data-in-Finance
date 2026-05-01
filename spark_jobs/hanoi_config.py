from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import yaml
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

DEFAULT_CONFIG_PATH = Path(os.getenv("HANOI_PIPELINE_CONFIG", "config/hanoi_pipeline.yaml"))


TABLES = {
    "openaq_bronze": "ais.air_quality.openaq_hourly_bronze",
    "weather_bronze": "ais.weather.weather_history_bronze",
    "sentinel5p_bronze": "ais.satellite.sentinel5p_summary_bronze",
    "maiac_bronze": "ais.satellite.maiac_summary_bronze",
    "era5_files_bronze": "ais.weather.era5_files_bronze",
    "openaq_station_silver": "ais.air_quality.openaq_hanoi_station_hourly_silver",
    "openaq_hourly_silver": "ais.air_quality.openaq_hanoi_hourly_silver",
    "weather_proxy_silver": "ais.weather.weather_hanoi_surface_proxy_silver",
    "era5_surface_silver": "ais.weather.era5_surface_hanoi_hourly_silver",
    "sentinel5p_silver": "ais.satellite.sentinel5p_hanoi_daily_silver",
    "maiac_silver": "ais.satellite.maiac_hanoi_daily_silver",
    "master_gold": "ais.features.hanoi_pm25_master_hourly_gold",
    "training_gold": "ais.features.hanoi_pm25_training_dataset_gold",
}


@dataclass(frozen=True)
class BBox:
    west: float
    east: float
    south: float
    north: float


@dataclass(frozen=True)
class Center:
    lat: float
    lon: float


def load_config(config_path: str | os.PathLike[str] | None = None) -> dict[str, Any]:
    path = Path(config_path) if config_path else DEFAULT_CONFIG_PATH
    if not path.exists():
        raise FileNotFoundError(f"Missing Hanoi pipeline config: {path}")

    with path.open("r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}

    if not isinstance(cfg, dict):
        raise ValueError(f"Invalid YAML root (expected mapping): {path}")

    return cfg


def _require_mapping(cfg: Mapping[str, Any], key: str) -> Mapping[str, Any]:
    value = cfg.get(key)
    if not isinstance(value, Mapping):
        raise KeyError(f"Missing or invalid config section: {key}")
    return value


def get_hanoi_bbox(cfg: Mapping[str, Any] | None = None) -> BBox:
    cfg = cfg or load_config()
    hanoi = _require_mapping(cfg, "hanoi")
    bbox = _require_mapping(hanoi, "bbox")

    return BBox(
        west=float(bbox["west"]),
        east=float(bbox["east"]),
        south=float(bbox["south"]),
        north=float(bbox["north"]),
    )


def get_hanoi_center(cfg: Mapping[str, Any] | None = None) -> Center:
    cfg = cfg or load_config()
    hanoi = _require_mapping(cfg, "hanoi")
    center = _require_mapping(hanoi, "center")

    return Center(lat=float(center["lat"]), lon=float(center["lon"]))


def get_table_names() -> dict[str, str]:
    return dict(TABLES)


def filter_hanoi_bbox(df: DataFrame, lat_col: str, lon_col: str, cfg: Mapping[str, Any] | None = None) -> DataFrame:
    bbox = get_hanoi_bbox(cfg)
    return df.filter(
        (col(lat_col) >= bbox.south)
        & (col(lat_col) <= bbox.north)
        & (col(lon_col) >= bbox.west)
        & (col(lon_col) <= bbox.east)
    )
