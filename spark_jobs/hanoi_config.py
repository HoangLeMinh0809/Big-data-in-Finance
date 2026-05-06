from __future__ import annotations

import os

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

HANOI_BBOX = {
    "west": float(os.getenv("HANOI_BBOX_WEST", "105.25")),
    "east": float(os.getenv("HANOI_BBOX_EAST", "106.10")),
    "south": float(os.getenv("HANOI_BBOX_SOUTH", "20.55")),
    "north": float(os.getenv("HANOI_BBOX_NORTH", "21.40")),
}

HANOI_CENTER = {
    "lat": float(os.getenv("HANOI_CENTER_LAT", "21.0285")),
    "lon": float(os.getenv("HANOI_CENTER_LON", "105.8542")),
}

ICEBERG_CATALOG = os.getenv("ICEBERG_CATALOG", "ais")
ICEBERG_WAREHOUSE = os.getenv("ICEBERG_WAREHOUSE", "hdfs://namenode:9000/warehouse/iceberg")

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
}


def load_config() -> dict[str, object]:
    return {
        "hanoi": {
            "bbox": get_hanoi_bbox(),
            "center": get_hanoi_center(),
        },
        "tables": get_table_names(),
    }


def get_hanoi_bbox() -> dict[str, float]:
    return HANOI_BBOX.copy()


def get_hanoi_center() -> dict[str, float]:
    return HANOI_CENTER.copy()


def get_table_names() -> dict[str, str]:
    return TABLES.copy()


def filter_hanoi_bbox(df: DataFrame, lat_col: str, lon_col: str) -> DataFrame:
    bbox = get_hanoi_bbox()
    return df.filter(
        F.col(lat_col).between(bbox["south"], bbox["north"])
        & F.col(lon_col).between(bbox["west"], bbox["east"])
    )
