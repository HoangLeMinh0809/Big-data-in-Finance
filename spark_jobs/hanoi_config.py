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
    "weather_bronze": f"{ICEBERG_CATALOG}.weather.weather_history_bronze",
    "maiac_bronze": f"{ICEBERG_CATALOG}.satellite.maiac_summary_bronze",
    "weather_proxy_silver": f"{ICEBERG_CATALOG}.weather.weather_hanoi_surface_proxy_silver",
    "maiac_silver": f"{ICEBERG_CATALOG}.satellite.maiac_hanoi_daily_silver",
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
