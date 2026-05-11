from __future__ import annotations

import argparse
import math
import os
import subprocess
import tempfile
from datetime import date, datetime
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from hanoi_config import ICEBERG_CATALOG, ICEBERG_WAREHOUSE, get_hanoi_bbox, get_hanoi_center, get_table_names

try:
    import netCDF4 as nc  # type: ignore
    import numpy as np  # type: ignore
except Exception as exc:  # pragma: no cover - checked at runtime.
    nc = None  # type: ignore[assignment]
    np = None  # type: ignore[assignment]
    NETCDF_IMPORT_ERROR = exc
else:
    NETCDF_IMPORT_ERROR = None


OUTPUT_COLUMNS = [
    "hour",
    "wind_u10",
    "wind_v10",
    "wind_speed",
    "wind_dir",
    "pbl_height_m",
    "low_pbl",
    "surface_pressure",
    "temperature_2m_c",
    "dewpoint_2m_c",
    "total_precipitation_mm",
    "mean_sea_level_pressure",
    "grid_point_count",
    "source_file",
    "year",
    "month",
    "day",
    "spark_processed_at",
]

OUTPUT_SCHEMA = StructType(
    [
        StructField("hour", TimestampType(), False),
        StructField("wind_u10", DoubleType(), True),
        StructField("wind_v10", DoubleType(), True),
        StructField("wind_speed", DoubleType(), True),
        StructField("wind_dir", DoubleType(), True),
        StructField("pbl_height_m", DoubleType(), True),
        StructField("low_pbl", BooleanType(), True),
        StructField("surface_pressure", DoubleType(), True),
        StructField("temperature_2m_c", DoubleType(), True),
        StructField("dewpoint_2m_c", DoubleType(), True),
        StructField("total_precipitation_mm", DoubleType(), True),
        StructField("mean_sea_level_pressure", DoubleType(), True),
        StructField("grid_point_count", IntegerType(), True),
        StructField("source_file", StringType(), True),
        StructField("year", IntegerType(), False),
        StructField("month", IntegerType(), False),
        StructField("day", IntegerType(), False),
        StructField("spark_processed_at", TimestampType(), False),
    ]
)

VAR_ALIASES = {
    "wind_u10": ["u10", "10u", "10m_u_component_of_wind"],
    "wind_v10": ["v10", "10v", "10m_v_component_of_wind"],
    "pbl_height_m": ["blh", "boundary_layer_height"],
    "surface_pressure": ["sp", "surface_pressure"],
    "temperature_2m_c": ["t2m", "2t", "2m_temperature"],
    "dewpoint_2m_c": ["d2m", "2d", "2m_dewpoint_temperature"],
    "total_precipitation_mm": ["tp", "total_precipitation"],
    "mean_sea_level_pressure": ["msl", "msl_pressure", "mean_sea_level_pressure"],
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Hanoi ERA5 surface hourly silver table")
    parser.add_argument("--start-date", default=os.getenv("START_DATE", ""))
    parser.add_argument("--end-date", default=os.getenv("END_DATE", ""))
    parser.add_argument("--full-refresh", default=os.getenv("FULL_REFRESH", "0"))
    return parser.parse_args()


def as_bool(raw: str) -> bool:
    return str(raw or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def parse_date(raw: str) -> date | None:
    return datetime.strptime(raw, "%Y-%m-%d").date() if raw else None


def require_netcdf() -> None:
    if nc is None or np is None:
        raise RuntimeError("ERA5 surface silver requires netCDF4 and numpy in the Spark Python environment") from NETCDF_IMPORT_ERROR


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("ERA5SurfaceHanoiSilver")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.warehouse", ICEBERG_WAREHOUSE)
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
        .getOrCreate()
    )


def ensure_table(spark: SparkSession, table_name: str) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ICEBERG_CATALOG}.weather")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            hour TIMESTAMP,
            wind_u10 DOUBLE,
            wind_v10 DOUBLE,
            wind_speed DOUBLE,
            wind_dir DOUBLE,
            pbl_height_m DOUBLE,
            low_pbl BOOLEAN,
            surface_pressure DOUBLE,
            temperature_2m_c DOUBLE,
            dewpoint_2m_c DOUBLE,
            total_precipitation_mm DOUBLE,
            mean_sea_level_pressure DOUBLE,
            grid_point_count INT,
            source_file STRING,
            year INT,
            month INT,
            day INT,
            spark_processed_at TIMESTAMP
        )
        USING ICEBERG
        PARTITIONED BY (year, month, day)
        TBLPROPERTIES ('format-version'='2')
        """
    )


def collect_candidate_files(spark: SparkSession, source_table: str, start_date: date | None, end_date: date | None) -> list[dict[str, Any]]:
    df = spark.table(source_table).filter(F.col("dataset_type") == F.lit("surface"))
    if start_date:
        df = df.filter(F.to_date("end_time") >= F.lit(start_date.isoformat()))
    if end_date:
        df = df.filter(F.to_date("start_time") <= F.lit(end_date.isoformat()))
    rows = (
        df.select("event_id", "file_path", "start_time", "end_time", "checksum")
        .dropDuplicates(["event_id"])
        .collect()
    )
    return [row.asDict(recursive=True) for row in rows if row["file_path"]]


def copy_hdfs_to_local(path: str) -> Path:
    if not path.startswith("hdfs://"):
        return Path(path)

    remote = "/" + path.split("/", 3)[3]
    local_dir = Path(tempfile.mkdtemp(prefix="era5_"))
    local_path = local_dir / Path(remote).name
    commands = [
        ["hdfs", "dfs", "-copyToLocal", "-f", remote, str(local_path)],
        ["/opt/hadoop/bin/hdfs", "dfs", "-copyToLocal", "-f", remote, str(local_path)],
    ]
    for command in commands:
        try:
            subprocess.check_call(command)
            return local_path
        except (FileNotFoundError, subprocess.CalledProcessError):
            continue
    raise RuntimeError(f"Unable to copy HDFS file to local path: {path}")


def _find_variable(dataset, aliases: list[str]):
    lowered = {name.lower(): name for name in dataset.variables}
    for alias in aliases:
        match = lowered.get(alias.lower())
        if match:
            return dataset.variables[match]
    return None


def _time_values(dataset) -> list[datetime]:
    time_var = dataset.variables.get("valid_time") or dataset.variables.get("time")
    if time_var is None:
        raise KeyError("ERA5 NetCDF missing valid_time/time variable")
    raw = np.asarray(time_var[:])
    units = getattr(time_var, "units", "")
    calendar = getattr(time_var, "calendar", "standard")
    if units:
        values = nc.num2date(raw, units=units, calendar=calendar, only_use_cftime_datetimes=False)
        return [datetime(v.year, v.month, v.day, v.hour, v.minute, v.second) for v in values]
    return [datetime.utcfromtimestamp(float(v)) for v in raw]


def _lat_lon(dataset):
    lat_var = dataset.variables.get("latitude") or dataset.variables.get("lat")
    lon_var = dataset.variables.get("longitude") or dataset.variables.get("lon")
    if lat_var is None or lon_var is None:
        raise KeyError("ERA5 NetCDF missing latitude/longitude variables")
    lat = np.asarray(lat_var[:], dtype=float)
    lon = np.asarray(lon_var[:], dtype=float)
    if lat.ndim == 1 and lon.ndim == 1:
        lon_grid, lat_grid = np.meshgrid(lon, lat)
    else:
        lat_grid = lat
        lon_grid = lon
    return lat, lon, lat_grid, lon_grid


def _to_time_lat_lon(variable, arr, time_len: int, lat_len: int, lon_len: int):
    dims = list(getattr(variable, "dimensions", []))
    data = np.ma.filled(np.ma.asarray(arr), np.nan).astype(float)
    data = np.where(np.isfinite(data), data, np.nan)

    time_axis = next((i for i, d in enumerate(dims) if d in {"time", "valid_time"}), None)
    lat_axis = next((i for i, d in enumerate(dims) if d in {"latitude", "lat"}), None)
    lon_axis = next((i for i, d in enumerate(dims) if d in {"longitude", "lon"}), None)

    if time_axis is None or lat_axis is None or lon_axis is None:
        shape = data.shape
        try:
            time_axis = shape.index(time_len)
            lat_axis = shape.index(lat_len)
            lon_axis = len(shape) - 1 if shape[-1] == lon_len else shape.index(lon_len)
        except ValueError as exc:
            raise ValueError(f"Cannot align ERA5 variable dimensions: dims={dims} shape={shape}") from exc

    keep_axes = [time_axis, lat_axis, lon_axis]
    extra_axes = [axis for axis in range(data.ndim) if axis not in keep_axes]
    for axis in sorted(extra_axes, reverse=True):
        data = np.nanmean(data, axis=axis)
        keep_axes = [a - 1 if a > axis else a for a in keep_axes]

    data = np.moveaxis(data, keep_axes, [0, 1, 2])
    return data


def _grid_mask(lat_grid, lon_grid, bbox: dict[str, float], center: dict[str, float]):
    mask = (
        (lat_grid >= bbox["south"])
        & (lat_grid <= bbox["north"])
        & (lon_grid >= bbox["west"])
        & (lon_grid <= bbox["east"])
    )
    if int(mask.sum()) > 0:
        return mask, int(mask.sum())
    distance = (lat_grid - center["lat"]) ** 2 + (lon_grid - center["lon"]) ** 2
    nearest = np.unravel_index(np.nanargmin(distance), distance.shape)
    mask = np.zeros_like(lat_grid, dtype=bool)
    mask[nearest] = True
    return mask, 1


def _mean_at_time(cube, time_index: int, mask):
    if cube is None:
        return None
    values = cube[time_index]
    selected = values[mask]
    selected = selected[np.isfinite(selected)]
    if selected.size == 0:
        return None
    return float(np.nanmean(selected))


def read_era5_file(path: Path, source_file: str, start_date: date | None, end_date: date | None) -> list[dict[str, Any]]:
    require_netcdf()
    bbox = get_hanoi_bbox()
    center = get_hanoi_center()
    rows: list[dict[str, Any]] = []
    now = datetime.utcnow()

    with nc.Dataset(str(path)) as dataset:
        times = _time_values(dataset)
        lat, lon, lat_grid, lon_grid = _lat_lon(dataset)
        mask, grid_count = _grid_mask(lat_grid, lon_grid, bbox, center)

        cubes = {}
        for output_name, aliases in VAR_ALIASES.items():
            variable = _find_variable(dataset, aliases)
            if variable is None:
                cubes[output_name] = None
                print(f"warning=era5_missing_variable output={output_name} aliases={aliases}")
                continue
            cubes[output_name] = _to_time_lat_lon(variable, variable[:], len(times), len(lat), len(lon))

        for idx, hour in enumerate(times):
            hour = hour.replace(minute=0, second=0, microsecond=0)
            if start_date and hour.date() < start_date:
                continue
            if end_date and hour.date() > end_date:
                continue

            wind_u10 = _mean_at_time(cubes["wind_u10"], idx, mask)
            wind_v10 = _mean_at_time(cubes["wind_v10"], idx, mask)
            wind_speed = None
            wind_dir = None
            if wind_u10 is not None and wind_v10 is not None:
                wind_speed = float(math.sqrt(wind_u10 ** 2 + wind_v10 ** 2))
                wind_dir = float((270.0 - math.degrees(math.atan2(wind_v10, wind_u10))) % 360.0)

            pbl = _mean_at_time(cubes["pbl_height_m"], idx, mask)
            t2m = _mean_at_time(cubes["temperature_2m_c"], idx, mask)
            d2m = _mean_at_time(cubes["dewpoint_2m_c"], idx, mask)
            tp = _mean_at_time(cubes["total_precipitation_mm"], idx, mask)

            rows.append(
                {
                    "hour": hour,
                    "wind_u10": wind_u10,
                    "wind_v10": wind_v10,
                    "wind_speed": wind_speed,
                    "wind_dir": wind_dir,
                    "pbl_height_m": pbl,
                    "low_pbl": bool(pbl < 300.0) if pbl is not None else None,
                    "surface_pressure": _mean_at_time(cubes["surface_pressure"], idx, mask),
                    "temperature_2m_c": float(t2m - 273.15) if t2m is not None and t2m > 150 else t2m,
                    "dewpoint_2m_c": float(d2m - 273.15) if d2m is not None and d2m > 150 else d2m,
                    "total_precipitation_mm": float(tp * 1000.0) if tp is not None and abs(tp) < 10 else tp,
                    "mean_sea_level_pressure": _mean_at_time(cubes["mean_sea_level_pressure"], idx, mask),
                    "grid_point_count": grid_count,
                    "source_file": source_file,
                    "year": hour.year,
                    "month": hour.month,
                    "day": hour.day,
                    "spark_processed_at": now,
                }
            )
    return rows


def build_output_df(spark: SparkSession, rows: list[dict[str, Any]]):
    if rows:
        raw_df = spark.createDataFrame(rows, OUTPUT_SCHEMA)
    else:
        raw_df = spark.createDataFrame([], OUTPUT_SCHEMA)
    return (
        raw_df
        .withColumn(
            "rn",
            F.row_number().over(Window.partitionBy("hour").orderBy(F.col("source_file").desc_nulls_last())),
        )
        .filter(F.col("rn") == 1)
        .drop("rn")
        .select(*OUTPUT_COLUMNS)
    )


def log_metrics(file_count: int, rows: list[dict[str, Any]], df) -> None:
    output_count = df.count()
    duplicate_count = max(0, len(rows) - output_count)
    bounds = df.agg(F.min("hour").alias("min_time"), F.max("hour").alias("max_time")).first()
    checks = df.agg(
        F.min("wind_speed").alias("wind_speed_min"),
        F.max("wind_speed").alias("wind_speed_max"),
        F.min("wind_dir").alias("wind_dir_min"),
        F.max("wind_dir").alias("wind_dir_max"),
        F.avg(F.when(F.col("pbl_height_m").isNull(), F.lit(1.0)).otherwise(F.lit(0.0))).alias("pbl_height_null_ratio"),
    ).first().asDict() if output_count else {}
    print(f"input_count={file_count}")
    print(f"raw_hour_count={len(rows)}")
    print(f"output_count={output_count}")
    print(f"duplicate_count={duplicate_count}")
    print(f"min_time={bounds['min_time'] if bounds else None}")
    print(f"max_time={bounds['max_time'] if bounds else None}")
    print(f"era5_checks={checks}")


def write_iceberg(spark: SparkSession, df, table_name: str, full_refresh: bool) -> None:
    if full_refresh:
        spark.sql(f"DELETE FROM {table_name}")
    df.createOrReplaceTempView("era5_surface_hanoi_silver_updates")
    assignments = ", ".join([f"t.{c} = s.{c}" for c in OUTPUT_COLUMNS])
    insert_cols = ", ".join(OUTPUT_COLUMNS)
    insert_vals = ", ".join([f"s.{c}" for c in OUTPUT_COLUMNS])
    spark.sql(
        f"""
        MERGE INTO {table_name} t
        USING era5_surface_hanoi_silver_updates s
        ON t.hour = s.hour
        WHEN MATCHED THEN UPDATE SET {assignments}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
        """
    )


def main() -> None:
    args = parse_args()
    tables = get_table_names()
    start_date = parse_date(args.start_date)
    end_date = parse_date(args.end_date)

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    source_table = os.getenv("SOURCE_ICEBERG_TABLE", tables["era5_files_bronze"])
    target_table = os.getenv("ICEBERG_TABLE", tables["era5_surface_silver"])
    ensure_table(spark, target_table)

    files = collect_candidate_files(spark, source_table, start_date, end_date)
    rows: list[dict[str, Any]] = []
    for item in files:
        source_file = item["file_path"]
        local_path = copy_hdfs_to_local(source_file)
        rows.extend(read_era5_file(local_path, source_file, start_date, end_date))

    df = build_output_df(spark, rows)
    log_metrics(len(files), rows, df)
    write_iceberg(spark, df, target_table, full_refresh=as_bool(args.full_refresh))
    print(f"Saved: {target_table}")
    spark.stop()


if __name__ == "__main__":
    main()
