from __future__ import annotations

import argparse
import math
import os
import re
import statistics
from datetime import date, datetime
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from hanoi_config import (
    ICEBERG_CATALOG,
    ICEBERG_WAREHOUSE,
    get_hanoi_bbox,
    get_maiac_local_fallback_path,
    get_maiac_scale_factor,
    get_table_names,
)


OUTPUT_COLUMNS = [
    "date",
    "aod_047_mean",
    "aod_055_mean",
    "aod_mean",
    "aod_min",
    "aod_max",
    "aod_std",
    "valid_pixel_count",
    "total_pixel_count",
    "valid_pct",
    "tile_count",
    "source_files",
    "year",
    "month",
    "day",
    "spark_processed_at",
]

OUTPUT_SCHEMA = StructType(
    [
        StructField("date", DateType(), True),
        StructField("aod_047_mean", DoubleType(), True),
        StructField("aod_055_mean", DoubleType(), True),
        StructField("aod_mean", DoubleType(), True),
        StructField("aod_min", DoubleType(), True),
        StructField("aod_max", DoubleType(), True),
        StructField("aod_std", DoubleType(), True),
        StructField("valid_pixel_count", LongType(), True),
        StructField("total_pixel_count", LongType(), True),
        StructField("valid_pct", DoubleType(), True),
        StructField("tile_count", IntegerType(), True),
        StructField("source_files", ArrayType(StringType()), True),
        StructField("year", IntegerType(), True),
        StructField("month", IntegerType(), True),
        StructField("day", IntegerType(), True),
        StructField("spark_processed_at", TimestampType(), True),
    ]
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Hanoi MAIAC/MODIS AOD silver table")
    parser.add_argument("--start-date", default=os.getenv("START_DATE", ""))
    parser.add_argument("--end-date", default=os.getenv("END_DATE", ""))
    parser.add_argument("--full-refresh", default=os.getenv("FULL_REFRESH", "0"))
    parser.add_argument(
        "--local-fallback-path",
        default=os.getenv("MAIAC_LOCAL_FALLBACK_PATH", get_maiac_local_fallback_path()),
    )
    parser.add_argument("--relaxed-qa", default=os.getenv("MAIAC_RELAXED_QA", "0"))
    return parser.parse_args()


def as_bool(raw: str) -> bool:
    return str(raw or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def parse_date(raw: str) -> date | None:
    if not raw:
        return None
    return datetime.strptime(raw, "%Y-%m-%d").date()


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("MAIACHanoiSilver")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.warehouse", ICEBERG_WAREHOUSE)
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
        .getOrCreate()
    )


def ensure_table(spark: SparkSession, table_name: str) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ICEBERG_CATALOG}.satellite")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            date DATE,
            aod_047_mean DOUBLE,
            aod_055_mean DOUBLE,
            aod_mean DOUBLE,
            aod_min DOUBLE,
            aod_max DOUBLE,
            aod_std DOUBLE,
            valid_pixel_count BIGINT,
            total_pixel_count BIGINT,
            valid_pct DOUBLE,
            tile_count INT,
            source_files ARRAY<STRING>,
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


def parse_maiac_filename(path: Path) -> dict[str, Any] | None:
    # MCD19A2.AYYYYDDD.hXXvYY.061.YYYYDDDHHMMSS.hdf
    match = re.match(
        r"(?P<product>[A-Z0-9]+)\.A(?P<year>\d{4})(?P<doy>\d{3})\.(?P<tile>h\d{2}v\d{2})\.(?P<collection>\d{3})\.(?P<processing>\d+)\.hdf$",
        path.name,
    )
    if not match:
        return None
    year = int(match.group("year"))
    doy = int(match.group("doy"))
    acquisition_date = datetime.strptime(f"{year}{doy:03d}", "%Y%j").date()
    return {
        "date": acquisition_date,
        "product": match.group("product"),
        "tile": match.group("tile"),
        "collection": match.group("collection"),
        "processing_timestamp": match.group("processing"),
    }


def find_local_file(row: dict[str, Any], fallback_dir: Path) -> Path | None:
    candidates = [
        row.get("producer_granule_id"),
        row.get("granule_name"),
        row.get("granule_id"),
    ]
    for raw in candidates:
        if not raw:
            continue
        name = str(raw).split("/")[-1]
        if not name.endswith(".hdf"):
            name = f"{name}.hdf"
        path = fallback_dir / name
        if path.exists():
            return path

    tile = row.get("tile")
    acquisition_date = row.get("acquisition_date")
    if tile and acquisition_date:
        dt = acquisition_date if isinstance(acquisition_date, date) else parse_date(str(acquisition_date))
        if dt:
            prefix = f"MCD19A2.A{dt.year}{dt.timetuple().tm_yday:03d}.{tile}."
            matches = sorted(fallback_dir.glob(f"{prefix}*.hdf"))
            if matches:
                return matches[-1]
    return None


def collect_candidate_files(
    spark: SparkSession,
    source_table: str,
    fallback_path: str,
    start_date: date | None,
    end_date: date | None,
) -> tuple[list[Path], int]:
    fallback_dir = Path(fallback_path)
    rows_from_bronze = 0
    files: list[Path] = []

    try:
        bronze = spark.table(source_table)
        if start_date:
            bronze = bronze.filter(F.col("acquisition_date") >= F.lit(start_date.isoformat()))
        if end_date:
            bronze = bronze.filter(F.col("acquisition_date") <= F.lit(end_date.isoformat()))
        bronze_rows = bronze.select(
            "granule_id", "granule_name", "producer_granule_id", "tile", "acquisition_date"
        ).collect()
        rows_from_bronze = len(bronze_rows)
        for row in bronze_rows:
            path = find_local_file(row.asDict(), fallback_dir)
            if path is not None:
                files.append(path)
    except Exception as exc:
        print(f"warning=unable_to_read_maiac_bronze reason={exc}")

    if fallback_dir.exists():
        for path in fallback_dir.glob("*.hdf"):
            meta = parse_maiac_filename(path)
            if meta is None:
                continue
            if start_date and meta["date"] < start_date:
                continue
            if end_date and meta["date"] > end_date:
                continue
            files.append(path)
    else:
        print(f"warning=maiac_local_fallback_path_missing path={fallback_dir}")

    deduped = sorted({str(path.resolve()): path for path in files}.values(), key=lambda p: p.name)
    return deduped, rows_from_bronze


def _load_hdf4_sds(hdf, names: list[str]):
    datasets = hdf.datasets()
    for name in names:
        if name in datasets:
            return hdf.select(name)[:]
    raise KeyError(f"Missing SDS. Tried: {names}")


def _to_2d(values):
    import numpy as np

    arr = np.asarray(values).astype(float)
    arr[arr == -28672] = np.nan
    arr[arr < -1000] = np.nan
    arr[arr > 30000] = np.nan
    if arr.ndim == 3:
        arr = np.nanmean(arr, axis=0)
    if arr.ndim != 2:
        raise ValueError(f"Expected 2D or 3D AOD array, got shape={arr.shape}")
    return arr


def _qa_mask(qa_values, target_shape: tuple[int, int], relaxed_qa: bool):
    import numpy as np

    if qa_values is None:
        return np.ones(target_shape, dtype=bool)

    qa = np.asarray(qa_values)
    if qa.ndim == 3:
        qa = qa[0]
    if qa.shape != target_shape:
        return np.ones(target_shape, dtype=bool)

    qa_bits = qa.astype(int) & 0b11
    return qa_bits <= (2 if relaxed_qa else 1)


def _tile_lat_lon(tile: str, shape: tuple[int, int]):
    import numpy as np

    tile_h = int(tile[1:3])
    tile_v = int(tile[4:6])
    rows, cols = shape

    earth_radius = 6371007.181
    tile_size_rad = math.radians(10.0)
    tile_size_m = tile_size_rad * earth_radius
    x0 = (tile_h - 18) * tile_size_m
    y0 = (9 - tile_v) * tile_size_m
    x_res = tile_size_m / cols
    y_res = tile_size_m / rows

    col_idx = np.arange(cols)
    row_idx = np.arange(rows)[:, None]
    x = x0 + (col_idx + 0.5) * x_res
    y = y0 - (row_idx + 0.5) * y_res

    lat = np.degrees(y / earth_radius)
    lon = np.degrees(x / (earth_radius * np.cos(y / earth_radius)))
    return lat, lon


def read_maiac_hdf(path: Path, bbox: dict[str, float], relaxed_qa: bool) -> dict[str, Any] | None:
    import numpy as np

    try:
        from pyhdf.SD import SD, SDC
    except Exception as exc:
        print(f"warning=pyhdf_unavailable reason={exc}")
        return None

    meta = parse_maiac_filename(path)
    if meta is None:
        print(f"warning=maiac_filename_unparseable file={path.name}")
        return None

    hdf = SD(str(path), SDC.READ)
    try:
        aod_047 = _to_2d(_load_hdf4_sds(hdf, ["Optical_Depth_047", "AOD_047"]))
        try:
            aod_055 = _to_2d(_load_hdf4_sds(hdf, ["Optical_Depth_055", "AOD_055"]))
        except Exception:
            aod_055 = np.full(aod_047.shape, np.nan)

        try:
            qa_values = _load_hdf4_sds(hdf, ["AOD_QA", "AOD_QA_1km"])
        except Exception:
            qa_values = None
    finally:
        hdf.end()

    scale_factor = get_maiac_scale_factor()
    aod_047 = aod_047 * scale_factor
    aod_055 = aod_055 * scale_factor
    qa_good = _qa_mask(qa_values, aod_047.shape, relaxed_qa=relaxed_qa)
    lat, lon = _tile_lat_lon(meta["tile"], aod_047.shape)

    bbox_mask = (
        (lat >= bbox["south"])
        & (lat <= bbox["north"])
        & (lon >= bbox["west"])
        & (lon <= bbox["east"])
    )
    total_pixel_count = int(bbox_mask.sum())
    if total_pixel_count == 0:
        print(f"warning=maiac_no_bbox_pixels file={path.name}")
        return None

    valid_047 = bbox_mask & qa_good & np.isfinite(aod_047)
    valid_055 = bbox_mask & qa_good & np.isfinite(aod_055)
    valid_any = valid_047 | valid_055
    valid_pixel_count = int(valid_any.sum())
    if valid_pixel_count == 0:
        print(f"warning=maiac_no_valid_pixels file={path.name} relaxed_qa={relaxed_qa}")
        return None

    values_047 = aod_047[valid_047]
    values_055 = aod_055[valid_055]
    merged_values = np.concatenate([values_047, values_055])

    return {
        "date": meta["date"],
        "tile": meta["tile"],
        "aod_047_mean": float(np.nanmean(values_047)) if values_047.size else None,
        "aod_055_mean": float(np.nanmean(values_055)) if values_055.size else None,
        "aod_mean": float(np.nanmean(merged_values)),
        "aod_min": float(np.nanmin(merged_values)),
        "aod_max": float(np.nanmax(merged_values)),
        "aod_std": float(np.nanstd(merged_values)),
        "valid_pixel_count": valid_pixel_count,
        "total_pixel_count": total_pixel_count,
        "source_file": str(path),
    }


def build_daily_rows(file_records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[date, list[dict[str, Any]]] = {}
    for record in file_records:
        grouped.setdefault(record["date"], []).append(record)

    daily_rows = []
    now = datetime.utcnow()
    for dt, rows in sorted(grouped.items()):
        valid_pixel_count = int(sum(row["valid_pixel_count"] for row in rows))
        total_pixel_count = int(sum(row["total_pixel_count"] for row in rows))
        source_files = sorted(row["source_file"] for row in rows)
        tiles = {row["tile"] for row in rows}

        def mean_present(key: str):
            values = [row[key] for row in rows if row.get(key) is not None]
            return float(statistics.fmean(values)) if values else None

        daily_rows.append(
            {
                "date": dt,
                "aod_047_mean": mean_present("aod_047_mean"),
                "aod_055_mean": mean_present("aod_055_mean"),
                "aod_mean": mean_present("aod_mean"),
                "aod_min": float(min(row["aod_min"] for row in rows)),
                "aod_max": float(max(row["aod_max"] for row in rows)),
                "aod_std": mean_present("aod_std"),
                "valid_pixel_count": valid_pixel_count,
                "total_pixel_count": total_pixel_count,
                "valid_pct": float(valid_pixel_count / total_pixel_count * 100.0) if total_pixel_count else None,
                "tile_count": len(tiles),
                "source_files": source_files,
                "year": dt.year,
                "month": dt.month,
                "day": dt.day,
                "spark_processed_at": now,
            }
        )
    return daily_rows


def log_metrics(candidate_count: int, bronze_count: int, file_records: list[dict[str, Any]], daily_rows: list[dict[str, Any]]) -> None:
    print(f"bronze_candidate_count={bronze_count}")
    print(f"local_candidate_file_count={candidate_count}")
    print(f"valid_source_file_count={len(file_records)}")
    print(f"output_count={len(daily_rows)}")
    if daily_rows:
        print(f"min_time={min(row['date'] for row in daily_rows)}")
        print(f"max_time={max(row['date'] for row in daily_rows)}")
        valid_pct_by_date = {str(row["date"]): row["valid_pct"] for row in daily_rows}
        print(f"valid_pct_by_product_date={valid_pct_by_date}")
        print(f"source_file_count={len({record['source_file'] for record in file_records})}")
    else:
        print("warning=maiac_silver_empty")


def write_iceberg(spark: SparkSession, rows: list[dict[str, Any]], table_name: str, full_refresh: bool) -> None:
    if full_refresh:
        spark.sql(f"DELETE FROM {table_name}")

    df = spark.createDataFrame(rows, schema=OUTPUT_SCHEMA)
    df.createOrReplaceTempView("maiac_hanoi_daily_silver_updates")

    assignments = ", ".join([f"t.{c} = s.{c}" for c in OUTPUT_COLUMNS])
    insert_cols = ", ".join(OUTPUT_COLUMNS)
    insert_vals = ", ".join([f"s.{c}" for c in OUTPUT_COLUMNS])

    spark.sql(
        f"""
        MERGE INTO {table_name} t
        USING maiac_hanoi_daily_silver_updates s
        ON t.date = s.date
        WHEN MATCHED THEN UPDATE SET {assignments}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
        """
    )


def main() -> None:
    args = parse_args()
    tables = get_table_names()
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    target_table = os.getenv("ICEBERG_TABLE", tables["maiac_silver"])
    source_table = os.getenv("SOURCE_ICEBERG_TABLE", tables["maiac_bronze"])

    ensure_table(spark, target_table)

    start_date = parse_date(args.start_date)
    end_date = parse_date(args.end_date)
    relaxed_qa = as_bool(args.relaxed_qa)

    candidate_files, bronze_count = collect_candidate_files(
        spark,
        source_table=source_table,
        fallback_path=args.local_fallback_path,
        start_date=start_date,
        end_date=end_date,
    )

    bbox = get_hanoi_bbox()
    file_records = []
    for path in candidate_files:
        record = read_maiac_hdf(path, bbox=bbox, relaxed_qa=relaxed_qa)
        if record is not None:
            file_records.append(record)

    daily_rows = build_daily_rows(file_records)
    log_metrics(len(candidate_files), bronze_count, file_records, daily_rows)
    write_iceberg(spark, daily_rows, target_table, full_refresh=as_bool(args.full_refresh))
    print(f"Saved: {target_table}")
    spark.stop()


if __name__ == "__main__":
    main()
