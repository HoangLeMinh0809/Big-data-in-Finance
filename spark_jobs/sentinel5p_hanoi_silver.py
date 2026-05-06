"""
Hanoi Sentinel-5P Silver Layer Job.

Purpose:
- Resolve Sentinel-5P NetCDF granules from bronze metadata
- Read raw NetCDF pixel grids
- Apply product-specific QA and Hanoi bbox clipping
- Aggregate daily satellite features for downstream gold joins

Input:
- ais.satellite.sentinel5p_summary_bronze
- raw Sentinel-5P NetCDF files from HDFS or local fallback directories

Output:
- ais.satellite.sentinel5p_hanoi_daily_silver

Usage:
    spark-submit sentinel5p_hanoi_silver.py \
        --start-date 2026-01-01 \
        --end-date 2026-03-31 \
        --full-refresh 0
"""

from __future__ import annotations

import argparse
import subprocess
import tempfile
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, coalesce, to_date, to_timestamp, upper
from pyspark.sql.types import DateType, DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType

try:
    import netCDF4 as nc  # type: ignore
    import numpy as np  # type: ignore
except Exception as exc:  # pragma: no cover - handled at runtime with a clear message
    nc = None  # type: ignore[assignment]
    np = None  # type: ignore[assignment]
    NETCDF_IMPORT_ERROR = exc
else:
    NETCDF_IMPORT_ERROR = None

from hanoi_config import TABLES, get_hanoi_bbox, get_sentinel5p_products, get_sentinel5p_raw_base_path


PRODUCTS = {
    "NO2": {
        "variable": "nitrogendioxide_tropospheric_column",
        "qa_threshold": 0.75,
        "unit": "mol/m²",
    },
    "CO": {
        "variable": "carbonmonoxide_total_column",
        "qa_threshold": 0.5,
        "unit": "mol/m²",
    },
    "SO2": {
        "variable": "sulfurdioxide_total_vertical_column",
        "qa_threshold": 0.75,
        "unit": "mol/m²",
    },
    "O3": {
        "variable": "ozone_total_vertical_column",
        "qa_threshold": 0.5,
        "unit": "mol/m²",
    },
    "AER_AI": {
        "variable": "aerosol_index_354_388",
        "qa_threshold": 0.5,
        "unit": "unitless",
    },
}

LOCAL_SEARCH_ROOTS = [
    Path("data/crawling/sentinel5p_downloads"),
    Path("crawler/sentinel5p_downloads"),
    Path("sentinel5p_data"),
]

OUTPUT_SCHEMA = StructType(
    [
        StructField("product", StringType(), False),
        StructField("date", DateType(), False),
        StructField("overpass_time_utc", TimestampType(), True),
        StructField("value_mean", DoubleType(), True),
        StructField("value_min", DoubleType(), True),
        StructField("value_max", DoubleType(), True),
        StructField("value_std", DoubleType(), True),
        StructField("valid_pixel_count", LongType(), False),
        StructField("total_pixel_count", LongType(), False),
        StructField("valid_pct", DoubleType(), True),
        StructField("unit", StringType(), True),
        StructField("source_file", StringType(), True),
        StructField("year", IntegerType(), False),
        StructField("month", IntegerType(), False),
        StructField("day", IntegerType(), False),
        StructField("spark_processed_at", TimestampType(), False),
    ]
)


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("hanoi-sentinel5p-silver")
        .config("spark.sql.catalog.ais", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.ais.type", "hadoop")
        .config("spark.sql.catalog.ais.warehouse", "hdfs://namenode:9000/warehouse/iceberg")
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .getOrCreate()
    )


def _require_netcdf_support() -> None:
    if nc is None or np is None:
        raise RuntimeError(
            "Sentinel-5P silver requires netCDF4 and numpy in the Spark Python environment"
        ) from NETCDF_IMPORT_ERROR


def _parse_iso_timestamp(raw_value: Any) -> datetime | None:
    if raw_value is None:
        return None
    if isinstance(raw_value, datetime):
        return raw_value.replace(tzinfo=None)
    text = str(raw_value).strip()
    if not text:
        return None
    text = text.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(text).replace(tzinfo=None)
    except ValueError:
        return None


def _normalize_product(product_value: Any) -> str:
    text = str(product_value or "").strip().upper()
    if text == "AER":
        return "AER_AI"
    return text


def _sanitize_fragment(value: str) -> str:
    return "".join(char if char.isalnum() or char in {"-", "_", "."} else "_" for char in value)


def _candidate_file_names(metadata: dict[str, Any]) -> list[str]:
    names: list[str] = []
    for field_name in ("file_name", "product_name", "product_id"):
        raw_value = metadata.get(field_name)
        if not raw_value:
            continue

        text = str(raw_value).strip()
        if not text:
            continue

        basename = Path(text).name
        candidates = {
            text,
            basename,
            _sanitize_fragment(basename),
        }

        for candidate in list(candidates):
            if not candidate.lower().endswith(".nc"):
                candidates.add(f"{candidate}.nc")

        names.extend(sorted(candidates, key=len))

    deduped: list[str] = []
    seen = set()
    for candidate in names:
        if candidate in seen:
            continue
        seen.add(candidate)
        deduped.append(candidate)
    return deduped


def _group_metadata_rows(rows: list[dict[str, Any]]) -> dict[tuple[str, date], list[dict[str, Any]]]:
    grouped: dict[tuple[str, date], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        product = _normalize_product(row.get("product") or row.get("product_name"))
        if product not in PRODUCTS:
            continue

        event_ts = _parse_iso_timestamp(row.get("content_start"))
        if event_ts is None:
            event_ts = _parse_iso_timestamp(row.get("event_time"))
        if event_ts is None:
            event_ts = _parse_iso_timestamp(row.get("ingest_time"))
        if event_ts is None:
            continue

        grouped[(product, event_ts.date())].append({**row, "_event_ts": event_ts})

    return grouped


def _list_hdfs_files(root_path: str) -> dict[str, str]:
    """
    List files under an HDFS path using the Hadoop FileSystem API via the Spark JVM.
    Returns a mapping of basename -> full HDFS path.
    """
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    jconf = spark._jsc.hadoopConfiguration()
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(jconf)
    path = spark._jvm.org.apache.hadoop.fs.Path(root_path)

    try:
        if not fs.exists(path):
            return {}
    except Exception:
        return {}

    index: dict[str, str] = {}
    iterator = fs.listFiles(path, True)
    while iterator.hasNext():
        file_status = iterator.next()
        path_text = file_status.getPath().toString()
        basename = file_status.getPath().getName()
        index.setdefault(basename, path_text)
        index.setdefault(_sanitize_fragment(basename), path_text)

    return index


def _resolve_source_file(metadata: dict[str, Any], raw_base_path: str) -> tuple[str | None, str | None]:
    candidates = _candidate_file_names(metadata)
    if not candidates:
        return None, None

    for root in LOCAL_SEARCH_ROOTS:
        if not root.exists():
            continue
        for candidate in candidates:
            direct = root / candidate
            if direct.exists():
                return str(direct.resolve()), str(direct.resolve())
        for candidate in candidates:
            basename = Path(candidate).name
            matches = list(root.rglob(basename))
            if matches:
                resolved = matches[0].resolve()
                return str(resolved), str(resolved)

    if raw_base_path.startswith("hdfs://") or raw_base_path.startswith("/"):
        hdfs_index = _list_hdfs_files(raw_base_path)
        for candidate in candidates:
            if candidate in hdfs_index:
                return hdfs_index[candidate], hdfs_index[candidate]
            basename = Path(candidate).name
            if basename in hdfs_index:
                return hdfs_index[basename], hdfs_index[basename]

    return None, None


def _copy_hdfs_file_to_local(remote_path: str) -> Path:
    """
    Copy an HDFS file to a local temporary directory using the Hadoop FileSystem API.
    """
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    jconf = spark._jsc.hadoopConfiguration()
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(jconf)

    temp_dir = Path(tempfile.mkdtemp(prefix="sentinel5p_"))
    local_dst = spark._jvm.org.apache.hadoop.fs.Path(str(temp_dir.resolve()))
    src = spark._jvm.org.apache.hadoop.fs.Path(remote_path)

    try:
        # preserve source, copy to local dir
        fs.copyToLocalFile(False, src, local_dst, True)
    except Exception as exc:
        raise RuntimeError(f"Failed to copy {remote_path}: {exc}") from exc

    return temp_dir / Path(remote_path).name


def _find_product_group(dataset: Any, variable_name: str) -> Any | None:
    group = dataset.groups.get("PRODUCT")
    if group is not None and variable_name in group.variables:
        return group

    for candidate_group in dataset.groups.values():
        if variable_name in candidate_group.variables:
            return candidate_group

    return None


def _read_granule_stats(local_path: Path, product: str, bbox: dict[str, float], event_ts: datetime) -> dict[str, Any] | None:
    _require_netcdf_support()

    config = PRODUCTS[product]
    variable_name = config["variable"]
    qa_threshold = config["qa_threshold"]

    with nc.Dataset(str(local_path)) as dataset:
        group = _find_product_group(dataset, variable_name)
        if group is None:
            print(f"[WARN] Variable {variable_name} not found in {local_path.name}")
            return None

        latitude = np.asarray(group.variables["latitude"][0].data, dtype=float)
        longitude = np.asarray(group.variables["longitude"][0].data, dtype=float)
        values = np.asarray(group.variables[variable_name][0].data, dtype=float)

        fill_value = getattr(group.variables[variable_name], "_FillValue", None)
        if fill_value is not None:
            values = np.where(values == fill_value, np.nan, values)
        values = np.where(values < -1e30, np.nan, values)

        base_mask = (
            np.isfinite(latitude)
            & np.isfinite(longitude)
            & np.isfinite(values)
            & (latitude >= bbox["south"])
            & (latitude <= bbox["north"])
            & (longitude >= bbox["west"])
            & (longitude <= bbox["east"])
        )

        total_pixel_count = int(np.count_nonzero(base_mask))
        if total_pixel_count == 0:
            return {
                "valid_values": np.asarray([], dtype=float),
                "valid_pixel_count": 0,
                "total_pixel_count": 0,
                "overpass_time_utc": event_ts,
            }

        qa_mask = np.ones_like(values, dtype=bool)
        qa_variable = group.variables.get("qa_value")
        if qa_variable is not None:
            qa_values = np.asarray(qa_variable[0].data, dtype=float)
            qa_mask = np.isfinite(qa_values) & (qa_values >= qa_threshold)

        valid_mask = base_mask & qa_mask
        valid_values = values[valid_mask]

        return {
            "valid_values": valid_values,
            "valid_pixel_count": int(valid_values.size),
            "total_pixel_count": total_pixel_count,
            "overpass_time_utc": event_ts,
        }


def _build_output_row(
    product: str,
    day_value: date,
    stats_payload: list[dict[str, Any]],
    source_files: list[str],
    spark_processed_at: datetime,
) -> dict[str, Any] | None:
    if not stats_payload:
        return None

    values = [payload["valid_values"] for payload in stats_payload if payload["valid_values"].size > 0]
    valid_pixel_count = sum(payload["valid_pixel_count"] for payload in stats_payload)
    total_pixel_count = sum(payload["total_pixel_count"] for payload in stats_payload)

    if not values or valid_pixel_count == 0:
        return None

    all_values = np.concatenate(values)
    overpass_times = [payload["overpass_time_utc"] for payload in stats_payload if payload["overpass_time_utc"] is not None]
    overpass_time = min(overpass_times) if overpass_times else None

    source_file = ";".join(dict.fromkeys(source_files)) if source_files else None
    year_value = day_value.year
    month_value = day_value.month
    day_number = day_value.day

    return {
        "product": product,
        "date": day_value,
        "overpass_time_utc": overpass_time,
        "value_mean": float(np.nanmean(all_values)),
        "value_min": float(np.nanmin(all_values)),
        "value_max": float(np.nanmax(all_values)),
        "value_std": float(np.nanstd(all_values, ddof=0)),
        "valid_pixel_count": int(valid_pixel_count),
        "total_pixel_count": int(total_pixel_count),
        "valid_pct": float((valid_pixel_count / total_pixel_count) * 100.0) if total_pixel_count else None,
        "unit": PRODUCTS[product]["unit"],
        "source_file": source_file,
        "year": year_value,
        "month": month_value,
        "day": day_number,
        "spark_processed_at": spark_processed_at,
    }


def build_daily_rows(spark: SparkSession, start_date: date, end_date: date) -> list[dict[str, Any]]:
    allowed_products = {product.upper() for product in get_sentinel5p_products()}
    allowed_products.add("AER")
    allowed_products.add("AER_AI")

    bronze_df = spark.table(TABLES["sentinel5p_bronze"])
    bronze_df = bronze_df.withColumn(
        "normalized_product",
        upper(coalesce(col("product"), col("product_name")))
    ).withColumn(
        "event_ts",
        coalesce(
            to_timestamp(col("content_start")),
            col("event_time"),
            col("ingest_time"),
        ),
    ).withColumn("event_date", to_date(col("event_ts")))

    filtered_rows = (
        bronze_df
        .filter(col("normalized_product").isin(sorted(allowed_products)))
        .filter(col("event_date").between(start_date, end_date))
        .select(
            "product",
            "product_name",
            "product_id",
            "file_name",
            "content_start",
            "event_time",
            "ingest_time",
            "event_ts",
            "event_date",
        )
        .collect()
    )

    metadata_rows = [row.asDict(recursive=True) for row in filtered_rows]

    grouped_rows = _group_metadata_rows(metadata_rows)
    if not grouped_rows:
        print("[WARN] No Sentinel-5P bronze metadata found for the requested window")
        return []

    bbox = get_hanoi_bbox()
    raw_base_path = get_sentinel5p_raw_base_path()
    spark_processed_at = datetime.utcnow()
    output_rows: list[dict[str, Any]] = []

    for (product, day_value), rows in sorted(grouped_rows.items(), key=lambda item: (item[0][0], item[0][1])):
        print(f"[INFO] Processing Sentinel-5P {product} for {day_value} ({len(rows)} metadata row(s))")
        stats_payload: list[dict[str, Any]] = []
        source_files: list[str] = []

        for row in rows:
            resolved_path, provenance_path = _resolve_source_file(row, raw_base_path)
            if resolved_path is None:
                print(
                    f"[WARN] No raw file found for product={product} date={day_value} "
                    f"(file_name={row.get('file_name')!r}, product_name={row.get('product_name')!r})"
                )
                continue

            source_files.append(provenance_path or resolved_path)
            event_ts = row.get("_event_ts") or _parse_iso_timestamp(row.get("content_start"))
            if event_ts is None:
                event_ts = datetime(day_value.year, day_value.month, day_value.day)

            local_path = Path(resolved_path)
            if not local_path.exists():
                local_path = _copy_hdfs_file_to_local(resolved_path)

            file_stats = _read_granule_stats(local_path, product, bbox, event_ts)
            if file_stats is None:
                continue

            stats_payload.append(file_stats)

        row = _build_output_row(product, day_value, stats_payload, source_files, spark_processed_at)
        if row is None:
            print(f"[WARN] No valid Sentinel-5P pixels for product={product} date={day_value}")
            continue

        output_rows.append(row)

    return output_rows


def create_output_dataframe(spark: SparkSession, rows: list[dict[str, Any]]):
    if rows:
        return spark.createDataFrame(rows, schema=OUTPUT_SCHEMA)
    return spark.createDataFrame([], schema=OUTPUT_SCHEMA)


def write_to_iceberg(df, table_name: str) -> None:
    print(f"[INFO] Writing {df.count()} Sentinel-5P row(s) to {table_name}")
    df.writeTo(table_name).overwritePartitions()


def validate_output(df) -> None:
    total_rows = df.count()
    print(f"[INFO] Output rows: {total_rows}")
    if total_rows == 0:
        return

    df.select(
        "product",
        "date",
        "valid_pixel_count",
        "total_pixel_count",
        "valid_pct",
        "value_mean",
    ).show(truncate=False)

    df.groupBy("product").count().show(truncate=False)

    summary = df.agg(
        {"valid_pct": "avg"},
    ).collect()[0][0]
    print(f"[INFO] Average valid_pct: {summary}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Hanoi Sentinel-5P Silver Layer Job")
    parser.add_argument("--start-date", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--full-refresh", type=int, default=0, help="Full refresh (1) or incremental (0)")
    args = parser.parse_args()

    start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
    end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()
    if start_date > end_date:
        raise ValueError("start-date must be earlier than or equal to end-date")

    print("Starting Hanoi Sentinel-5P Silver job")
    print(f"Date range: {start_date} to {end_date}")
    print(f"Full refresh: {bool(args.full_refresh)}")

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    rows = build_daily_rows(spark, start_date, end_date)
    output_df = create_output_dataframe(spark, rows)
    validate_output(output_df)
    write_to_iceberg(output_df, TABLES["sentinel5p_silver"])

    print(f"Successfully wrote Sentinel-5P daily silver to {TABLES['sentinel5p_silver']}")
    spark.stop()


if __name__ == "__main__":
    main()