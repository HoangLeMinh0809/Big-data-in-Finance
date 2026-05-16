from __future__ import annotations

import argparse
import os
import shutil
import tempfile
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from typing import Any

from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F

try:
    import netCDF4 as nc  # type: ignore
    import numpy as np  # type: ignore
except Exception as exc:  # pragma: no cover
    nc = None  # type: ignore[assignment]
    np = None  # type: ignore[assignment]
    NETCDF_IMPORT_ERROR = exc
else:
    NETCDF_IMPORT_ERROR = None

from hanoi_config import (
    ICEBERG_CATALOG,
    ICEBERG_WAREHOUSE,
    TABLES,
    get_hanoi_bbox,
    get_sentinel5p_products,
    get_sentinel5p_raw_base_path,
)


PRODUCTS = {
    "NO2": {
        "variable": "nitrogendioxide_tropospheric_column",
        "qa_threshold": 0.75,
    },
    "CO": {
        "variable": "carbonmonoxide_total_column",
        "qa_threshold": 0.5,
    },
    "SO2": {
        "variable": "sulfurdioxide_total_vertical_column",
        "qa_threshold": 0.75,
    },
    "O3": {
        "variable": "ozone_total_vertical_column",
        "qa_threshold": 0.5,
    },
    "AER_AI": {
        "variable": "aerosol_index_354_388",
        "qa_threshold": 0.5,
    },
}

LOCAL_SEARCH_ROOTS = [
    Path("data/crawling/sentinel5p_downloads"),
    Path("crawler/sentinel5p_downloads"),
    Path("sentinel5p_data"),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Sentinel-5P pixel grid silver table")
    parser.add_argument("--start-date", default=os.getenv("START_DATE", ""))
    parser.add_argument("--end-date", default=os.getenv("END_DATE", ""))
    parser.add_argument("--full-refresh", default=os.getenv("FULL_REFRESH", "0"))
    return parser.parse_args()


def as_bool(raw: str) -> bool:
    return str(raw or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def parse_date(raw: str) -> date | None:
    text = (raw or "").strip()
    if not text:
        return None
    return datetime.strptime(text, "%Y-%m-%d").date()


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("Sentinel5PGridSilver")
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
            product STRING,
            date DATE,
            lat DOUBLE,
            lon DOUBLE,
            value DOUBLE,
            valid_pct DOUBLE,
            source_file STRING,
            year INT,
            month INT,
            day INT,
            spark_processed_at TIMESTAMP
        )
        USING ICEBERG
        PARTITIONED BY (product, year, month)
        TBLPROPERTIES ('format-version'='2')
        """
    )


def _require_netcdf_support() -> None:
    if nc is None or np is None:
        raise RuntimeError(
            "Sentinel-5P grid silver requires netCDF4 and numpy in the Spark Python environment"
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
        candidates = {text, basename, _sanitize_fragment(basename)}
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
        status = iterator.next()
        path_text = status.getPath().toString()
        basename = status.getPath().getName()
        index.setdefault(basename, path_text)
        index.setdefault(_sanitize_fragment(basename), path_text)
    return index


def _resolve_source_file(metadata: dict[str, Any], raw_base_path: str) -> tuple[str | None, str | None]:
    raw_file_path = str(metadata.get("raw_file_path") or "").strip()
    raw_downloaded = metadata.get("raw_downloaded")
    if raw_file_path and raw_downloaded is not False:
        return raw_file_path, raw_file_path

    candidates = _candidate_file_names(metadata)
    if not candidates:
        return None, None

    for root in LOCAL_SEARCH_ROOTS:
        if not root.exists():
            continue
        for candidate in candidates:
            direct = root / candidate
            if direct.exists():
                resolved = direct.resolve()
                return str(resolved), str(resolved)
        for candidate in candidates:
            matches = list(root.rglob(Path(candidate).name))
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


def _copy_hdfs_file_to_local(remote_path: str) -> tuple[Path, Path]:
    spark = SparkSession.builder.getOrCreate()
    jconf = spark._jsc.hadoopConfiguration()
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(jconf)
    temp_dir = Path(tempfile.mkdtemp(prefix="sentinel5p_grid_"))
    local_dst = spark._jvm.org.apache.hadoop.fs.Path(str(temp_dir.resolve()))
    src = spark._jvm.org.apache.hadoop.fs.Path(remote_path)
    fs.copyToLocalFile(False, src, local_dst, True)
    return temp_dir / Path(remote_path).name, temp_dir


def _find_product_group(dataset: Any, variable_name: str) -> Any | None:
    group = dataset.groups.get("PRODUCT")
    if group is not None and variable_name in group.variables:
        return group
    for candidate_group in dataset.groups.values():
        if variable_name in candidate_group.variables:
            return candidate_group
    return None


def _get_qa_threshold(product: str) -> float:
    default_threshold = float(PRODUCTS[product]["qa_threshold"])
    for env_name in (f"S5P_{product}_QA_THRESHOLD", "S5P_QA_THRESHOLD"):
        raw_value = os.getenv(env_name)
        if raw_value is None or not raw_value.strip():
            continue
        try:
            return max(0.0, min(1.0, float(raw_value)))
        except ValueError:
            print(f"[WARN] Invalid {env_name}={raw_value!r}; using default {default_threshold}")
            return default_threshold
    return default_threshold


def _extract_grid_rows(
    local_path: Path,
    product: str,
    obs_date: date,
    bbox: dict[str, float],
    source_file: str,
    spark_processed_at: datetime,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    _require_netcdf_support()
    config = PRODUCTS[product]
    variable_name = config["variable"]
    qa_threshold = _get_qa_threshold(product)

    with nc.Dataset(str(local_path)) as dataset:
        group = _find_product_group(dataset, variable_name)
        if group is None:
            raise ValueError(f"Variable {variable_name} not found in {local_path.name}")

        latitude = np.asarray(group.variables["latitude"][0].data, dtype=float)
        longitude = np.asarray(group.variables["longitude"][0].data, dtype=float)
        values = np.asarray(group.variables[variable_name][0].data, dtype=float)

        fill_value = getattr(group.variables[variable_name], "_FillValue", None)
        if fill_value is not None:
            values = np.where(values == fill_value, np.nan, values)
        values = np.where(values < -1e30, np.nan, values)

        bbox_mask = (
            np.isfinite(latitude)
            & np.isfinite(longitude)
            & (latitude >= bbox["south"])
            & (latitude <= bbox["north"])
            & (longitude >= bbox["west"])
            & (longitude <= bbox["east"])
        )

        total_pixel_count = int(np.count_nonzero(bbox_mask))
        if total_pixel_count == 0:
            return [], {"valid_pct": None, "total_pixel_count": 0, "valid_pixel_count": 0}

        qa_mask = np.ones_like(values, dtype=bool)
        qa_variable = group.variables.get("qa_value")
        if qa_variable is not None:
            qa_values = np.asarray(qa_variable[0].data, dtype=float)
            qa_mask = np.isfinite(qa_values) & (qa_values >= qa_threshold)

        valid_mask = bbox_mask & qa_mask & np.isfinite(values)
        valid_pixel_count = int(np.count_nonzero(valid_mask))
        if valid_pixel_count == 0:
            return [], {
                "valid_pct": 0.0,
                "total_pixel_count": total_pixel_count,
                "valid_pixel_count": 0,
            }

        valid_pct = float((valid_pixel_count / total_pixel_count) * 100.0)
        rows: list[dict[str, Any]] = []
        valid_lat = latitude[valid_mask]
        valid_lon = longitude[valid_mask]
        valid_values = values[valid_mask]
        for lat_value, lon_value, cell_value in zip(valid_lat, valid_lon, valid_values):
            rows.append(
                {
                    "product": product,
                    "date": obs_date,
                    "lat": float(lat_value),
                    "lon": float(lon_value),
                    "value": float(cell_value),
                    "valid_pct": valid_pct,
                    "source_file": source_file,
                    "year": int(obs_date.year),
                    "month": int(obs_date.month),
                    "day": int(obs_date.day),
                    "spark_processed_at": spark_processed_at,
                }
            )

        return rows, {
            "valid_pct": valid_pct,
            "total_pixel_count": total_pixel_count,
            "valid_pixel_count": valid_pixel_count,
        }


def collect_metadata_rows(spark: SparkSession, start_date: date | None, end_date: date | None) -> list[dict[str, Any]]:
    allowed_products = {_normalize_product(product) for product in get_sentinel5p_products()}
    bronze_df = spark.table(TABLES["sentinel5p_bronze"]).withColumn(
        "normalized_product",
        F.upper(F.coalesce(F.col("product"), F.col("product_name"))),
    ).withColumn(
        "event_ts",
        F.coalesce(
            F.to_timestamp(F.col("content_start")),
            F.col("event_time"),
            F.col("ingest_time"),
        ),
    ).withColumn("event_date", F.to_date(F.col("event_ts")))

    if start_date:
        bronze_df = bronze_df.filter(F.col("event_date") >= F.lit(start_date.isoformat()))
    if end_date:
        bronze_df = bronze_df.filter(F.col("event_date") <= F.lit(end_date.isoformat()))

    optional_columns = ["raw_file_path", "raw_downloaded", "download_url", "raw_download_error"]
    select_exprs = [
        F.col(column_name) if column_name in bronze_df.columns else F.lit(None).alias(column_name)
        for column_name in optional_columns
    ]

    rows = (
        bronze_df
        .filter(F.col("normalized_product").isin(sorted(allowed_products | {"AER"})))
        .select(
            "product",
            "product_name",
            "product_id",
            "file_name",
            "content_start",
            "event_time",
            "ingest_time",
            *select_exprs,
        )
        .collect()
    )
    return [row.asDict(recursive=True) for row in rows]


def build_output_rows(
    spark: SparkSession,
    start_date: date | None,
    end_date: date | None,
) -> tuple[list[dict[str, Any]], int, dict[tuple[str, date], dict[str, Any]], date | None, date | None, int, int]:
    metadata_rows = collect_metadata_rows(spark, start_date, end_date)
    grouped_rows = _group_metadata_rows(metadata_rows)
    if not grouped_rows:
        return [], 0, {}, None, None, 0, 0

    unique_metadata_keys = {
        (
            _normalize_product(row.get("product") or row.get("product_name")),
            (_parse_iso_timestamp(row.get("content_start")) or _parse_iso_timestamp(row.get("event_time")) or _parse_iso_timestamp(row.get("ingest_time"))),
            str(row.get("raw_file_path") or row.get("file_name") or row.get("product_id") or row.get("product_name") or ""),
        )
        for row in metadata_rows
    }
    input_count = len(metadata_rows)
    duplicate_count = max(0, input_count - len(unique_metadata_keys))

    bbox = get_hanoi_bbox()
    raw_base_path = get_sentinel5p_raw_base_path()
    spark_processed_at = datetime.utcnow()
    output_rows: list[dict[str, Any]] = []
    metrics_by_group: dict[tuple[str, date], dict[str, Any]] = {}
    failure_count = 0

    for (product, day_value), rows in sorted(grouped_rows.items(), key=lambda item: (item[0][0], item[0][1])):
        group_output_count = 0
        group_valid_pct_values: list[float] = []
        for row in rows:
            resolved_path, provenance_path = _resolve_source_file(row, raw_base_path)
            if resolved_path is None:
                failure_count += 1
                print(
                    f"[WARN] Missing raw Sentinel-5P file product={product} date={day_value} "
                    f"file_name={row.get('file_name')!r}"
                )
                continue

            local_path = Path(resolved_path)
            cleanup_dir: Path | None = None
            if not local_path.exists():
                local_path, cleanup_dir = _copy_hdfs_file_to_local(resolved_path)

            try:
                rows_payload, stats = _extract_grid_rows(
                    local_path=local_path,
                    product=product,
                    obs_date=day_value,
                    bbox=bbox,
                    source_file=str(provenance_path or resolved_path),
                    spark_processed_at=spark_processed_at,
                )
            except Exception as exc:
                failure_count += 1
                print(f"[WARN] Failed to parse Sentinel-5P grid product={product} date={day_value} error={exc}")
                rows_payload = []
                stats = {"valid_pct": None, "total_pixel_count": 0, "valid_pixel_count": 0}
            finally:
                if cleanup_dir is not None:
                    shutil.rmtree(cleanup_dir, ignore_errors=True)

            output_rows.extend(rows_payload)
            group_output_count += len(rows_payload)
            if stats["valid_pct"] is not None:
                group_valid_pct_values.append(float(stats["valid_pct"]))

        metrics_by_group[(product, day_value)] = {
            "output_count": group_output_count,
            "avg_valid_pct": (sum(group_valid_pct_values) / len(group_valid_pct_values)) if group_valid_pct_values else None,
        }

    min_time = min((group_day for (_, group_day) in grouped_rows.keys()), default=None)
    max_time = max((group_day for (_, group_day) in grouped_rows.keys()), default=None)
    return output_rows, failure_count, metrics_by_group, min_time, max_time, input_count, duplicate_count


def write_output(spark: SparkSession, rows: list[dict[str, Any]], target_table: str, full_refresh: bool) -> None:
    if full_refresh:
        spark.sql(f"DELETE FROM {target_table}")
    if not rows:
        return

    df = spark.createDataFrame([Row(**row) for row in rows])
    df.createOrReplaceTempView("s5p_grid_updates")
    spark.sql(
        f"""
        MERGE INTO {target_table} t
        USING s5p_grid_updates s
        ON t.product = s.product
           AND t.date = s.date
           AND t.lat = s.lat
           AND t.lon = s.lon
           AND t.source_file = s.source_file
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )


def main() -> None:
    args = parse_args()
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    target_table = os.getenv("ICEBERG_TABLE", TABLES["s5p_grid_silver"])
    full_refresh = as_bool(args.full_refresh)

    ensure_table(spark, target_table)
    start_date = parse_date(args.start_date)
    end_date = parse_date(args.end_date)
    rows, failure_count, metrics_by_group, min_time, max_time, input_count, duplicate_count = build_output_rows(
        spark,
        start_date,
        end_date,
    )

    output_count = len(rows)

    for (product, day_value), metrics in sorted(metrics_by_group.items(), key=lambda item: (item[0][0], item[0][1])):
        print(
            f"product={product} date={day_value} "
            f"valid_pct={metrics['avg_valid_pct']} output_count={metrics['output_count']}"
        )

    print(f"input_count={input_count}")
    print(f"output_count={output_count}")
    print(f"duplicate_count={duplicate_count}")
    print(f"failure_count={failure_count}")
    print(f"min_time={min_time}")
    print(f"max_time={max_time}")

    write_output(spark, rows, target_table, full_refresh=full_refresh)
    print(f"Saved: {target_table}")
    spark.stop()

    if failure_count:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
