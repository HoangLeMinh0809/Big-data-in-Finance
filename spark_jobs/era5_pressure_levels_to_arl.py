from __future__ import annotations

import argparse
import hashlib
import os
import shlex
import subprocess
import tempfile
from datetime import date, datetime
from pathlib import Path
from typing import Any

from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F

from hanoi_config import ICEBERG_CATALOG, ICEBERG_WAREHOUSE, get_era5_raw_base_path, get_table_names


DEFAULT_BINARY = "/opt/hysplit/exec/era5_2arl"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Convert ERA5 pressure-level GRIB files to HYSPLIT ARL files")
    parser.add_argument("--start-date", default=os.getenv("START_DATE", ""))
    parser.add_argument("--end-date", default=os.getenv("END_DATE", ""))
    parser.add_argument("--full-refresh", default=os.getenv("FULL_REFRESH", "0"))
    parser.add_argument("--source-table", default=os.getenv("SOURCE_ICEBERG_TABLE", ""))
    parser.add_argument("--target-table", default=os.getenv("ICEBERG_TABLE", ""))
    parser.add_argument("--output-base-path", default=os.getenv("ERA5_ARL_OUTPUT_BASE_PATH", ""))
    parser.add_argument("--era5-2arl-bin", default=os.getenv("HYSPLIT_ERA5_2ARL_BIN") or DEFAULT_BINARY)
    parser.add_argument("--command-template", default=os.getenv("HYSPLIT_ERA5_2ARL_TEMPLATE", ""))
    return parser.parse_args()


def as_bool(raw: str) -> bool:
    return str(raw or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def parse_date(raw: str) -> date | None:
    raw = (raw or "").strip()
    if not raw:
        return None
    return datetime.strptime(raw, "%Y-%m-%d").date()


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("ERA5PressureLevelsToARL")
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
            dataset_type STRING,
            year INT,
            month INT,
            source_nc STRING,
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            arl_path STRING,
            checksum STRING,
            created_at TIMESTAMP,
            spark_processed_at TIMESTAMP
        )
        USING ICEBERG
        PARTITIONED BY (dataset_type, year, month)
        TBLPROPERTIES ('format-version'='2')
        """
    )
    existing = set(spark.table(table_name).columns)
    for column, dtype in {"start_time": "TIMESTAMP", "end_time": "TIMESTAMP"}.items():
        if column not in existing:
            spark.sql(f"ALTER TABLE {table_name} ADD COLUMN {column} {dtype}")


def hdfs_remote_path(path: str) -> str:
    if path.startswith("hdfs://"):
        return "/" + path.split("/", 3)[3]
    return path


def _hadoop_path(spark: SparkSession, path: str):
    return spark._jvm.org.apache.hadoop.fs.Path(path)


def _hadoop_fs(spark: SparkSession, path: str):
    uri = spark._jvm.java.net.URI.create(path if path.startswith("hdfs://") else f"hdfs://namenode:9000{path}")
    return spark._jvm.org.apache.hadoop.fs.FileSystem.get(uri, spark._jsc.hadoopConfiguration())


def copy_hdfs_to_local(spark: SparkSession, path: str, local_dir: Path) -> Path:
    remote = hdfs_remote_path(path)
    local_path = local_dir / Path(remote).name
    fs = _hadoop_fs(spark, path)
    fs.copyToLocalFile(False, _hadoop_path(spark, remote), _hadoop_path(spark, str(local_path)), True)
    return local_path


def upload_local_to_hdfs(spark: SparkSession, local_path: Path, hdfs_path: str) -> None:
    remote = hdfs_remote_path(hdfs_path)
    fs = _hadoop_fs(spark, hdfs_path)
    parent = _hadoop_path(spark, str(Path(remote).parent))
    if not fs.exists(parent):
        fs.mkdirs(parent)
    target = _hadoop_path(spark, remote)
    if fs.exists(target):
        fs.delete(target, False)
    fs.copyFromLocalFile(False, True, _hadoop_path(spark, str(local_path)), target)


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def collect_candidates(
    spark: SparkSession,
    source_table: str,
    target_table: str,
    start_date: date | None,
    end_date: date | None,
    full_refresh: bool,
) -> tuple[list[dict[str, Any]], int]:
    df = spark.table(source_table).filter(F.col("dataset_type") == F.lit("pressure_levels"))
    if start_date:
        df = df.filter(F.to_date("end_time") >= F.lit(start_date.isoformat()))
    if end_date:
        df = df.filter(F.to_date("start_time") <= F.lit(end_date.isoformat()))

    rows = (
        df.select(
            "event_id",
            "dataset_type",
            "year",
            "month",
            "start_time",
            "end_time",
            "file_path",
            "surface_file_path",
            "checksum",
        )
        .dropDuplicates(["event_id"])
        .collect()
    )
    candidates = [
        row.asDict(recursive=True)
        for row in rows
        if row["file_path"] and row["surface_file_path"]
    ]
    if full_refresh or not candidates:
        return candidates, 0

    source_paths = [item["file_path"] for item in candidates]
    existing = (
        spark.table(target_table)
        .filter(F.col("source_nc").isin(source_paths))
        .select("source_nc")
        .distinct()
        .collect()
    )
    existing_paths = {row["source_nc"] for row in existing}
    return [item for item in candidates if item["file_path"] not in existing_paths], len(existing_paths)


def build_arl_path(output_base_path: str, year: int, month: int, source_nc: str) -> str:
    base = output_base_path.rstrip("/")
    source_name = Path(hdfs_remote_path(source_nc)).stem
    return f"{base}/pressure_levels/year={year}/month={month:02d}/{source_name}.arl"


def run_converter(
    binary: str,
    command_template: str,
    input_grib: Path,
    surface_grib: Path,
    output_arl: Path,
) -> None:
    converter_env = os.environ.copy()
    if not converter_env.get("ECCODES_DEFINITION_PATH") and Path("/usr/share/eccodes/definitions").exists():
        converter_env["ECCODES_DEFINITION_PATH"] = "/usr/share/eccodes/definitions"

    def finalize_output() -> bool:
        if output_arl.exists() and output_arl.stat().st_size > 0:
            return True
        candidates = [
            path
            for pattern in ("*.arl", "*.ARL", "*")
            for path in output_arl.parent.glob(pattern)
            if path.is_file() and path not in {input_grib, surface_grib} and path.stat().st_size > 0
        ]
        if not candidates:
            return False
        generated = max(candidates, key=lambda path: path.stat().st_mtime)
        generated.replace(output_arl)
        return output_arl.exists() and output_arl.stat().st_size > 0

    if command_template.strip():
        rendered = command_template.format(
            input=str(input_grib),
            surface=str(surface_grib),
            output=str(output_arl),
        )
        subprocess.check_call(shlex.split(rendered), env=converter_env)
        if not finalize_output():
            raise RuntimeError(f"Converter template completed but no ARL output was produced: {output_arl}")
        return

    candidates = [
        [binary, f"-i{input_grib}", f"-a{surface_grib}", f"-o{output_arl}"],
        [binary, "-i", str(input_grib), "-a", str(surface_grib), "-o", str(output_arl)],
        [binary, str(input_grib), str(surface_grib), str(output_arl)],
    ]
    last_error: Exception | None = None
    for command in candidates:
        try:
            subprocess.check_call(command, env=converter_env)
            if finalize_output():
                return
        except (FileNotFoundError, subprocess.CalledProcessError) as exc:
            last_error = exc

    raise RuntimeError(
        "ERA5 GRIB to ARL conversion failed. Set HYSPLIT_ERA5_2ARL_TEMPLATE if your era5_2arl binary "
        "uses a different CLI. Example: HYSPLIT_ERA5_2ARL_TEMPLATE='/opt/hysplit/exec/era5_2arl -i{input} -a{surface} -o{output}'"
    ) from last_error


def write_metadata(spark: SparkSession, rows: list[dict[str, Any]], target_table: str) -> None:
    if not rows:
        return

    df = spark.createDataFrame([Row(**row) for row in rows])
    df.createOrReplaceTempView("era5_arl_updates")
    spark.sql(
        f"""
        MERGE INTO {target_table} t
        USING era5_arl_updates s
        ON t.source_nc = s.source_nc
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )


def main() -> None:
    args = parse_args()
    tables = get_table_names()
    source_table = args.source_table or tables["era5_files_bronze"]
    target_table = args.target_table or tables["era5_arl_bronze"]
    output_base_path = args.output_base_path or f"{get_era5_raw_base_path()}/arl"
    start_date = parse_date(args.start_date)
    end_date = parse_date(args.end_date)
    full_refresh = as_bool(args.full_refresh)

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    ensure_table(spark, target_table)

    candidates, duplicate_count = collect_candidates(spark, source_table, target_table, start_date, end_date, full_refresh)
    input_count = len(candidates) + duplicate_count
    min_time = min((item["start_time"] for item in candidates if item.get("start_time")), default=None)
    max_time = max((item["end_time"] for item in candidates if item.get("end_time")), default=None)

    success_rows: list[dict[str, Any]] = []
    failure_count = 0
    created_at = datetime.utcnow()

    with tempfile.TemporaryDirectory(prefix="era5_arl_") as tmp:
        tmp_dir = Path(tmp)
        for item in candidates:
            source_nc = item["file_path"]
            surface_grib = item["surface_file_path"]
            year = int(item["year"])
            month = int(item["month"])
            arl_path = build_arl_path(output_base_path, year, month, source_nc)
            local_grib = copy_hdfs_to_local(spark, source_nc, tmp_dir)
            local_surface_grib = copy_hdfs_to_local(spark, surface_grib, tmp_dir)
            local_arl = tmp_dir / Path(hdfs_remote_path(arl_path)).name
            try:
                run_converter(
                    args.era5_2arl_bin,
                    args.command_template,
                    local_grib,
                    local_surface_grib,
                    local_arl,
                )
                checksum = file_sha256(local_arl)
                upload_local_to_hdfs(spark, local_arl, arl_path)
                success_rows.append(
                    {
                        "dataset_type": "pressure_levels",
                        "year": year,
                        "month": month,
                        "source_nc": source_nc,
                        "start_time": item.get("start_time"),
                        "end_time": item.get("end_time"),
                        "arl_path": arl_path,
                        "checksum": checksum,
                        "created_at": created_at,
                        "spark_processed_at": datetime.utcnow(),
                    }
                )
                print(f"converted source_nc={source_nc} arl_path={arl_path}")
            except Exception as exc:
                failure_count += 1
                print(f"conversion_failed source_nc={source_nc} error={exc}")

    write_metadata(spark, success_rows, target_table)
    print(
        "era5_arl_checks="
        f"{{'input_count': {input_count}, 'output_count': {len(success_rows)}, "
        f"'duplicate_count': {duplicate_count}, 'failure_count': {failure_count}, "
        f"'min_time': {repr(str(min_time) if min_time else None)}, "
        f"'max_time': {repr(str(max_time) if max_time else None)}}}"
    )
    print(f"Saved: {target_table}")
    spark.stop()

    if failure_count:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
