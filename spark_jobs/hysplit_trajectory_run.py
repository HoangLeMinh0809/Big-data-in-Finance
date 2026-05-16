"""Run HYSPLIT trajectories from ERA5 ARL metadata and write run metadata.

The job reads monthly ARL files from Iceberg, creates HYSPLIT CONTROL files
for configured Hanoi start points, runs ``hyts_std`` locally in the Spark
driver container, uploads tdump outputs to HDFS, and upserts run metadata.
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import tempfile
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any

from pyspark.sql import Row, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from hanoi_config import (
    ICEBERG_CATALOG,
    ICEBERG_WAREHOUSE,
    get_hanoi_center,
    get_hysplit_config,
    get_table_names,
)


DEFAULT_HYSPLIT_BIN = "/opt/hysplit/exec/hyts_std"
DEFAULT_OUTPUT_BASE = "hdfs://namenode:9000/raw/hysplit/trajectories"
DEFAULT_HYSPLIT_WORK_DIR = "/opt/hysplit/working"
RUN_METADATA_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("direction", StringType(), True),
        StructField("init_time", TimestampType(), True),
        StructField("duration_hours", IntegerType(), True),
        StructField("init_lat", DoubleType(), True),
        StructField("init_lon", DoubleType(), True),
        StructField("init_alt_m", DoubleType(), True),
        StructField("arl_path", StringType(), True),
        StructField("output_path", StringType(), True),
        StructField("status", StringType(), True),
        StructField("error_message", StringType(), True),
        StructField("spark_processed_at", TimestampType(), True),
    ]
)


@dataclass(frozen=True)
class ArlFile:
    year: int
    month: int
    arl_path: str
    start_time: datetime | None
    end_time: datetime | None


@dataclass(frozen=True)
class PlannedRun:
    run_id: str
    direction: str
    init_time: datetime
    duration_hours: int
    init_lat: float
    init_lon: float
    init_alt_m: float
    arl_path: str
    output_path: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run HYSPLIT trajectories from ARL files")
    parser.add_argument("--start-date", default=os.getenv("START_DATE", ""))
    parser.add_argument("--end-date", default=os.getenv("END_DATE", ""))
    parser.add_argument("--direction", choices=("backward", "forward", "both"), default=os.getenv("DIRECTION", "both"))
    parser.add_argument("--full-refresh", nargs="?", const="1", default=os.getenv("FULL_REFRESH", "0"))
    parser.add_argument("--source-table", default=os.getenv("ERA5_ARL_TABLE", ""))
    parser.add_argument("--openaq-table", default=os.getenv("OPENAQ_TABLE", ""))
    parser.add_argument("--target-table", default=os.getenv("HYSPLIT_RUNS_TABLE", ""))
    parser.add_argument("--output-base-path", default=os.getenv("HYSPLIT_OUTPUT_BASE_PATH", DEFAULT_OUTPUT_BASE))
    parser.add_argument("--hysplit-bin", default=os.getenv("HYSPLIT_BIN", DEFAULT_HYSPLIT_BIN))
    return parser.parse_args()


def as_bool(raw: str) -> bool:
    return str(raw or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def parse_date(raw: str) -> date | None:
    raw = (raw or "").strip()
    if not raw:
        return None
    return datetime.strptime(raw, "%Y-%m-%d").date()


def utc_dt(d: date, hour: int) -> datetime:
    return datetime.combine(d, time(hour=hour), tzinfo=timezone.utc).replace(tzinfo=None)


def date_range(start: date, end: date) -> list[date]:
    days: list[date] = []
    cur = start
    while cur <= end:
        days.append(cur)
        cur += timedelta(days=1)
    return days


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("HYSPLITTrajectoryRun")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.warehouse", ICEBERG_WAREHOUSE)
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
        .getOrCreate()
    )


def ensure_table(spark: SparkSession, table_name: str) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ICEBERG_CATALOG}.trajectory")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            run_id STRING,
            direction STRING,
            init_time TIMESTAMP,
            duration_hours INT,
            init_lat DOUBLE,
            init_lon DOUBLE,
            init_alt_m DOUBLE,
            arl_path STRING,
            output_path STRING,
            status STRING,
            error_message STRING,
            spark_processed_at TIMESTAMP
        )
        USING ICEBERG
        PARTITIONED BY (direction)
        TBLPROPERTIES ('format-version'='2')
        """
    )


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


def collect_arl_files(
    spark: SparkSession,
    table_name: str,
    source_metadata_table: str,
    start_date: date | None,
    end_date: date | None,
) -> dict[tuple[int, int], ArlFile]:
    arl_cols = set(spark.table(table_name).columns)
    df = spark.table(table_name).filter(F.col("dataset_type") == F.lit("pressure_levels")).alias("a")
    if start_date:
        df = df.filter(
            (F.col("a.year") > F.lit(start_date.year))
            | ((F.col("a.year") == F.lit(start_date.year)) & (F.col("a.month") >= F.lit(start_date.month)))
        )
    if end_date:
        df = df.filter(
            (F.col("a.year") < F.lit(end_date.year))
            | ((F.col("a.year") == F.lit(end_date.year)) & (F.col("a.month") <= F.lit(end_date.month)))
        )

    start_expr = F.col("a.start_time") if "start_time" in arl_cols else F.lit(None).cast("timestamp")
    end_expr = F.col("a.end_time") if "end_time" in arl_cols else F.lit(None).cast("timestamp")
    try:
        source_df = (
            spark.table(source_metadata_table)
            .filter(F.col("dataset_type") == F.lit("pressure_levels"))
            .select(
                F.col("file_path").alias("source_file_path"),
                F.col("start_time").alias("source_start_time"),
                F.col("end_time").alias("source_end_time"),
            )
            .alias("s")
        )
        df = df.join(source_df, F.col("a.source_nc") == F.col("s.source_file_path"), "left")
        start_expr = F.coalesce(start_expr, F.col("s.source_start_time"))
        end_expr = F.coalesce(end_expr, F.col("s.source_end_time"))
    except Exception as exc:
        print(f"[WARN] Cannot read ERA5 source metadata table {source_metadata_table}: {exc}")

    rows = (
        df.select(
            F.col("a.year").alias("year"),
            F.col("a.month").alias("month"),
            F.col("a.arl_path").alias("arl_path"),
            start_expr.alias("start_time"),
            end_expr.alias("end_time"),
        )
        .where(F.col("arl_path").isNotNull())
        .collect()
    )
    return {
        (int(row["year"]), int(row["month"])): ArlFile(
            int(row["year"]),
            int(row["month"]),
            row["arl_path"],
            row["start_time"].replace(tzinfo=None) if row["start_time"] else None,
            row["end_time"].replace(tzinfo=None) if row["end_time"] else None,
        )
        for row in rows
    }


def openaq_pm25_column(df) -> str | None:
    for candidate in ("pm25_median", "pm25_mean", "pm25"):
        if candidate in df.columns:
            return candidate
    return None


def collect_backward_hours(
    spark: SparkSession,
    table_name: str,
    start_date: date,
    end_date: date,
    threshold: float,
) -> list[datetime]:
    try:
        df = spark.table(table_name)
    except Exception as exc:
        print(f"[WARN] Cannot read OpenAQ table {table_name}: {exc}")
        return []

    pm25_col = openaq_pm25_column(df)
    if not pm25_col or "hour" not in df.columns:
        print(f"[WARN] OpenAQ table {table_name} has no usable PM2.5 hour columns")
        return []

    rows = (
        df.filter(F.col("hour") >= F.to_timestamp(F.lit(f"{start_date.isoformat()} 00:00:00")))
        .filter(F.col("hour") <= F.to_timestamp(F.lit(f"{end_date.isoformat()} 23:59:59")))
        .filter(F.col(pm25_col) >= F.lit(float(threshold)))
        .select("hour")
        .distinct()
        .orderBy("hour")
        .collect()
    )
    return [row["hour"].replace(tzinfo=None) for row in rows if row["hour"] is not None]


def default_window_from_arl(arl_files: dict[tuple[int, int], ArlFile]) -> tuple[date, date] | tuple[None, None]:
    if not arl_files:
        return None, None
    first_year, first_month = min(arl_files)
    last_year, last_month = max(arl_files)
    start = date(first_year, first_month, 1)
    if last_month == 12:
        end = date(last_year + 1, 1, 1) - timedelta(days=1)
    else:
        end = date(last_year, last_month + 1, 1) - timedelta(days=1)
    return start, end


def safe_id(value: Any) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", str(value))


def build_output_path(base_path: str, run_id: str, direction: str, init_time: datetime) -> str:
    base = base_path.rstrip("/")
    return (
        f"{base}/direction={direction}/year={init_time.year}/month={init_time.month:02d}/"
        f"day={init_time.day:02d}/{safe_id(run_id)}.tdump"
    )


def meteo_coverage_fits(arl: ArlFile, init_time: datetime, duration_hours: int, meteo_interval_hours: int) -> bool:
    if arl.start_time is None or arl.end_time is None:
        return True
    end_time = init_time + timedelta(hours=duration_hours)
    required_start = min(init_time, end_time)
    required_end = max(init_time, end_time)
    interval = timedelta(hours=max(0, meteo_interval_hours))
    if duration_hours >= 0:
        required_end += interval
    else:
        required_start -= interval
    return required_start >= arl.start_time and required_end <= arl.end_time


def build_planned_runs(
    *,
    arl_files: dict[tuple[int, int], ArlFile],
    start_date: date,
    end_date: date,
    backward_hours: list[datetime],
    directions: list[str],
    output_base_path: str,
) -> list[PlannedRun]:
    cfg = get_hysplit_config()
    center = get_hanoi_center()
    offsets = cfg.get("init_offsets_deg", {})
    lat_offsets = [float(v) for v in offsets.get("lat", [0.0])]
    lon_offsets = [float(v) for v in offsets.get("lon", [0.0])]
    run_hours = [int(v) for v in cfg.get("run_hours_utc", [0, 6, 12, 18])]
    meteo_interval_hours = int(cfg.get("meteo_interval_hours", 6))
    back_alts = [float(v) for v in cfg.get("backward_altitudes_m", [100, 500, 1000])]
    fwd_alts = [float(v) for v in cfg.get("forward_altitudes_m", [50, 200, 500])]
    back_duration = int(cfg.get("backward_hours", 72))
    fwd_duration = int(cfg.get("forward_hours", 24))

    runs: list[PlannedRun] = []

    def add_run(direction: str, init_time: datetime, lat: float, lon: float, alt: float, duration: int) -> None:
        arl = arl_files.get((init_time.year, init_time.month))
        if arl is None:
            return
        if not meteo_coverage_fits(arl, init_time, duration, meteo_interval_hours):
            return
        run_id = (
            f"{direction}_{init_time:%Y%m%d%H}_"
            f"lat{lat:.3f}_lon{lon:.3f}_alt{int(round(alt))}"
        )
        runs.append(
            PlannedRun(
                run_id=safe_id(run_id),
                direction=direction,
                init_time=init_time,
                duration_hours=duration,
                init_lat=lat,
                init_lon=lon,
                init_alt_m=alt,
                arl_path=arl.arl_path,
                output_path=build_output_path(output_base_path, run_id, direction, init_time),
            )
        )

    if "backward" in directions:
        for init_time in backward_hours:
            for lat_off in lat_offsets:
                for lon_off in lon_offsets:
                    for alt in back_alts:
                        add_run(
                            "backward",
                            init_time,
                            float(center["lat"]) + lat_off,
                            float(center["lon"]) + lon_off,
                            alt,
                            -abs(back_duration),
                        )

    if "forward" in directions:
        for d in date_range(start_date, end_date):
            for hour in run_hours:
                init_time = utc_dt(d, hour)
                for lat_off in lat_offsets:
                    for lon_off in lon_offsets:
                        for alt in fwd_alts:
                            add_run(
                                "forward",
                                init_time,
                                float(center["lat"]) + lat_off,
                                float(center["lon"]) + lon_off,
                                alt,
                                abs(fwd_duration),
                            )

    return runs


def existing_run_ids(spark: SparkSession, table_name: str, planned_ids: list[str]) -> set[str]:
    if not planned_ids:
        return set()
    return {
        row["run_id"]
        for row in spark.table(table_name).filter(F.col("run_id").isin(planned_ids)).select("run_id").distinct().collect()
    }


def write_control_file(work_dir: Path, local_arl: Path, run: PlannedRun, output_name: str) -> None:
    met_dir = str(local_arl.parent.resolve()) + "/"
    out_dir = str(work_dir.resolve()) + "/"
    yy = run.init_time.year % 100
    lines = [
        f"{yy:02d} {run.init_time.month:02d} {run.init_time.day:02d} {run.init_time.hour:02d}",
        "1",
        f"{run.init_lat:.4f} {run.init_lon:.4f} {run.init_alt_m:.1f}",
        str(int(run.duration_hours)),
        "0",
        "10000.0",
        "1",
        met_dir,
        local_arl.name,
        out_dir,
        output_name,
    ]
    (work_dir / "CONTROL").write_text("\n".join(lines) + "\n", encoding="ascii")


def hysplit_temp_parent(hysplit_bin: str) -> str | None:
    configured = os.getenv("HYSPLIT_WORK_DIR", "").strip()
    candidates = [Path(configured)] if configured else []
    candidates.append(Path(DEFAULT_HYSPLIT_WORK_DIR))

    bin_path = Path(hysplit_bin)
    source_bdyfiles = None
    if len(bin_path.parents) >= 2:
        hysplit_root = bin_path.parents[1]
        source_bdyfiles = hysplit_root / "bdyfiles"
        candidates.append(hysplit_root / "working")

    for candidate in candidates:
        if not candidate:
            continue
        try:
            candidate.mkdir(parents=True, exist_ok=True)
            bdyfiles = candidate / "bdyfiles"
            if not (bdyfiles / "ASCDATA.CFG").exists() and source_bdyfiles and source_bdyfiles.exists():
                try:
                    bdyfiles.symlink_to(source_bdyfiles, target_is_directory=True)
                except FileExistsError:
                    pass
            if (bdyfiles / "ASCDATA.CFG").exists() and os.access(candidate, os.W_OK):
                return str(candidate)
        except OSError:
            continue
    return None


def read_text(path: Path, max_chars: int = 2000) -> str:
    if not path.exists():
        return ""
    try:
        return path.read_text(encoding="utf-8", errors="replace")[:max_chars]
    except OSError:
        return ""


def tdump_point_count(path: Path) -> int:
    count = 0
    try:
        with path.open("r", encoding="utf-8", errors="replace") as handle:
            for line in handle:
                parts = line.strip().split()
                if len(parts) < 12:
                    continue
                try:
                    int(float(parts[0]))
                    int(float(parts[2]))
                    lat = float(parts[9])
                    lon = float(parts[10])
                except ValueError:
                    continue
                if -90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0:
                    count += 1
    except OSError:
        return 0
    return count


def compact_hysplit_message(stdout: str, stderr: str, message: str) -> str:
    parts = []
    for label, value in (("stdout", stdout), ("stderr", stderr), ("MESSAGE", message)):
        text = (value or "").strip()
        if text:
            parts.append(f"{label}: {text}")
    return "\n".join(parts)[:4000]


def run_hysplit(spark: SparkSession, run: PlannedRun, hysplit_bin: str, cache_dir: Path) -> tuple[str, str | None]:
    if not os.path.exists(hysplit_bin):
        return "failed", f"HYSPLIT binary not found: {hysplit_bin}"

    local_arl = cache_dir / Path(hdfs_remote_path(run.arl_path)).name
    if not local_arl.exists():
        copy_hdfs_to_local(spark, run.arl_path, cache_dir)

    temp_parent = hysplit_temp_parent(hysplit_bin)
    with tempfile.TemporaryDirectory(prefix=f"{run.run_id}_", dir=temp_parent) as tmp:
        work_dir = Path(tmp)
        output_name = f"{run.run_id}.tdump"
        local_output = work_dir / output_name
        write_control_file(work_dir, local_arl, run, output_name)

        proc = subprocess.run(
            [hysplit_bin],
            cwd=work_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )
        stdout = proc.stdout or ""
        stderr = proc.stderr or ""
        message_file = read_text(work_dir / "MESSAGE")
        if proc.returncode != 0:
            message = compact_hysplit_message(stdout, stderr, message_file)
            return "failed", message or f"HYSPLIT exited with code {proc.returncode}"
        if not local_output.exists() or local_output.stat().st_size == 0:
            message = compact_hysplit_message(stdout, stderr, message_file)
            return "failed", (message[:3800] + " no tdump output") if message else "HYSPLIT produced no tdump output"
        point_count = tdump_point_count(local_output)
        if point_count == 0:
            message = compact_hysplit_message(stdout, stderr, message_file)
            detail = "HYSPLIT produced tdump header only; no trajectory points"
            return "failed", f"{detail}\n{message}"[:4000] if message else detail

        upload_local_to_hdfs(spark, local_output, run.output_path)
        return "success", None


def write_metadata(spark: SparkSession, rows: list[dict[str, Any]], table_name: str) -> None:
    if not rows:
        return
    df = spark.createDataFrame([Row(**row) for row in rows], schema=RUN_METADATA_SCHEMA)
    df.createOrReplaceTempView("hysplit_run_updates")
    spark.sql(
        f"""
        MERGE INTO {table_name} t
        USING hysplit_run_updates s
        ON t.run_id = s.run_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )


def clear_refresh_window(
    spark: SparkSession,
    table_name: str,
    start_date: date,
    end_date: date,
    directions: list[str],
) -> None:
    if not directions:
        return
    start_ts = f"{start_date.isoformat()} 00:00:00"
    end_ts = f"{end_date.isoformat()} 23:59:59"
    direction_list = ", ".join(f"'{direction}'" for direction in directions)
    spark.sql(
        f"""
        DELETE FROM {table_name}
        WHERE direction IN ({direction_list})
          AND init_time >= TIMESTAMP '{start_ts}'
          AND init_time <= TIMESTAMP '{end_ts}'
        """
    )


def main() -> None:
    args = parse_args()
    full_refresh = as_bool(args.full_refresh)
    tables = get_table_names()
    source_table = args.source_table or tables["era5_arl_bronze"]
    source_metadata_table = tables["era5_files_bronze"]
    openaq_table = args.openaq_table or tables["openaq_hourly_silver"]
    target_table = args.target_table or tables["hysplit_runs_bronze"]
    cfg = get_hysplit_config()
    threshold = float(cfg.get("pm25_trigger_threshold", 75))

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    ensure_table(spark, target_table)

    start_date = parse_date(args.start_date)
    end_date = parse_date(args.end_date)
    arl_files = collect_arl_files(spark, source_table, source_metadata_table, start_date, end_date)
    if start_date is None or end_date is None:
        default_start, default_end = default_window_from_arl(arl_files)
        start_date = start_date or default_start
        end_date = end_date or default_end
    if start_date is None or end_date is None:
        print(
            "hysplit_run_checks={'input_count': 0, 'output_count': 0, 'duplicate_count': 0, "
            "'success_count': 0, 'failure_count': 0, 'min_time': None, 'max_time': None}"
        )
        print("[INFO] No ARL files available for HYSPLIT")
        spark.stop()
        return
    if end_date < start_date:
        raise ValueError("--end-date must be >= --start-date")

    directions = ["backward", "forward"] if args.direction == "both" else [args.direction]
    backward_hours = (
        collect_backward_hours(spark, openaq_table, start_date, end_date, threshold)
        if "backward" in directions
        else []
    )
    planned = build_planned_runs(
        arl_files=arl_files,
        start_date=start_date,
        end_date=end_date,
        backward_hours=backward_hours,
        directions=directions,
        output_base_path=args.output_base_path,
    )

    duplicate_count = 0
    if full_refresh:
        clear_refresh_window(spark, target_table, start_date, end_date, directions)
    else:
        existing = existing_run_ids(spark, target_table, [run.run_id for run in planned])
        duplicate_count = len(existing)
        planned = [run for run in planned if run.run_id not in existing]

    min_time = min((run.init_time for run in planned), default=None)
    max_time = max((run.init_time for run in planned), default=None)
    rows: list[dict[str, Any]] = []
    success_count = 0
    failure_count = 0

    with tempfile.TemporaryDirectory(prefix="hysplit_arl_cache_") as cache:
        cache_dir = Path(cache)
        for run in planned:
            status, error = run_hysplit(spark, run, args.hysplit_bin, cache_dir)
            success_count += int(status == "success")
            failure_count += int(status != "success")
            rows.append(
                {
                    "run_id": run.run_id,
                    "direction": run.direction,
                    "init_time": run.init_time,
                    "duration_hours": int(run.duration_hours),
                    "init_lat": float(run.init_lat),
                    "init_lon": float(run.init_lon),
                    "init_alt_m": float(run.init_alt_m),
                    "arl_path": run.arl_path,
                    "output_path": run.output_path if status == "success" else None,
                    "status": status,
                    "error_message": error,
                    "spark_processed_at": datetime.utcnow(),
                }
            )
            print(f"hysplit_run run_id={run.run_id} status={status} output_path={run.output_path if status == 'success' else None}")

    write_metadata(spark, rows, target_table)
    print(
        "hysplit_run_checks="
        f"{{'input_count': {len(rows) + duplicate_count}, 'output_count': {len(rows)}, "
        f"'duplicate_count': {duplicate_count}, 'success_count': {success_count}, "
        f"'failure_count': {failure_count}, "
        f"'min_time': {repr(str(min_time) if min_time else None)}, "
        f"'max_time': {repr(str(max_time) if max_time else None)}}}"
    )
    print(f"Saved: {target_table}")
    spark.stop()

    if failure_count:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
