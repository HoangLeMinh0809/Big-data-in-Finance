"""Parse HYSPLIT trajectory text outputs from HDFS and write to Iceberg (silver).

Reads metadata from `ais.trajectory.hysplit_runs_bronze` to discover
successful runs and their `output_path` on HDFS, parses the trajectory text
files and writes normalized rows into `ais.trajectory.hysplit_trajectories_silver`.

The job supports standard args: `--start-date`, `--end-date`, `--full-refresh`.

Acceptance logs: prints `input_count`, `output_count`, `duplicate_count`,
`min_time`, `max_time`.

Usage example:
  spark-submit --master spark://spark-master:7077 \ 
    spark_jobs/hysplit_trajectory_parse_silver.py --start-date 2026-05-09 --end-date 2026-05-16
"""

from __future__ import annotations

import argparse
import hashlib
import os
import re
from datetime import datetime, timezone
from typing import Optional, Tuple

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType,
)
from pyspark.sql.functions import (
    input_file_name,
    current_timestamp,
    col,
    lit,
    to_timestamp,
    min as sf_min,
    max as sf_max,
    sum as sf_sum,
)


RUNS_TABLE = os.environ.get("HYSPLIT_RUNS_TABLE", "ais.trajectory.hysplit_runs_bronze")
TRAJ_SILVER_TABLE = os.environ.get("HYSPLIT_TRAJ_SILVER_TABLE", "ais.trajectory.hysplit_trajectories_silver")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--start-date", type=str, help="YYYY-MM-DD inclusive start date", required=False)
    p.add_argument("--end-date", type=str, help="YYYY-MM-DD inclusive end date", required=False)
    p.add_argument("--full-refresh", nargs="?", const="1", default=os.environ.get("FULL_REFRESH", "0"))
    args = p.parse_args()
    args.full_refresh = str(args.full_refresh).strip().lower() in {"1", "true", "yes", "y", "on"}
    return args


def make_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("HYSPLIT_Trajectory_Parse_Silver")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.ais", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.ais.type", "hadoop")
        .config("spark.sql.catalog.ais.warehouse", os.environ.get("ICEBERG_WAREHOUSE", "hdfs://namenode:9000/warehouse/iceberg"))
        .config("spark.hadoop.fs.defaultFS", os.environ.get("HDFS_DEFAULT", "hdfs://namenode:9000"))
        .getOrCreate()
    )


def _parse_datetime_from_tokens(tokens: list[str]) -> Optional[datetime]:
    """Try several common datetime token layouts, return aware UTC datetime or None."""
    try:
        # Case: ISO date + time e.g. 2026-05-16 12:00:00
        if len(tokens) >= 2 and re.match(r"\d{4}-\d{2}-\d{2}", tokens[0]) and ":" in tokens[1]:
            dt = datetime.fromisoformat(tokens[0] + " " + tokens[1])
            return dt.replace(tzinfo=timezone.utc)

        # Case: space-separated ints: YYYY MM DD HH MM SS
        if len(tokens) >= 6 and all(re.match(r"^\d+$", t) for t in tokens[:6]):
            y, m, d, hh, mm, ss = map(int, tokens[:6])
            return datetime(y, m, d, hh, mm, ss, tzinfo=timezone.utc)
    except Exception:
        return None
    return None


def _find_latlonalt(tokens: list[str]) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """Search token sequence for plausible (lat, lon, alt_m).

    Returns (lat, lon, alt_m) or (None, None, None).
    """
    for i in range(len(tokens) - 1):
        try:
            a = float(tokens[i])
            b = float(tokens[i + 1])
        except Exception:
            continue

        # lat/lon ordering: lat in [-90,90], lon in [-180,180]
        if -90.0 <= a <= 90.0 and -180.0 <= b <= 180.0:
            lat, lon = a, b
            alt = None
            if i + 2 < len(tokens):
                try:
                    alt = float(tokens[i + 2])
                except Exception:
                    alt = None
            return lat, lon, alt

        # or lon/lat ordering
        if -180.0 <= a <= 180.0 and -90.0 <= b <= 90.0:
            lon, lat = a, b
            alt = None
            if i + 2 < len(tokens):
                try:
                    alt = float(tokens[i + 2])
                except Exception:
                    alt = None
            return lat, lon, alt

    return None, None, None


def parse_line_for_run(record: tuple) -> Optional[tuple]:
    """Parse one HYSPLIT output line for a specific run.

    record: (run_id, direction, path, init_time (ts or None), line)
    returns tuple matching schema or None
    """
    run_id, direction, path, init_time, line = record
    line = line.strip()
    if not line:
        return None
    if line.startswith("#") or line.lower().startswith("start"):
        return None

    # Tokens
    tokens = re.split(r"\s+", line)

    # attempt to parse datetime
    dt = _parse_datetime_from_tokens(tokens)

    # find lat/lon/alt
    lat, lon, alt = _find_latlonalt(tokens)

    if dt is None and (lat is None or lon is None):
        # unable to parse
        return None

    # compute year/month/day/hour/minute
    try:
        year = dt.year if dt is not None else None
        month = dt.month if dt is not None else None
        day = dt.day if dt is not None else None
        hour = dt.hour if dt is not None else None
        minute = dt.minute if dt is not None else None
    except Exception:
        year = month = day = hour = minute = None

    # compute age_h relative to init_time if available
    age_h = None
    if dt is not None and init_time is not None:
        try:
            # init_time can be a python datetime, or string; normalize
            if isinstance(init_time, str):
                init_dt = datetime.fromisoformat(init_time)
            else:
                init_dt = init_time
            # age = (dt - init_dt) hours. Round to nearest int
            age_h = int(round((dt - init_dt).total_seconds() / 3600.0))
        except Exception:
            age_h = None

    # deterministic traj_id: md5 of line + run_id + basename
    try:
        file_basename = os.path.basename(path) if path else ""
        h = hashlib.md5((line + (run_id or "") + file_basename).encode("utf-8")).hexdigest()
        traj_id = f"{run_id}-{h}"
    except Exception:
        traj_id = f"{run_id}-{hash(line)}"

    return (
        traj_id,
        direction,
        0,
        year if year is not None else -1,
        month if month is not None else -1,
        day if day is not None else -1,
        hour if hour is not None else -1,
        minute if minute is not None else 0,
        None,
        age_h if age_h is not None else None,
        float(lat) if lat is not None else None,
        float(lon) if lon is not None else None,
        float(alt) if alt is not None else None,
        dt,
    )


SCHEMA = StructType(
    [
        StructField("traj_id", StringType(), False),
        StructField("direction", StringType(), True),
        StructField("traj_no", IntegerType(), True),
        StructField("year", IntegerType(), True),
        StructField("month", IntegerType(), True),
        StructField("day", IntegerType(), True),
        StructField("hour", IntegerType(), True),
        StructField("minute", IntegerType(), True),
        StructField("forecast_hour", IntegerType(), True),
        StructField("age_h", IntegerType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True),
        StructField("alt_m", DoubleType(), True),
        StructField("timestamp", TimestampType(), True),
    ]
)


def main() -> None:
    args = parse_args()
    spark = make_spark()
    spark.sparkContext.setLogLevel("WARN")

    runs_df = spark.read.format("iceberg").load(RUNS_TABLE)
    runs_df = runs_df.filter(col("status") == lit("success"))

    if args.start_date:
        runs_df = runs_df.filter(col("init_time") >= to_timestamp(lit(args.start_date + " 00:00:00")))
    if args.end_date:
        runs_df = runs_df.filter(col("init_time") <= to_timestamp(lit(args.end_date + " 23:59:59")))

    runs = runs_df.select("run_id", "direction", "output_path", "init_time").collect()
    rdds = []

    for r in runs:
        path = r.output_path
        if not path:
            continue

        # If path contains a wildcard or is a directory, use wholeTextFiles to preserve filenames.
        try:
            if "*" in path or path.endswith("/"):
                pattern = path if "*" in path else path.rstrip("/") + "/*"
                files_rdd = spark.sparkContext.wholeTextFiles(pattern)
                # files_rdd: RDD[(filename, filecontents)]
                src_rdd = files_rdd.flatMap(lambda kv: [(r.run_id, r.direction, kv[0], r.init_time, ln) for ln in kv[1].splitlines()])
            else:
                # Try wholeTextFiles first (works for single file URIs), fallback to textFile
                try:
                    files_rdd = spark.sparkContext.wholeTextFiles(path)
                    src_rdd = files_rdd.flatMap(lambda kv: [(r.run_id, r.direction, kv[0], r.init_time, ln) for ln in kv[1].splitlines()])
                except Exception:
                    src_rdd = spark.sparkContext.textFile(path).map(lambda ln: (r.run_id, r.direction, path, r.init_time, ln))

            parsed = src_rdd.map(parse_line_for_run).filter(lambda x: x is not None)
            rdds.append(parsed)
        except Exception as exc:
            print(f"[WARN] Unable to read HYSPLIT outputs for run {r.run_id} at {path}: {exc}")
            continue

    if not rdds:
        print("[INFO] No HYSPLIT outputs found to parse.")
        return

    all_rdd = spark.sparkContext.union(rdds)
    parsed_df = spark.createDataFrame(all_rdd, schema=SCHEMA)
    parsed_df = parsed_df.withColumn("spark_processed_at", current_timestamp())

    # Metrics
    input_count = all_rdd.count()
    output_count = parsed_df.count()

    # duplicates by traj_id + age_h
    dup_df = parsed_df.groupBy("traj_id", "age_h").count().filter(col("count") > 1)
    duplicate_count_row = dup_df.select(sf_sum(col("count") - 1)).collect()
    duplicate_count = int(duplicate_count_row[0][0]) if duplicate_count_row and duplicate_count_row[0][0] is not None else 0

    min_max = parsed_df.agg(sf_min(col("timestamp")), sf_max(col("timestamp"))).collect()[0]
    min_time = min_max[0]
    max_time = min_max[1]

    # Deduplicate (keep first by traj_id+age_h)
    deduped = (
        parsed_df.dropDuplicates(["traj_id", "age_h"]) if output_count > 0 else parsed_df
    )

    # Write to Iceberg
    if deduped.count() > 0:
        if args.full_refresh:
            spark.sql(f"DELETE FROM {TRAJ_SILVER_TABLE}")
        deduped.write.format("iceberg").mode("append").saveAsTable(TRAJ_SILVER_TABLE)

    print(f"Parsed HYSPLIT summary: input_count={input_count}, output_count={output_count}, duplicate_count={duplicate_count}")
    print(f"time_range: {min_time} to {max_time}")


if __name__ == "__main__":
    main()
