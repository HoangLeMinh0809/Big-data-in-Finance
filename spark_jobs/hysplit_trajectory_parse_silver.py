"""Parse HYSPLIT tdump outputs from HDFS into trajectory silver Iceberg table."""

from __future__ import annotations

import argparse
import os
import re
from datetime import datetime, timezone
from typing import Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from hanoi_config import ICEBERG_CATALOG, ICEBERG_WAREHOUSE, get_table_names


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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Parse HYSPLIT tdump outputs into Iceberg")
    parser.add_argument("--start-date", default=os.getenv("START_DATE", ""))
    parser.add_argument("--end-date", default=os.getenv("END_DATE", ""))
    parser.add_argument("--full-refresh", nargs="?", const="1", default=os.getenv("FULL_REFRESH", "0"))
    parser.add_argument("--runs-table", default=os.getenv("HYSPLIT_RUNS_TABLE", ""))
    parser.add_argument("--target-table", default=os.getenv("HYSPLIT_TRAJ_SILVER_TABLE", ""))
    return parser.parse_args()


def as_bool(raw: str) -> bool:
    return str(raw or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("HYSPLITTrajectoryParseSilver")
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
            traj_id STRING,
            direction STRING,
            traj_no INT,
            year INT,
            month INT,
            day INT,
            hour INT,
            minute INT,
            forecast_hour INT,
            age_h INT,
            lat DOUBLE,
            lon DOUBLE,
            alt_m DOUBLE,
            timestamp TIMESTAMP,
            spark_processed_at TIMESTAMP
        )
        USING ICEBERG
        PARTITIONED BY (direction, year, month)
        TBLPROPERTIES ('format-version'='2')
        """
    )


def parse_year(value: int) -> int:
    if value < 100:
        return 2000 + value if value < 70 else 1900 + value
    return value


def parse_hysplit_numeric(run_id: str, direction: str, tokens: list[str]) -> Optional[tuple]:
    if len(tokens) < 12:
        return None
    try:
        traj_no = int(float(tokens[0]))
        year = parse_year(int(float(tokens[2])))
        month = int(float(tokens[3]))
        day = int(float(tokens[4]))
        hour = int(float(tokens[5]))
        minute = int(float(tokens[6]))
        forecast_hour = int(round(float(tokens[7])))
        age_h = int(round(float(tokens[8])))
        lat = float(tokens[9])
        lon = float(tokens[10])
        alt_m = float(tokens[11])
        if not (-90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0):
            return None
        ts = datetime(year, month, day, hour, minute, tzinfo=timezone.utc).replace(tzinfo=None)
    except Exception:
        return None
    return (
        f"{run_id}-traj{traj_no}",
        direction,
        traj_no,
        year,
        month,
        day,
        hour,
        minute,
        forecast_hour,
        age_h,
        lat,
        lon,
        alt_m,
        ts,
    )


def parse_iso_fallback(run_id: str, direction: str, line: str) -> Optional[tuple]:
    tokens = re.split(r"\s+", line.strip())
    if len(tokens) < 5:
        return None
    try:
        if re.match(r"\d{4}-\d{2}-\d{2}", tokens[0]) and ":" in tokens[1]:
            ts = datetime.fromisoformat(f"{tokens[0]} {tokens[1]}").replace(tzinfo=timezone.utc).replace(tzinfo=None)
            numeric = [float(t) for t in tokens[2:] if re.match(r"^-?\d+(\.\d+)?$", t)]
            for i in range(len(numeric) - 1):
                lat = numeric[i]
                lon = numeric[i + 1]
                if -90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0:
                    alt_m = numeric[i + 2] if i + 2 < len(numeric) else None
                    return (
                        f"{run_id}-traj0",
                        direction,
                        0,
                        ts.year,
                        ts.month,
                        ts.day,
                        ts.hour,
                        ts.minute,
                        None,
                        None,
                        float(lat),
                        float(lon),
                        float(alt_m) if alt_m is not None else None,
                        ts,
                    )
    except Exception:
        return None
    return None


def parse_line(record: tuple) -> Optional[tuple]:
    run_id, direction, line = record
    text = (line or "").strip()
    if not text:
        return None
    if text.startswith("#"):
        return None
    lowered = text.lower()
    if any(marker in lowered for marker in ("press", "trajectory", "meteorology", "job id")):
        return None

    tokens = re.split(r"\s+", text)
    parsed = parse_hysplit_numeric(run_id, direction, tokens)
    if parsed is not None:
        return parsed
    return parse_iso_fallback(run_id, direction, text)


def load_runs(spark: SparkSession, runs_table: str, start_date: str, end_date: str):
    runs_df = spark.table(runs_table).filter(F.col("status") == F.lit("success")).filter(F.col("output_path").isNotNull())
    if start_date:
        runs_df = runs_df.filter(F.col("init_time") >= F.to_timestamp(F.lit(f"{start_date} 00:00:00")))
    if end_date:
        runs_df = runs_df.filter(F.col("init_time") <= F.to_timestamp(F.lit(f"{end_date} 23:59:59")))
    return runs_df.select("run_id", "direction", "output_path").collect()


def merge_trajectory_rows(spark: SparkSession, df, table_name: str) -> None:
    df.createOrReplaceTempView("hysplit_trajectory_updates")
    spark.sql(
        f"""
        MERGE INTO {table_name} t
        USING hysplit_trajectory_updates s
        ON t.traj_id = s.traj_id AND t.age_h = s.age_h
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )


def main() -> None:
    args = parse_args()
    full_refresh = as_bool(args.full_refresh)
    tables = get_table_names()
    runs_table = args.runs_table or tables["hysplit_runs_bronze"]
    target_table = args.target_table or tables["hysplit_traj_silver"]

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    ensure_table(spark, target_table)

    runs = load_runs(spark, runs_table, args.start_date, args.end_date)
    rdds = []
    file_count = 0
    for row in runs:
        path = row["output_path"]
        try:
            files = spark.sparkContext.wholeTextFiles(path)
            rdds.append(
                files.flatMap(
                    lambda kv, run_id=row["run_id"], direction=row["direction"]: [
                        (run_id, direction, line) for line in kv[1].splitlines()
                    ]
                )
            )
            file_count += 1
        except Exception as exc:
            print(f"[WARN] Cannot read HYSPLIT output run_id={row['run_id']} path={path}: {exc}")

    if not rdds:
        print(
            "hysplit_parse_checks={'input_count': 0, 'output_count': 0, 'duplicate_count': 0, "
            "'min_time': None, 'max_time': None}"
        )
        spark.stop()
        return

    raw_rdd = spark.sparkContext.union(rdds)
    parsed_rdd = raw_rdd.map(parse_line).filter(lambda value: value is not None)
    parsed_df = spark.createDataFrame(parsed_rdd, schema=SCHEMA).withColumn("spark_processed_at", F.current_timestamp())

    input_count = raw_rdd.count()
    output_count = parsed_df.count()
    if output_count == 0:
        print(
            f"hysplit_parse_checks={{'input_count': {input_count}, 'output_count': 0, "
            "'duplicate_count': 0, 'min_time': None, 'max_time': None}"
        )
        spark.stop()
        return

    duplicate_count = (
        parsed_df.groupBy("traj_id", "age_h")
        .count()
        .filter(F.col("count") > 1)
        .select(F.sum(F.col("count") - F.lit(1)).alias("duplicates"))
        .first()["duplicates"]
    )
    duplicate_count = int(duplicate_count or 0)
    bounds = parsed_df.agg(F.min("timestamp").alias("min_time"), F.max("timestamp").alias("max_time")).first()
    deduped = parsed_df.dropDuplicates(["traj_id", "age_h"])

    if full_refresh:
        merge_trajectory_rows(spark, deduped, target_table)
    else:
        existing = spark.table(target_table).select("traj_id", "age_h")
        deduped = deduped.join(existing, on=["traj_id", "age_h"], how="left_anti")
        merge_trajectory_rows(spark, deduped, target_table)

    print(
        "hysplit_parse_checks="
        f"{{'input_count': {input_count}, 'output_count': {output_count}, "
        f"'duplicate_count': {duplicate_count}, 'file_count': {file_count}, "
        f"'min_time': {repr(str(bounds['min_time']) if bounds else None)}, "
        f"'max_time': {repr(str(bounds['max_time']) if bounds else None)}}}"
    )
    print(f"Saved: {target_table}")
    spark.stop()


if __name__ == "__main__":
    main()
