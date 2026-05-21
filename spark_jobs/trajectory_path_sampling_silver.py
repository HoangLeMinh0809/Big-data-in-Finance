from __future__ import annotations

import argparse
import os
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from hanoi_config import (
    ICEBERG_CATALOG,
    ICEBERG_WAREHOUSE,
    get_sampling_config,
    get_table_names,
)


def parse_args() -> argparse.Namespace:
    cfg = get_sampling_config()
    parser = argparse.ArgumentParser(description="Sample Sentinel-5P pixels along backward HYSPLIT paths")
    parser.add_argument("--start-date", default=os.getenv("START_DATE", ""))
    parser.add_argument("--end-date", default=os.getenv("END_DATE", ""))
    parser.add_argument("--full-refresh", nargs="?", const="1", default=os.getenv("FULL_REFRESH", "0"))
    parser.add_argument("--max-distance-deg", type=float, default=float(cfg.get("max_distance_deg", 0.5)))
    # Path segment window relative to init hour (default -72..-24)
    parser.add_argument("--path-window-start-h", type=int, default=int(cfg.get("path_window_start_h", -72)))
    parser.add_argument("--path-window-end-h", type=int, default=int(cfg.get("path_window_end_h", -24)))
    parser.add_argument("--trajectory-table", default=os.getenv("HYSPLIT_TRAJ_SILVER_TABLE", ""))
    parser.add_argument("--grid-table", default=os.getenv("S5P_GRID_SILVER_TABLE", ""))
    parser.add_argument("--target-table", default=os.getenv("TRAJ_PATH_SILVER_TABLE", ""))
    return parser.parse_args()


def as_bool(raw: str) -> bool:
    return str(raw or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("TrajectoryPathSamplingSilver")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.warehouse", ICEBERG_WAREHOUSE)
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
        .getOrCreate()
    )


def ensure_table(spark: SparkSession, table_name: str) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ICEBERG_CATALOG}.features")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            traj_id STRING,
            path_no2_mean DOUBLE,
            path_aer_mean DOUBLE,
            path_no2_max DOUBLE,
            path_no2_std DOUBLE,
            path_no2_aer_ratio DOUBLE,
            spark_processed_at TIMESTAMP
        )
        USING ICEBERG
        TBLPROPERTIES ('format-version'='2')
        """
    )


def apply_date_range(df, start_date: str, end_date: str):
    if start_date:
        df = df.filter(F.to_date("timestamp") >= F.to_date(F.lit(start_date)))
    if end_date:
        df = df.filter(F.to_date("timestamp") <= F.to_date(F.lit(end_date)))
    return df


def merge_iceberg(spark: SparkSession, df, table_name: str, full_refresh: bool) -> None:
    if full_refresh:
        spark.sql(f"DELETE FROM {table_name}")

    df.createOrReplaceTempView("traj_path_updates")
    spark.sql(
        f"""
        MERGE INTO {table_name} t
        USING traj_path_updates s
        ON t.traj_id = s.traj_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )


def build_output(spark: SparkSession, traj_table: str, grid_table: str, args: argparse.Namespace):
    # 1) Load backward trajectories and compute init_time (age_h=0)
    traj = (
        spark.table(traj_table)
        .filter(F.col("direction") == F.lit("backward"))
        .filter(F.col("traj_id").isNotNull())
        .filter(F.col("timestamp").isNotNull())
        .filter(F.col("age_h").isNotNull())
    )
    traj = apply_date_range(traj, args.start_date, args.end_date)

    init_times = (
        traj.filter(F.col("age_h") == F.lit(0))
        .select("traj_id", F.col("timestamp").alias("init_time"))
        .distinct()
    )
    traj = traj.join(init_times, on="traj_id", how="inner")

    # 2) Window filter: keep points where age_h in [-72, -24]
    windowed = traj.filter(
        (F.col("age_h") >= F.lit(args.path_window_start_h))
        & (F.col("age_h") <= F.lit(args.path_window_end_h))
    ).select(
        "traj_id",
        "init_time",
        F.to_date("timestamp").alias("obs_date"),
        F.col("lat").cast("double").alias("traj_lat"),
        F.col("lon").cast("double").alias("traj_lon"),
    )

    # 3) Load S5P grid pixels; only need NO2 + AER_AI
    grid = (
        spark.table(grid_table)
        .filter(F.col("date").isNotNull())
        .filter(F.col("product").isin(["NO2", "AER_AI"]))
        .select(
            F.col("product"),
            F.col("date").alias("obs_date"),
            F.col("lat").cast("double").alias("pix_lat"),
            F.col("lon").cast("double").alias("pix_lon"),
            F.col("value").cast("double").alias("value"),
        )
    )

    # 4) Join by date, then filter by radius; take nearest pixel per traj point per product
    joined = windowed.join(grid, on="obs_date", how="left")
    joined = joined.withColumn(
        "dist_deg",
        F.sqrt(
            F.pow(F.col("traj_lat") - F.col("pix_lat"), F.lit(2.0))
            + F.pow(F.col("traj_lon") - F.col("pix_lon"), F.lit(2.0))
        ),
    ).filter(F.col("dist_deg") <= F.lit(args.max_distance_deg))

    nearest_w = Window.partitionBy(
        "traj_id",
        "init_time",
        "obs_date",
        "product",
        "traj_lat",
        "traj_lon",
    ).orderBy(F.col("dist_deg").asc())

    nearest = joined.withColumn("rn", F.row_number().over(nearest_w)).filter(F.col("rn") == 1)

    # 5) Aggregate per traj_id
    no2 = (
        nearest.filter(F.col("product") == F.lit("NO2"))
        .groupBy("traj_id")
        .agg(
            F.avg("value").alias("path_no2_mean"),
            F.max("value").alias("path_no2_max"),
            F.stddev_samp("value").alias("path_no2_std"),
        )
    )
    aer = (
        nearest.filter(F.col("product") == F.lit("AER_AI"))
        .groupBy("traj_id")
        .agg(F.avg("value").alias("path_aer_mean"))
    )

    output = (
        init_times.select("traj_id")
        .distinct()
        .join(no2, on="traj_id", how="left")
        .join(aer, on="traj_id", how="left")
        .withColumn(
            "path_no2_aer_ratio",
            F.when(
                F.col("path_no2_mean").isNotNull() & F.col("path_aer_mean").isNotNull() & (F.col("path_aer_mean") != 0),
                F.col("path_no2_mean") / F.col("path_aer_mean"),
            ).otherwise(F.lit(None).cast("double")),
        )
        .withColumn("spark_processed_at", F.current_timestamp())
        .select(
            "traj_id",
            "path_no2_mean",
            "path_aer_mean",
            "path_no2_max",
            "path_no2_std",
            "path_no2_aer_ratio",
            "spark_processed_at",
        )
    )

    matched_traj = nearest.select("traj_id").distinct()
    return init_times.select("traj_id").distinct(), matched_traj, output


def main() -> None:
    args = parse_args()
    tables = get_table_names()
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    traj_table = args.trajectory_table or tables["hysplit_traj_silver"]
    grid_table = args.grid_table or tables["s5p_grid_silver"]
    target_table = args.target_table or tables["trajectory_path_silver"]
    full_refresh = as_bool(args.full_refresh)

    ensure_table(spark, target_table)

    input_traj, matched_traj, output_df = build_output(spark, traj_table, grid_table, args)

    input_count = input_traj.count()
    matched_count = matched_traj.count()
    output_count = output_df.count()
    duplicate_count = 0
    matched_pixel_ratio = float(matched_count) / float(input_count) if input_count else None
    path_no2_stats = output_df.agg(
        F.min("path_no2_mean").alias("path_no2_mean_min"),
        F.max("path_no2_mean").alias("path_no2_mean_max"),
    ).first()

    bounds = spark.table(traj_table).filter(F.col("direction") == F.lit("backward")).agg(
        F.min("timestamp").alias("min_time"), F.max("timestamp").alias("max_time")
    ).first()

    print(f"input_count={input_count}")
    print(f"output_count={output_count}")
    print(f"duplicate_count={duplicate_count}")
    print(f"min_time={bounds['min_time'] if bounds else None}")
    print(f"max_time={bounds['max_time'] if bounds else None}")
    print(f"matched_traj_count={matched_count}")
    print(f"unmatched_traj_count={max(0, input_count - matched_count)}")
    print(f"matched_pixel_ratio={matched_pixel_ratio}")
    print(f"path_no2_mean_min={path_no2_stats['path_no2_mean_min'] if path_no2_stats else None}")
    print(f"path_no2_mean_max={path_no2_stats['path_no2_mean_max'] if path_no2_stats else None}")
    print(
        "trajectory_path_sampling_checks="
        f"{{'input_count': {input_count}, 'output_count': {output_count}, "
        f"'duplicate_count': {duplicate_count}, 'matched_traj_count': {matched_count}, "
        f"'unmatched_traj_count': {max(0, input_count - matched_count)}, "
        f"'matched_pixel_ratio': {matched_pixel_ratio}, "
        f"'path_no2_mean_min': {path_no2_stats['path_no2_mean_min'] if path_no2_stats else None}, "
        f"'path_no2_mean_max': {path_no2_stats['path_no2_mean_max'] if path_no2_stats else None}, "
        f"'min_time': {repr(str(bounds['min_time']) if bounds else None)}, "
        f"'max_time': {repr(str(bounds['max_time']) if bounds else None)}}}"
    )

    merge_iceberg(spark, output_df, target_table, full_refresh=full_refresh)
    print(f"Saved: {target_table}")
    spark.stop()


if __name__ == "__main__":
    main()
