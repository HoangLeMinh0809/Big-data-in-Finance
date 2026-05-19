from __future__ import annotations

import argparse
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from hanoi_config import (
    ICEBERG_CATALOG,
    ICEBERG_WAREHOUSE,
    get_table_names,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Aggregate hourly trajectory features")
    parser.add_argument("--start-date", default=os.getenv("START_DATE", ""))
    parser.add_argument("--end-date", default=os.getenv("END_DATE", ""))
    parser.add_argument("--full-refresh", nargs="?", const="1", default=os.getenv("FULL_REFRESH", "0"))
    parser.add_argument("--cluster-table", default=os.getenv("HYSPLIT_CLUSTER_SILVER_TABLE", ""))
    parser.add_argument("--path-table", default=os.getenv("TRAJ_PATH_SILVER_TABLE", ""))
    parser.add_argument("--target-table", default=os.getenv("TRAJ_HOURLY_SILVER_TABLE", ""))
    return parser.parse_args()


def as_bool(raw: str) -> bool:
    return str(raw or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("TrajectoryHourlyFeaturesSilver")
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
            hour TIMESTAMP,
            dominant_cluster INT,
            n_traj INT,
            source_lat DOUBLE,
            source_lon DOUBLE,
            path_no2_mean DOUBLE,
            path_aer_mean DOUBLE,
            path_no2_aer_ratio DOUBLE,
            spark_processed_at TIMESTAMP,
            year INT,
            month INT
        )
        USING ICEBERG
        PARTITIONED BY (year, month)
        TBLPROPERTIES ('format-version'='2')
        """
    )


def filter_window(df, start_date: str, end_date: str):
    if start_date:
        df = df.filter(F.col("timestamp") >= F.to_timestamp(F.lit(f"{start_date} 00:00:00")))
    if end_date:
        df = df.filter(F.col("timestamp") <= F.to_timestamp(F.lit(f"{end_date} 23:59:59")))
    return df


def merge_iceberg(spark: SparkSession, df, table_name: str, full_refresh: bool) -> None:
    if full_refresh:
        spark.sql(f"DELETE FROM {table_name}")

    df.createOrReplaceTempView("traj_hourly_updates")
    spark.sql(
        f"""
        MERGE INTO {table_name} t
        USING traj_hourly_updates s
        ON t.hour = s.hour
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )


def main() -> None:
    args = parse_args()
    full_refresh = as_bool(args.full_refresh)
    tables = get_table_names()

    cluster_table = args.cluster_table or tables["hysplit_cluster_silver"]
    path_table = args.path_table or tables["trajectory_path_silver"]
    target_table = args.target_table or tables["trajectory_hourly_silver"]

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    ensure_table(spark, target_table)

    clustered = spark.table(cluster_table).filter(F.col("direction") == F.lit("backward"))
    clustered = filter_window(clustered, args.start_date, args.end_date)

    # Map each trajectory to its init hour (age_h=0, truncated to hour)
    init = (
        clustered.filter(F.col("age_h") == F.lit(0))
        .select(
            "traj_id",
            F.date_trunc("hour", F.col("timestamp")).alias("hour"),
            F.col("cluster_id").cast("int").alias("cluster_id"),
            F.col("source_lat").cast("double").alias("source_lat"),
            F.col("source_lon").cast("double").alias("source_lon"),
        )
        .dropDuplicates(["traj_id", "hour"])
    )

    # Join path features per trajectory
    path = spark.table(path_table).select(
        "traj_id",
        F.col("path_no2_mean").cast("double").alias("path_no2_mean"),
        F.col("path_aer_mean").cast("double").alias("path_aer_mean"),
        F.col("path_no2_aer_ratio").cast("double").alias("path_no2_aer_ratio"),
    )

    joined = init.join(path, on="traj_id", how="left")

    # dominant_cluster via mode (cluster_id with max count)
    counts = joined.groupBy("hour", "cluster_id").agg(F.countDistinct("traj_id").alias("n_traj_cluster"))
    win = Window.partitionBy("hour").orderBy(F.col("n_traj_cluster").desc(), F.col("cluster_id").asc())
    dominant = (
        counts.withColumn("rn", F.row_number().over(win))
        .filter(F.col("rn") == 1)
        .select("hour", F.col("cluster_id").alias("dominant_cluster"))
    )

    hourly = (
        joined.groupBy("hour")
        .agg(
            F.countDistinct("traj_id").alias("n_traj"),
            F.avg("source_lat").alias("source_lat"),
            F.avg("source_lon").alias("source_lon"),
            F.avg("path_no2_mean").alias("path_no2_mean"),
            F.avg("path_aer_mean").alias("path_aer_mean"),
            F.avg("path_no2_aer_ratio").alias("path_no2_aer_ratio"),
        )
        .join(dominant, on="hour", how="left")
        .withColumn("spark_processed_at", F.current_timestamp())
        .withColumn("year", F.year("hour"))
        .withColumn("month", F.month("hour"))
        .select(
            "hour",
            "dominant_cluster",
            "n_traj",
            "source_lat",
            "source_lon",
            "path_no2_mean",
            "path_aer_mean",
            "path_no2_aer_ratio",
            "spark_processed_at",
            "year",
            "month",
        )
    )

    input_count = init.count()
    output_count = hourly.count()
    duplicate_count = 0
    bounds = clustered.agg(F.min("timestamp").alias("min_time"), F.max("timestamp").alias("max_time")).first()

    coverage = (
        joined.groupBy("hour")
        .agg(
            F.countDistinct("traj_id").alias("n_traj"),
            F.sum(F.when(F.col("path_no2_mean").isNotNull(), F.lit(1)).otherwise(F.lit(0))).alias("traj_with_no2"),
            F.sum(F.when(F.col("path_aer_mean").isNotNull(), F.lit(1)).otherwise(F.lit(0))).alias("traj_with_aer"),
        )
        .orderBy("hour")
        .collect()
    )
    coverage_payload = {
        str(row["hour"]): {
            "n_traj": int(row["n_traj"] or 0),
            "traj_with_no2": int(row["traj_with_no2"] or 0),
            "traj_with_aer": int(row["traj_with_aer"] or 0),
        }
        for row in coverage
    }

    print(f"input_count={input_count}")
    print(f"output_count={output_count}")
    print(f"duplicate_count={duplicate_count}")
    print(f"min_time={bounds['min_time'] if bounds else None}")
    print(f"max_time={bounds['max_time'] if bounds else None}")
    print(f"coverage_by_hour={coverage_payload}")

    merge_iceberg(spark, hourly, target_table, full_refresh=full_refresh)
    print(f"Saved: {target_table}")
    spark.stop()


if __name__ == "__main__":
    main()
