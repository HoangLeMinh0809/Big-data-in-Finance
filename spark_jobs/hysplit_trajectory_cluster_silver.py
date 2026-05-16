"""Cluster HYSPLIT trajectories and write per-point clustered silver rows."""

from __future__ import annotations

import argparse
import os
from datetime import datetime

from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import Imputer, StandardScaler, VectorAssembler
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from hanoi_config import (
    ICEBERG_CATALOG,
    ICEBERG_WAREHOUSE,
    get_table_names,
    get_trajectory_config,
)


def parse_args() -> argparse.Namespace:
    cfg = get_trajectory_config()
    parser = argparse.ArgumentParser(description="Cluster HYSPLIT trajectories")
    parser.add_argument("--start-date", default=os.getenv("START_DATE", ""))
    parser.add_argument("--end-date", default=os.getenv("END_DATE", ""))
    parser.add_argument("--full-refresh", nargs="?", const="1", default=os.getenv("FULL_REFRESH", "0"))
    parser.add_argument("--direction", choices=("backward", "forward", "all"), default=os.getenv("DIRECTION", "backward"))
    parser.add_argument("--source-table", default=os.getenv("HYSPLIT_TRAJ_SILVER_TABLE", ""))
    parser.add_argument("--target-table", default=os.getenv("HYSPLIT_CLUSTER_SILVER_TABLE", ""))
    parser.add_argument(
        "--anchor-hours",
        default=os.getenv("ANCHOR_HOURS")
        or ",".join(str(v) for v in cfg.get("anchor_hours", [0, -6, -12, -24, -36, -48, -60, -72])),
    )
    parser.add_argument("--k-min", type=int, default=int(cfg.get("cluster_k_min", 3)))
    parser.add_argument("--k-max", type=int, default=int(cfg.get("cluster_k_max", 10)))
    parser.add_argument("--k-default", type=int, default=int(cfg.get("cluster_k_default", 6)))
    return parser.parse_args()


def as_bool(raw: str) -> bool:
    return str(raw or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def anchor_col(prefix: str, hour: int) -> str:
    label = f"m{abs(hour)}" if hour < 0 else f"p{hour}"
    return f"{prefix}_{label}"


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("HYSPLITTrajectoryClusterSilver")
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
            cluster_id INT,
            direction STRING,
            age_h INT,
            lat DOUBLE,
            lon DOUBLE,
            alt_m DOUBLE,
            timestamp TIMESTAMP,
            source_lat DOUBLE,
            source_lon DOUBLE,
            source_alt_m DOUBLE,
            spark_processed_at TIMESTAMP
        )
        USING ICEBERG
        PARTITIONED BY (direction)
        TBLPROPERTIES ('format-version'='2')
        """
    )


def filter_window(df, start_date: str, end_date: str):
    if start_date:
        df = df.filter(F.col("timestamp") >= F.to_timestamp(F.lit(f"{start_date} 00:00:00")))
    if end_date:
        df = df.filter(F.col("timestamp") <= F.to_timestamp(F.lit(f"{end_date} 23:59:59")))
    return df


def filter_by_init_window(df, start_date: str, end_date: str):
    if not start_date and not end_date:
        return df
    init_points = df.filter(F.col("age_h") == F.lit(0))
    init_points = filter_window(init_points, start_date, end_date)
    init_ids = init_points.select("traj_id").distinct()
    return df.join(init_ids, on="traj_id", how="inner")


def delete_target_directions(spark: SparkSession, table_name: str, directions: list[str]) -> None:
    if not directions:
        return
    direction_list = ", ".join(f"'{value}'" for value in sorted(set(directions)))
    spark.sql(f"DELETE FROM {table_name} WHERE direction IN ({direction_list})")


def append_cluster_rows(df, table_name: str) -> None:
    df.writeTo(table_name).append()


def main() -> None:
    args = parse_args()
    full_refresh = as_bool(args.full_refresh)
    anchor_hours = [int(value) for value in args.anchor_hours.split(",") if value.strip()]
    tables = get_table_names()
    source_table = args.source_table or tables["hysplit_traj_silver"]
    target_table = args.target_table or tables["hysplit_cluster_silver"]

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    ensure_table(spark, target_table)

    points = spark.table(source_table).filter(
        F.col("lat").isNotNull() & F.col("lon").isNotNull() & F.col("age_h").isNotNull()
    )
    if args.direction != "all":
        points = points.filter(F.col("direction") == F.lit(args.direction))
    points = filter_by_init_window(points, args.start_date, args.end_date)
    raw_point_count = points.count()
    if raw_point_count == 0:
        print(
            "hysplit_cluster_checks={'input_count': 0, 'output_count': 0, 'duplicate_count': 0, "
            "'min_time': None, 'max_time': None}"
        )
        spark.stop()
        return

    agg_exprs = []
    for hour in anchor_hours:
        agg_exprs.extend(
            [
                F.max(F.when(F.col("age_h") == F.lit(hour), F.col("lat"))).alias(anchor_col("lat", hour)),
                F.max(F.when(F.col("age_h") == F.lit(hour), F.col("lon"))).alias(anchor_col("lon", hour)),
                F.max(F.when(F.col("age_h") == F.lit(hour), F.col("alt_m"))).alias(anchor_col("alt", hour)),
            ]
        )

    grouped = points.groupBy("traj_id", "direction").agg(*agg_exprs)
    input_count = grouped.count()
    bounds = points.agg(F.min("timestamp").alias("min_time"), F.max("timestamp").alias("max_time")).first()

    source_lat_name = anchor_col("lat", 0)
    source_lon_name = anchor_col("lon", 0)
    source_alt_name = anchor_col("alt", 0)
    feature_cols = [anchor_col("lat", h) for h in anchor_hours] + [anchor_col("lon", h) for h in anchor_hours]
    count_exprs = [F.count(F.col(name)).alias(name) for name in feature_cols]
    non_null_counts = grouped.agg(*count_exprs).first().asDict()
    feature_cols = [name for name in feature_cols if int(non_null_counts.get(name) or 0) > 0]
    assign_without_kmeans = input_count < 2 or len(feature_cols) < 2

    if assign_without_kmeans:
        assignments = (
            grouped
            .withColumn("cluster_id", F.lit(0).cast("int"))
            .select(
                "traj_id",
                "direction",
                "cluster_id",
                F.col(source_lat_name).alias("source_lat"),
                F.col(source_lon_name).alias("source_lon"),
                F.col(source_alt_name).alias("source_alt_m"),
            )
        )
        print("[SWEEP] skipped: insufficient trajectories or anchor features; assigned cluster_id=0")
    else:
        imputed_cols = [f"{col_name}_imp" for col_name in feature_cols]
        imputer = Imputer(strategy="mean", inputCols=feature_cols, outputCols=imputed_cols)
        imputed = imputer.fit(grouped).transform(grouped)
        assembled = VectorAssembler(inputCols=imputed_cols, outputCol="raw_features").transform(imputed)
        scaler = StandardScaler(inputCol="raw_features", outputCol="features", withStd=True, withMean=True)
        scaled = scaler.fit(assembled).transform(assembled).persist()

        k_min = max(2, min(int(args.k_min), input_count))
        k_max = max(k_min, min(int(args.k_max), input_count))
        k_default = min(max(int(args.k_default), k_min), k_max)

        print(f"[INFO] Running k-sweep {k_min}..{k_max}; final k={k_default}")
        for k in range(k_min, k_max + 1):
            model = KMeans(k=k, featuresCol="features", seed=42, maxIter=40).fit(scaled)
            try:
                cost = float(model.summary.trainingCost)
            except Exception:
                cost = float("nan")
            print(f"[SWEEP] k={k} WCSS={cost}")

        final_model = KMeans(k=k_default, featuresCol="features", seed=42, maxIter=100).fit(scaled)
        assignments = (
            final_model.transform(scaled)
            .withColumnRenamed("prediction", "cluster_id")
            .select(
                "traj_id",
                "direction",
                F.col("cluster_id").cast("int"),
                F.col(source_lat_name).alias("source_lat"),
                F.col(source_lon_name).alias("source_lon"),
                F.col(source_alt_name).alias("source_alt_m"),
            )
        )

    output = (
        points.join(assignments, on=["traj_id", "direction"], how="inner")
        .select(
            "traj_id",
            "cluster_id",
            "direction",
            "age_h",
            "lat",
            "lon",
            "alt_m",
            "timestamp",
            "source_lat",
            "source_lon",
            "source_alt_m",
        )
        .withColumn("spark_processed_at", F.lit(datetime.utcnow()).cast("timestamp"))
    )

    duplicate_count = (
        output.groupBy("traj_id", "age_h")
        .count()
        .filter(F.col("count") > 1)
        .select(F.sum(F.col("count") - F.lit(1)).alias("duplicates"))
        .first()["duplicates"]
    )
    duplicate_count = int(duplicate_count or 0)
    output = output.dropDuplicates(["traj_id", "age_h"])

    if full_refresh:
        refresh_directions = (
            [args.direction]
            if args.direction != "all"
            else [row["direction"] for row in output.select("direction").distinct().collect()]
        )
        delete_target_directions(spark, target_table, refresh_directions)
    else:
        existing = spark.table(target_table).select("traj_id", "age_h")
        output = output.join(existing, on=["traj_id", "age_h"], how="left_anti")

    output_count = output.count()
    if output_count:
        append_cluster_rows(output, target_table)

    print("Cluster distribution:")
    for row in output.groupBy("cluster_id").count().orderBy("cluster_id").collect():
        print(f"  cluster={row['cluster_id']}: n={row['count']}")
    print(
        "hysplit_cluster_checks="
        f"{{'input_count': {input_count}, 'output_count': {output_count}, "
        f"'duplicate_count': {duplicate_count}, "
        f"'min_time': {repr(str(bounds['min_time']) if bounds else None)}, "
        f"'max_time': {repr(str(bounds['max_time']) if bounds else None)}}}"
    )
    print(f"Saved: {target_table}")
    spark.stop()


if __name__ == "__main__":
    main()
