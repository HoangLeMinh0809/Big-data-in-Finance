"""Trajectory clustering (silver)

Pivot HYSPLIT trajectories on configured `anchor_hours`, build a feature
vector from lat/lon at those anchor ages, standardize and run KMeans.

Writes per-trajectory cluster assignment to
`ais.trajectory.hysplit_trajectories_clustered_silver`.

Usage:
  spark-submit --master spark://spark-master:7077 \
    spark_jobs/hysplit_trajectory_cluster_silver.py \
    --start-date 2026-05-09 --end-date 2026-05-16 --k-default 6
"""

from __future__ import annotations

import argparse
import os
from datetime import datetime
from typing import List

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    lit,
    to_timestamp,
    when,
    max as sf_max,
    min as sf_min,
    current_timestamp,
)

from pyspark.ml.feature import VectorAssembler, StandardScaler, Imputer
from pyspark.ml.clustering import KMeans


DEFAULT_ANCHOR_HOURS = [0, -6, -12, -24, -36, -48, -60, -72]
TRAJ_SILVER_TABLE = os.environ.get(
    "HYSPLIT_TRAJ_SILVER_TABLE", "ais.trajectory.hysplit_trajectories_silver"
)
HYSPLIT_CLUSTER_SILVER_TABLE = os.environ.get(
    "HYSPLIT_CLUSTER_SILVER_TABLE", "ais.trajectory.hysplit_trajectories_clustered_silver"
)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--start-date", type=str, required=False)
    p.add_argument("--end-date", type=str, required=False)
    p.add_argument("--full-refresh", nargs="?", const="1", default=os.environ.get("FULL_REFRESH", "0"))
    p.add_argument(
        "--anchor-hours",
        type=str,
        default=",".join(map(str, DEFAULT_ANCHOR_HOURS)),
        help="Comma-separated anchor hours (e.g. 0,-6,-12)",
    )
    p.add_argument("--k-min", type=int, default=3)
    p.add_argument("--k-max", type=int, default=10)
    p.add_argument("--k-default", type=int, default=6)
    args = p.parse_args()
    args.full_refresh = str(args.full_refresh).strip().lower() in {"1", "true", "yes", "y", "on"}
    return args


def make_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("HYSPLIT_Trajectory_Cluster_Silver")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.ais", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.ais.type", "hadoop")
        .config("spark.sql.catalog.ais.warehouse", os.environ.get("ICEBERG_WAREHOUSE", "hdfs://namenode:9000/warehouse/iceberg"))
        .config("spark.hadoop.fs.defaultFS", os.environ.get("HDFS_DEFAULT", "hdfs://namenode:9000"))
        .getOrCreate()
    )


def main() -> None:
    args = parse_args()
    anchor_hours: List[int] = [int(x) for x in args.anchor_hours.split(",") if x != ""]

    spark = make_spark()
    spark.sparkContext.setLogLevel("WARN")

    # Load trajectories (silver)
    df = spark.read.format("iceberg").load(TRAJ_SILVER_TABLE)

    if args.start_date:
        df = df.filter(col("timestamp") >= to_timestamp(lit(args.start_date + " 00:00:00")))
    if args.end_date:
        df = df.filter(col("timestamp") <= to_timestamp(lit(args.end_date + " 23:59:59")))

    # Build pivoted features: lat_{h}, lon_{h} for each anchor hour
    agg_exprs = []
    for h in anchor_hours:
        agg_exprs.append(sf_max(when(col("age_h") == lit(h), col("lat"))).alias(f"lat_{h}"))
        agg_exprs.append(sf_max(when(col("age_h") == lit(h), col("lon"))).alias(f"lon_{h}"))
        agg_exprs.append(sf_max(when(col("age_h") == lit(h), col("alt_m"))).alias(f"alt_{h}"))

    grouped = df.groupBy("traj_id", "direction").agg(*agg_exprs)
    grouped = grouped.withColumn("spark_processed_at", current_timestamp())

    input_count = grouped.count()
    if input_count == 0:
        print("[INFO] No trajectories available for clustering.")
        return

    # Feature columns: lat then lon for each anchor hour
    feature_cols = [f"lat_{h}" for h in anchor_hours] + [f"lon_{h}" for h in anchor_hours]

    # Impute missing values (mean) into new _imp columns so original columns remain available
    imputed_cols = [c + "_imp" for c in feature_cols]
    imputer = Imputer(strategy="mean", inputCols=feature_cols, outputCols=imputed_cols)
    imputer_model = imputer.fit(grouped)
    imputed = imputer_model.transform(grouped)

    assembler = VectorAssembler(inputCols=imputed_cols, outputCol="raw_features")
    assembled = assembler.transform(imputed)

    scaler = StandardScaler(inputCol="raw_features", outputCol="features", withStd=True, withMean=False)
    scaler_model = scaler.fit(assembled)
    scaled = scaler_model.transform(assembled)
    # Persist scaled to avoid recomputing during k-sweep
    scaled = scaled.persist()

    # Compute time range for logging
    try:
        time_row = df.agg(sf_min(col("timestamp")), sf_max(col("timestamp"))).collect()[0]
        min_time = time_row[0]
        max_time = time_row[1]
        print(f"Trajectory time_range: {min_time} to {max_time}")
    except Exception:
        min_time = max_time = None

    # Sweep k to log WCSS, but use configured default for final model
    k_min = args.k_min
    k_max = args.k_max
    k_default = args.k_default if args.k_min <= args.k_default <= args.k_max else args.k_min

    print(f"[INFO] Running k-sweep {k_min}..{k_max} (will choose k_default={k_default} for final model)")
    wcss = []
    for k in range(k_min, k_max + 1):
        km = KMeans(k=k, featuresCol="features", seed=42, maxIter=40)
        model = km.fit(scaled)
        # trainingCost / summary may vary by Spark version; prefer summary.trainingCost
        try:
            cost = float(model.summary.trainingCost)
        except Exception:
            try:
                cost = float(model.computeCost(scaled.select("features")))
            except Exception:
                cost = float("nan")
        wcss.append((k, cost))
        print(f"[SWEEP] k={k} WCSS={cost}")

    # Fit final model
    print(f"[INFO] Fitting final KMeans with k={k_default}")
    final_km = KMeans(k=k_default, featuresCol="features", seed=42, maxIter=100)
    final_model = final_km.fit(scaled)
    predicted = final_model.transform(scaled).withColumnRenamed("prediction", "cluster_id")

    # Derive source_lat/lon: prefer original lat_0/lon_0 if present, else use imputed columns
    preferred_lat = "lat_0"
    preferred_lon = "lon_0"
    imp_lat = "lat_0_imp"
    imp_lon = "lon_0_imp"
    source_lat_col = preferred_lat if preferred_lat in predicted.columns else imp_lat
    source_lon_col = preferred_lon if preferred_lon in predicted.columns else imp_lon

    assignments = predicted.select(
        col("traj_id"),
        col("cluster_id"),
        col("direction"),
        col(source_lat_col).alias("source_lat"),
        col(source_lon_col).alias("source_lon"),
        col(f"alt_{anchor_hours[0]}").alias("source_alt_m"),
        col("spark_processed_at").alias("cluster_processed_at"),
    )

    out = (
        df.join(assignments, ["traj_id", "direction"], "inner")
        .select(
            col("traj_id"),
            col("cluster_id"),
            col("direction"),
            col("age_h"),
            col("lat"),
            col("lon"),
            col("alt_m"),
            col("timestamp"),
            col("source_lat"),
            col("source_lon"),
            col("source_alt_m"),
            col("cluster_processed_at").alias("spark_processed_at"),
        )
    )

    # Metrics and distribution
    output_count = out.count()
    dist = out.groupBy("cluster_id").count().orderBy("cluster_id").collect()
    print(f"Clustering summary: input_traj={input_count}, output_rows={output_count}")
    print("Cluster distribution:")
    for row in dist:
        print(f"  cluster={row['cluster_id']}: n={row['count']}")

    # Write to Iceberg
    if args.full_refresh:
        spark.sql(f"DELETE FROM {HYSPLIT_CLUSTER_SILVER_TABLE}")
    out.write.format("iceberg").mode("append").saveAsTable(HYSPLIT_CLUSTER_SILVER_TABLE)

    print(f"Wrote clustered trajectories to {HYSPLIT_CLUSTER_SILVER_TABLE}")


if __name__ == "__main__":
    main()
