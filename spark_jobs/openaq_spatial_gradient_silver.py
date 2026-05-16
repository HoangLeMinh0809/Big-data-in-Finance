from __future__ import annotations

import argparse
import os
from math import sqrt

import pandas as pd
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StructField,
    StructType,
    TimestampType,
)

from hanoi_config import (
    ICEBERG_CATALOG,
    ICEBERG_WAREHOUSE,
    get_hanoi_center,
    get_table_names,
)


OUTPUT_SCHEMA = StructType(
    [
        StructField("hour", TimestampType(), False),
        StructField("pm25_grad_n", DoubleType(), True),
        StructField("pm25_grad_s", DoubleType(), True),
        StructField("pm25_grad_e", DoubleType(), True),
        StructField("pm25_grad_w", DoubleType(), True),
        StructField("pm25_spatial_std", DoubleType(), True),
        StructField("pm25_grad_mag", DoubleType(), True),
        StructField("spark_processed_at", TimestampType(), False),
        StructField("year", IntegerType(), False),
        StructField("month", IntegerType(), False),
    ]
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build OpenAQ spatial gradient silver table")
    parser.add_argument("--start-date", default=os.getenv("START_DATE", ""))
    parser.add_argument("--end-date", default=os.getenv("END_DATE", ""))
    parser.add_argument("--full-refresh", default=os.getenv("FULL_REFRESH", "0"))
    return parser.parse_args()


def as_bool(raw: str) -> bool:
    return str(raw or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("OpenAQSpatialGradientSilver")
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
            pm25_grad_n DOUBLE,
            pm25_grad_s DOUBLE,
            pm25_grad_e DOUBLE,
            pm25_grad_w DOUBLE,
            pm25_spatial_std DOUBLE,
            pm25_grad_mag DOUBLE,
            spark_processed_at TIMESTAMP,
            year INT,
            month INT
        )
        USING ICEBERG
        PARTITIONED BY (year, month)
        TBLPROPERTIES ('format-version'='2')
        """
    )


def apply_date_range(df, start_date: str, end_date: str):
    if start_date:
        df = df.filter(F.to_date("hour") >= F.to_date(F.lit(start_date)))
    if end_date:
        df = df.filter(F.to_date("hour") <= F.to_date(F.lit(end_date)))
    return df


def _inverse_distance_weighted(points: pd.DataFrame, target_lat: float, target_lon: float, k: int = 3) -> float | None:
    if points.empty:
        return None

    distances = ((points["latitude"] - target_lat) ** 2 + (points["longitude"] - target_lon) ** 2) ** 0.5
    nearest = points.assign(_dist=distances).sort_values("_dist").head(min(k, len(points)))
    if nearest.empty:
        return None

    weights = 1.0 / nearest["_dist"].clip(lower=1e-6)
    numerator = float((weights * nearest["pm25"]).sum())
    denominator = float(weights.sum())
    if denominator == 0.0:
        return None
    return numerator / denominator


def compute_spatial_gradient_factory(center_lat: float, center_lon: float):
    offset = 0.25

    def compute_spatial_gradient(pdf: pd.DataFrame) -> pd.DataFrame:
        hour = pdf["hour"].iloc[0] if not pdf.empty else None
        valid = pdf.dropna(subset=["latitude", "longitude", "pm25"]).copy()
        if valid["location_id"].nunique() < 3:
            return pd.DataFrame(
                [
                    {
                        "hour": hour,
                        "pm25_grad_n": None,
                        "pm25_grad_s": None,
                        "pm25_grad_e": None,
                        "pm25_grad_w": None,
                        "pm25_spatial_std": None,
                        "pm25_grad_mag": None,
                        "spark_processed_at": pd.Timestamp.utcnow().tz_localize(None),
                        "year": int(pd.Timestamp(hour).year) if hour is not None else 1970,
                        "month": int(pd.Timestamp(hour).month) if hour is not None else 1,
                    }
                ]
            )

        pm25_n = _inverse_distance_weighted(valid, center_lat + offset, center_lon)
        pm25_s = _inverse_distance_weighted(valid, center_lat - offset, center_lon)
        pm25_e = _inverse_distance_weighted(valid, center_lat, center_lon + offset)
        pm25_w = _inverse_distance_weighted(valid, center_lat, center_lon - offset)

        grad_mag = None
        if None not in {pm25_n, pm25_s, pm25_e, pm25_w}:
            grad_mag = sqrt((pm25_n - pm25_s) ** 2 + (pm25_e - pm25_w) ** 2)

        hour_ts = pd.Timestamp(hour)
        return pd.DataFrame(
            [
                {
                    "hour": hour,
                    "pm25_grad_n": pm25_n,
                    "pm25_grad_s": pm25_s,
                    "pm25_grad_e": pm25_e,
                    "pm25_grad_w": pm25_w,
                    "pm25_spatial_std": float(valid["pm25"].std(ddof=1)) if len(valid) > 1 else 0.0,
                    "pm25_grad_mag": grad_mag,
                    "spark_processed_at": pd.Timestamp.utcnow().tz_localize(None),
                    "year": int(hour_ts.year),
                    "month": int(hour_ts.month),
                }
            ]
        )

    return compute_spatial_gradient


def build_output_df(spark: SparkSession, source_table: str, start_date: str, end_date: str):
    station = spark.table(source_table)
    station = apply_date_range(station, start_date, end_date)
    station = station.filter(F.col("hour").isNotNull())

    duplicate_count_row = (
        station.groupBy("hour", "location_id", "sensor_id")
        .count()
        .filter(F.col("count") > 1)
        .select(F.sum(F.col("count") - F.lit(1)).alias("duplicate_count"))
        .first()
    )
    duplicate_count = int((duplicate_count_row["duplicate_count"] if duplicate_count_row else 0) or 0)

    center = get_hanoi_center()
    gradient_fn = compute_spatial_gradient_factory(center["lat"], center["lon"])
    output = station.groupBy("hour").applyInPandas(gradient_fn, schema=OUTPUT_SCHEMA)
    return station, output, duplicate_count


def merge_iceberg(spark: SparkSession, df, table_name: str, full_refresh: bool) -> None:
    if full_refresh:
        spark.sql(f"DELETE FROM {table_name}")

    df.createOrReplaceTempView("openaq_gradient_updates")
    spark.sql(
        f"""
        MERGE INTO {table_name} t
        USING openaq_gradient_updates s
        ON t.hour = s.hour
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )


def log_metrics(station_df, output_df, duplicate_count: int) -> None:
    input_count = station_df.count()
    output_count = output_df.count()
    bounds = station_df.agg(F.min("hour").alias("min_time"), F.max("hour").alias("max_time")).first()
    null_ratio = output_df.agg(
        F.avg(F.when(F.col("pm25_grad_mag").isNull(), F.lit(1.0)).otherwise(F.lit(0.0))).alias("pm25_grad_mag"),
        F.avg(F.when(F.col("pm25_spatial_std").isNull(), F.lit(1.0)).otherwise(F.lit(0.0))).alias("pm25_spatial_std"),
    ).first()
    coverage = output_df.agg(
        F.min("pm25_grad_mag").alias("pm25_grad_mag_min"),
        F.max("pm25_grad_mag").alias("pm25_grad_mag_max"),
    ).first()

    print(f"input_count={input_count}")
    print(f"output_count={output_count}")
    print(f"duplicate_count={duplicate_count}")
    print(f"min_time={bounds['min_time'] if bounds else None}")
    print(f"max_time={bounds['max_time'] if bounds else None}")
    print(f"pm25_grad_mag_min={coverage['pm25_grad_mag_min'] if coverage else None}")
    print(f"pm25_grad_mag_max={coverage['pm25_grad_mag_max'] if coverage else None}")
    print(
        "null_ratio="
        f"{{'pm25_grad_mag': {null_ratio['pm25_grad_mag'] if null_ratio else None}, "
        f"'pm25_spatial_std': {null_ratio['pm25_spatial_std'] if null_ratio else None}}}"
    )


def main() -> None:
    args = parse_args()
    tables = get_table_names()
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    source_table = os.getenv("SOURCE_ICEBERG_TABLE", tables["openaq_station_silver"])
    target_table = os.getenv("ICEBERG_TABLE", tables["openaq_gradient_silver"])
    full_refresh = as_bool(args.full_refresh)

    ensure_table(spark, target_table)
    station_df, output_df, duplicate_count = build_output_df(
        spark,
        source_table=source_table,
        start_date=args.start_date,
        end_date=args.end_date,
    )
    log_metrics(station_df, output_df, duplicate_count)
    merge_iceberg(spark, output_df, target_table, full_refresh=full_refresh)
    print(f"Saved: {target_table}")
    spark.stop()


if __name__ == "__main__":
    main()
