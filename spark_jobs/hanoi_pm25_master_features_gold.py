from __future__ import annotations

import argparse
import os
import math

from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as F

from hanoi_config import (
    ICEBERG_CATALOG,
    ICEBERG_WAREHOUSE,
    get_gold_horizons_hours,
    get_gold_lag_hours,
    get_gold_rolling_hours,
    get_table_names,
)


OUTPUT_COLUMNS = [
    "hour",
    "pm25_median",
    "pm25_mean",
    "station_count",
    "coverage_avg",
    "vis_km",
    "uv",
    "condition_code",
    "is_day",
    "will_it_rain",
    "chance_of_rain",
    "wind_u10",
    "wind_v10",
    "wind_speed",
    "wind_dir",
    "pbl_height_m",
    "low_pbl",
    "surface_pressure",
    "temperature_2m_c",
    "dewpoint_2m_c",
    "total_precipitation_mm",
    "s5p_no2_mean",
    "s5p_co_mean",
    "s5p_so2_mean",
    "s5p_o3_mean",
    "s5p_aer_ai_mean",
    "s5p_no2_valid_pct",
    "s5p_aer_ai_valid_pct",
    "aod_047_mean",
    "aod_055_mean",
    "aod_mean",
    "aod_max",
    "aod_valid_pct",
    # New Tier-2 features
    "pm25_grad_n",
    "pm25_grad_s",
    "pm25_grad_e",
    "pm25_grad_w",
    "pm25_spatial_std",
    "pm25_grad_mag",
    "dominant_cluster",
    "n_traj",
    "traj_source_lat",
    "traj_source_lon",
    "traj_path_no2_mean",
    "traj_path_aer_mean",
    "traj_path_no2_aer_ratio",
    # Existing time features
    "hour_of_day",
    "day_of_week",
    "month",
    "season",
    "is_weekend",
    # New sin/cos + rush hour
    "hour_sin",
    "hour_cos",
    "dow_sin",
    "dow_cos",
    "month_sin",
    "month_cos",
    "is_rush_hour",
    "pm25_lag_1h",
    "pm25_lag_3h",
    "pm25_lag_6h",
    "pm25_lag_12h",
    "pm25_lag_24h",
    "pm25_roll_mean_3h",
    "pm25_roll_mean_6h",
    "pm25_roll_mean_24h",
    "pm25_roll_max_24h",
    "pm25_roll_std_24h",
    "pm25_next_6h",
    "pm25_next_12h",
    "pm25_next_24h",
    "year",
    "month_partition",
    "spark_processed_at",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Hanoi PM2.5 master feature gold table")
    parser.add_argument("--start-date", default=os.getenv("START_DATE", ""))
    parser.add_argument("--end-date", default=os.getenv("END_DATE", ""))
    parser.add_argument("--full-refresh", default=os.getenv("FULL_REFRESH", "0"))
    return parser.parse_args()


def as_bool(raw: str) -> bool:
    return str(raw or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("HanoiPM25MasterFeaturesGold")
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
            pm25_median DOUBLE,
            pm25_mean DOUBLE,
            station_count INT,
            coverage_avg DOUBLE,
            vis_km DOUBLE,
            uv DOUBLE,
            condition_code INT,
            is_day INT,
            will_it_rain INT,
            chance_of_rain INT,
            wind_u10 DOUBLE,
            wind_v10 DOUBLE,
            wind_speed DOUBLE,
            wind_dir DOUBLE,
            pbl_height_m DOUBLE,
            low_pbl BOOLEAN,
            surface_pressure DOUBLE,
            temperature_2m_c DOUBLE,
            dewpoint_2m_c DOUBLE,
            total_precipitation_mm DOUBLE,
            s5p_no2_mean DOUBLE,
            s5p_co_mean DOUBLE,
            s5p_so2_mean DOUBLE,
            s5p_o3_mean DOUBLE,
            s5p_aer_ai_mean DOUBLE,
            s5p_no2_valid_pct DOUBLE,
            s5p_aer_ai_valid_pct DOUBLE,
            aod_047_mean DOUBLE,
            aod_055_mean DOUBLE,
            aod_mean DOUBLE,
            aod_max DOUBLE,
            aod_valid_pct DOUBLE,
            pm25_grad_n DOUBLE,
            pm25_grad_s DOUBLE,
            pm25_grad_e DOUBLE,
            pm25_grad_w DOUBLE,
            pm25_spatial_std DOUBLE,
            pm25_grad_mag DOUBLE,
            dominant_cluster INT,
            n_traj INT,
            traj_source_lat DOUBLE,
            traj_source_lon DOUBLE,
            traj_path_no2_mean DOUBLE,
            traj_path_aer_mean DOUBLE,
            traj_path_no2_aer_ratio DOUBLE,
            hour_of_day INT,
            day_of_week INT,
            month INT,
            season STRING,
            is_weekend BOOLEAN,
            hour_sin DOUBLE,
            hour_cos DOUBLE,
            dow_sin DOUBLE,
            dow_cos DOUBLE,
            month_sin DOUBLE,
            month_cos DOUBLE,
            is_rush_hour BOOLEAN,
            pm25_lag_1h DOUBLE,
            pm25_lag_3h DOUBLE,
            pm25_lag_6h DOUBLE,
            pm25_lag_12h DOUBLE,
            pm25_lag_24h DOUBLE,
            pm25_roll_mean_3h DOUBLE,
            pm25_roll_mean_6h DOUBLE,
            pm25_roll_mean_24h DOUBLE,
            pm25_roll_max_24h DOUBLE,
            pm25_roll_std_24h DOUBLE,
            pm25_next_6h DOUBLE,
            pm25_next_12h DOUBLE,
            pm25_next_24h DOUBLE,
            year INT,
            month_partition INT,
            spark_processed_at TIMESTAMP
        )
        USING ICEBERG
        PARTITIONED BY (year, month_partition)
        TBLPROPERTIES ('format-version'='2')
        """
    )


def apply_date_range(df, time_col: str, start_date: str, end_date: str):
    if start_date:
        df = df.filter(F.to_date(time_col) >= F.to_date(F.lit(start_date)))
    if end_date:
        df = df.filter(F.to_date(time_col) <= F.to_date(F.lit(end_date)))
    return df


def build_hour_grid(aq):
    bounds = aq.agg(F.min("hour").alias("min_hour"), F.max("hour").alias("max_hour")).first()
    if not bounds or bounds["min_hour"] is None or bounds["max_hour"] is None:
        return None
    return aq.sparkSession.range(1).select(
        F.explode(F.sequence(F.lit(bounds["min_hour"]), F.lit(bounds["max_hour"]), F.expr("interval 1 hour"))).alias("hour")
    )


def build_s5p_asof_features(hours, s5p):
    s5p_norm = (
        s5p
        .withColumn("product_norm", F.upper(F.col("product")))
        .withColumn("product_norm", F.when(F.col("product_norm") == "AER", F.lit("AER_AI")).otherwise(F.col("product_norm")))
        .select("product_norm", "date", "overpass_time_utc", "value_mean", "valid_pct")
    )
    candidates = (
        hours.select("hour", F.to_date("hour").alias("hour_date"))
        .join(
            s5p_norm,
            (F.col("date") < F.col("hour_date"))
            | ((F.col("date") == F.col("hour_date")) & (F.col("overpass_time_utc").isNotNull()) & (F.col("overpass_time_utc") <= F.col("hour"))),
            "left",
        )
    )
    w = Window.partitionBy("hour", "product_norm").orderBy(F.col("date").desc_nulls_last(), F.col("overpass_time_utc").desc_nulls_last())
    latest = candidates.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1)
    return (
        latest
        .groupBy("hour")
        .agg(
            F.max(F.when(F.col("product_norm") == "NO2", F.col("value_mean"))).alias("s5p_no2_mean"),
            F.max(F.when(F.col("product_norm") == "CO", F.col("value_mean"))).alias("s5p_co_mean"),
            F.max(F.when(F.col("product_norm") == "SO2", F.col("value_mean"))).alias("s5p_so2_mean"),
            F.max(F.when(F.col("product_norm") == "O3", F.col("value_mean"))).alias("s5p_o3_mean"),
            F.max(F.when(F.col("product_norm") == "AER_AI", F.col("value_mean"))).alias("s5p_aer_ai_mean"),
            F.max(F.when(F.col("product_norm") == "NO2", F.col("valid_pct"))).alias("s5p_no2_valid_pct"),
            F.max(F.when(F.col("product_norm") == "AER_AI", F.col("valid_pct"))).alias("s5p_aer_ai_valid_pct"),
        )
    )


def build_maiac_asof_features(hours, maiac):
    candidates = (
        hours.select("hour", F.to_date("hour").alias("hour_date"))
        .join(maiac, F.col("date") < F.col("hour_date"), "left")
    )
    w = Window.partitionBy("hour").orderBy(F.col("date").desc_nulls_last())
    latest = candidates.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1)
    return latest.select(
        "hour",
        "aod_047_mean",
        "aod_055_mean",
        "aod_mean",
        "aod_max",
        F.col("valid_pct").alias("aod_valid_pct"),
    )


def add_time_lag_target_features(df):
    order_w = Window.orderBy("hour")
    df = (
        df
        .withColumn("hour_of_day", F.hour("hour").cast("int"))
        .withColumn("day_of_week", F.dayofweek("hour").cast("int"))
        .withColumn("month", F.month("hour").cast("int"))
        .withColumn(
            "season",
            F.when(F.col("month").isin(12, 1, 2), F.lit("winter"))
            .when(F.col("month").isin(3, 4, 5), F.lit("spring"))
            .when(F.col("month").isin(6, 7, 8), F.lit("summer"))
            .otherwise(F.lit("autumn")),
        )
        .withColumn("is_weekend", F.dayofweek("hour").isin(1, 7))
        .withColumn("is_rush_hour", F.col("hour_of_day").isin([7, 8, 9, 17, 18, 19]))
        .withColumn("hour_sin", F.sin(F.lit(2.0 * math.pi) * (F.col("hour_of_day") / F.lit(24.0))))
        .withColumn("hour_cos", F.cos(F.lit(2.0 * math.pi) * (F.col("hour_of_day") / F.lit(24.0))))
        .withColumn("dow_sin", F.sin(F.lit(2.0 * math.pi) * (F.col("day_of_week") / F.lit(7.0))))
        .withColumn("dow_cos", F.cos(F.lit(2.0 * math.pi) * (F.col("day_of_week") / F.lit(7.0))))
        .withColumn("month_sin", F.sin(F.lit(2.0 * math.pi) * (F.col("month") / F.lit(12.0))))
        .withColumn("month_cos", F.cos(F.lit(2.0 * math.pi) * (F.col("month") / F.lit(12.0))))
    )

    for lag in get_gold_lag_hours():
        df = df.withColumn(f"pm25_lag_{lag}h", F.lag("pm25_mean", lag).over(order_w))

    for window_hours in get_gold_rolling_hours():
        roll_w = order_w.rowsBetween(-(window_hours - 1), 0)
        df = df.withColumn(f"pm25_roll_mean_{window_hours}h", F.avg("pm25_mean").over(roll_w))
        if window_hours == 24:
            df = df.withColumn("pm25_roll_max_24h", F.max("pm25_mean").over(roll_w))
            df = df.withColumn("pm25_roll_std_24h", F.stddev_samp("pm25_mean").over(roll_w))

    for horizon in get_gold_horizons_hours():
        df = df.withColumn(f"pm25_next_{horizon}h", F.lead("pm25_mean", horizon).over(order_w))

    return (
        df
        .withColumn("year", F.year("hour").cast("int"))
        .withColumn("month_partition", F.month("hour").cast("int"))
        .withColumn("spark_processed_at", F.current_timestamp())
    )


def build_master(spark: SparkSession, tables: dict[str, str], target_table: str, start_date: str, end_date: str):
    aq = apply_date_range(spark.table(tables["openaq_hourly_silver"]), "hour", start_date, end_date)
    hours = build_hour_grid(aq)
    if hours is None:
        print("warning=no_openaq_target_hours")
        return spark.table(target_table).limit(0)

    weather = apply_date_range(spark.table(tables["weather_proxy_silver"]), "hour", start_date, end_date)
    era5 = apply_date_range(spark.table(tables["era5_surface_silver"]), "hour", start_date, end_date)
    s5p = spark.table(tables["sentinel5p_silver"])
    maiac = spark.table(tables["maiac_silver"])

    s5p_features = build_s5p_asof_features(hours, s5p)
    maiac_features = build_maiac_asof_features(hours, maiac)

    gradient = spark.table(tables["openaq_gradient_silver"]).select(
        "hour",
        F.col("pm25_grad_n").alias("pm25_grad_n"),
        F.col("pm25_grad_s").alias("pm25_grad_s"),
        F.col("pm25_grad_e").alias("pm25_grad_e"),
        F.col("pm25_grad_w").alias("pm25_grad_w"),
        F.col("pm25_spatial_std").alias("pm25_spatial_std"),
        F.col("pm25_grad_mag").alias("pm25_grad_mag"),
    )
    gradient = apply_date_range(gradient, "hour", start_date, end_date)

    traj_hourly = spark.table(tables["trajectory_hourly_silver"]).select(
        "hour",
        F.col("dominant_cluster").cast("int").alias("dominant_cluster"),
        F.col("n_traj").cast("int").alias("n_traj"),
        F.col("source_lat").cast("double").alias("traj_source_lat"),
        F.col("source_lon").cast("double").alias("traj_source_lon"),
        F.col("path_no2_mean").cast("double").alias("traj_path_no2_mean"),
        F.col("path_aer_mean").cast("double").alias("traj_path_aer_mean"),
        F.col("path_no2_aer_ratio").cast("double").alias("traj_path_no2_aer_ratio"),
    )
    traj_hourly = apply_date_range(traj_hourly, "hour", start_date, end_date)

    weather_cols = ["hour", "vis_km", "uv", "condition_code", "is_day", "will_it_rain", "chance_of_rain"]
    era5_cols = [
        "hour",
        "wind_u10",
        "wind_v10",
        "wind_speed",
        "wind_dir",
        "pbl_height_m",
        "low_pbl",
        "surface_pressure",
        "temperature_2m_c",
        "dewpoint_2m_c",
        "total_precipitation_mm",
    ]

    base = (
        hours
        .join(aq.select("hour", "pm25_median", "pm25_mean", "station_count", "coverage_avg"), "hour", "left")
        .join(weather.select(*weather_cols), "hour", "left")
        .join(era5.select(*era5_cols), "hour", "left")
        .join(s5p_features, "hour", "left")
        .join(maiac_features, "hour", "left")
        .join(gradient, "hour", "left")
        .join(traj_hourly, "hour", "left")
    )
    return add_time_lag_target_features(base).select(*OUTPUT_COLUMNS)


def log_metrics(df) -> None:
    count = df.count()
    bounds = df.agg(F.min("hour").alias("min_time"), F.max("hour").alias("max_time")).first()
    target_counts = df.agg(
        F.sum(F.when(F.col("pm25_next_6h").isNotNull(), F.lit(1)).otherwise(F.lit(0))).alias("pm25_next_6h"),
        F.sum(F.when(F.col("pm25_next_12h").isNotNull(), F.lit(1)).otherwise(F.lit(0))).alias("pm25_next_12h"),
        F.sum(F.when(F.col("pm25_next_24h").isNotNull(), F.lit(1)).otherwise(F.lit(0))).alias("pm25_next_24h"),
    ).first().asDict() if count else {}
    lag_nulls = df.agg(
        F.sum(F.when(F.col("pm25_lag_1h").isNull(), F.lit(1)).otherwise(F.lit(0))).alias("pm25_lag_1h"),
        F.sum(F.when(F.col("pm25_lag_24h").isNull(), F.lit(1)).otherwise(F.lit(0))).alias("pm25_lag_24h"),
    ).first().asDict() if count else {}
    print(f"feature_row_count={count}")
    print(f"output_count={count}")
    print(f"duplicate_count=0")
    print(f"min_time={bounds['min_time'] if bounds else None}")
    print(f"max_time={bounds['max_time'] if bounds else None}")
    print(f"target_non_null_count_by_horizon={target_counts}")
    print(f"lag_null_count_by_lag={lag_nulls}")


def write_iceberg(spark: SparkSession, df, table_name: str, full_refresh: bool) -> None:
    if full_refresh:
        spark.sql(f"DELETE FROM {table_name}")
    df.createOrReplaceTempView("hanoi_pm25_master_updates")
    assignments = ", ".join([f"t.{c} = s.{c}" for c in OUTPUT_COLUMNS])
    insert_cols = ", ".join(OUTPUT_COLUMNS)
    insert_vals = ", ".join([f"s.{c}" for c in OUTPUT_COLUMNS])
    spark.sql(
        f"""
        MERGE INTO {table_name} t
        USING hanoi_pm25_master_updates s
        ON t.hour = s.hour
        WHEN MATCHED THEN UPDATE SET {assignments}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
        """
    )


def main() -> None:
    args = parse_args()
    tables = get_table_names()
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    target_table = os.getenv("ICEBERG_TABLE", tables["master_gold"])

    ensure_table(spark, target_table)
    master = build_master(spark, tables, target_table, args.start_date, args.end_date)
    log_metrics(master)
    write_iceberg(spark, master, target_table, full_refresh=as_bool(args.full_refresh))
    print(f"Saved: {target_table}")
    spark.stop()


if __name__ == "__main__":
    main()
