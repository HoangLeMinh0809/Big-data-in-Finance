from __future__ import annotations

import argparse
import os

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from hanoi_config import (
    ICEBERG_CATALOG,
    ICEBERG_WAREHOUSE,
    filter_hanoi_bbox,
    get_pm25_qc,
    get_table_names,
)


STATION_COLUMNS = [
    "hour",
    "location_id",
    "location_name",
    "city",
    "latitude",
    "longitude",
    "provider",
    "sensor_id",
    "parameter",
    "unit",
    "pm25",
    "coverage_pct",
    "source",
    "ingest_time",
    "spark_processed_at",
    "year",
    "month",
    "day",
]

HOURLY_COLUMNS = [
    "hour",
    "pm25_median",
    "pm25_mean",
    "pm25_min",
    "pm25_max",
    "pm25_std",
    "station_count",
    "coverage_avg",
    "year",
    "month",
    "day",
    "spark_processed_at",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Hanoi OpenAQ PM2.5 silver tables")
    parser.add_argument("--start-date", default=os.getenv("START_DATE", ""))
    parser.add_argument("--end-date", default=os.getenv("END_DATE", ""))
    parser.add_argument("--full-refresh", default=os.getenv("FULL_REFRESH", "0"))
    return parser.parse_args()


def as_bool(raw: str) -> bool:
    return str(raw or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("HanoiOpenAQSilver")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.warehouse", ICEBERG_WAREHOUSE)
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
        .getOrCreate()
    )


def ensure_tables(spark: SparkSession, station_table: str, hourly_table: str) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ICEBERG_CATALOG}.air_quality")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {station_table} (
            hour TIMESTAMP,
            location_id BIGINT,
            location_name STRING,
            city STRING,
            latitude DOUBLE,
            longitude DOUBLE,
            provider STRING,
            sensor_id BIGINT,
            parameter STRING,
            unit STRING,
            pm25 DOUBLE,
            coverage_pct DOUBLE,
            source STRING,
            ingest_time TIMESTAMP,
            spark_processed_at TIMESTAMP,
            year INT,
            month INT,
            day INT
        )
        USING ICEBERG
        PARTITIONED BY (year, month, day)
        TBLPROPERTIES ('format-version'='2')
        """
    )
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {hourly_table} (
            hour TIMESTAMP,
            pm25_median DOUBLE,
            pm25_mean DOUBLE,
            pm25_min DOUBLE,
            pm25_max DOUBLE,
            pm25_std DOUBLE,
            station_count INT,
            coverage_avg DOUBLE,
            year INT,
            month INT,
            day INT,
            spark_processed_at TIMESTAMP
        )
        USING ICEBERG
        PARTITIONED BY (year, month, day)
        TBLPROPERTIES ('format-version'='2')
        """
    )


def apply_date_range(df, start_date: str, end_date: str):
    if start_date:
        df = df.filter(F.to_date("event_time") >= F.to_date(F.lit(start_date)))
    if end_date:
        df = df.filter(F.to_date("event_time") <= F.to_date(F.lit(end_date)))
    return df


def build_station_silver(spark: SparkSession, source_table: str, start_date: str, end_date: str):
    qc = get_pm25_qc()
    raw = spark.table(source_table).filter(F.col("event_time").isNotNull())
    raw = apply_date_range(raw, start_date, end_date)

    pm25 = (
        raw
        .withColumn("parameter_norm", F.regexp_replace(F.lower(F.col("parameter")), r"[^a-z0-9]", ""))
        .filter(F.col("parameter_norm") == F.lit("pm25"))
        .filter(F.col("value").isNotNull())
        .filter(F.col("value").between(qc["min_value"], qc["max_value"]))
        .filter(F.col("coverage_pct").isNull() | (F.col("coverage_pct") >= F.lit(qc["min_coverage_pct"])))
        .withColumn("hour", F.date_trunc("hour", F.col("event_time")))
    )

    hanoi = filter_hanoi_bbox(pm25, "latitude", "longitude")
    outside_bbox_count = pm25.count() - hanoi.count()

    duplicate_count = (
        hanoi.groupBy("location_id", "sensor_id", "parameter", "hour")
        .count()
        .filter(F.col("count") > 1)
        .select(F.sum(F.col("count") - F.lit(1)).alias("duplicate_count"))
        .first()["duplicate_count"]
    )
    duplicate_count = int(duplicate_count or 0)

    window = Window.partitionBy("location_id", "sensor_id", "parameter", "hour").orderBy(
        F.col("ingest_time").desc_nulls_last(),
        F.col("spark_processed_at").desc_nulls_last(),
        F.col("event_id").desc_nulls_last(),
    )

    station = (
        hanoi
        .withColumn("rn", F.row_number().over(window))
        .filter(F.col("rn") == 1)
        .withColumn("pm25", F.col("value").cast("double"))
        .withColumn("parameter", F.lit("pm25"))
        .withColumn("spark_processed_at", F.current_timestamp())
        .withColumn("year", F.year("hour"))
        .withColumn("month", F.month("hour"))
        .withColumn("day", F.dayofmonth("hour"))
        .select(*STATION_COLUMNS)
    )
    return raw, pm25, hanoi, station, duplicate_count, outside_bbox_count


def build_hourly_silver(station_df):
    return (
        station_df
        .groupBy("hour")
        .agg(
            F.expr("percentile_approx(pm25, 0.5)").cast("double").alias("pm25_median"),
            F.avg("pm25").cast("double").alias("pm25_mean"),
            F.min("pm25").cast("double").alias("pm25_min"),
            F.max("pm25").cast("double").alias("pm25_max"),
            F.stddev_samp("pm25").cast("double").alias("pm25_std"),
            F.countDistinct(F.concat_ws(":", F.col("location_id"), F.col("sensor_id"))).cast("int").alias("station_count"),
            F.avg("coverage_pct").cast("double").alias("coverage_avg"),
        )
        .withColumn("year", F.year("hour"))
        .withColumn("month", F.month("hour"))
        .withColumn("day", F.dayofmonth("hour"))
        .withColumn("spark_processed_at", F.current_timestamp())
        .select(*HOURLY_COLUMNS)
    )


def log_metrics(raw, pm25, hanoi, station, hourly, duplicate_count: int, outside_bbox_count: int) -> None:
    input_count = raw.count()
    pm25_candidate_count = pm25.count()
    hanoi_count = hanoi.count()
    station_count = station.count()
    output_count = hourly.count()
    bounds = station.agg(F.min("hour").alias("min_time"), F.max("hour").alias("max_time")).first()
    pm25_bounds = station.agg(F.min("pm25").alias("pm25_min"), F.max("pm25").alias("pm25_max")).first()
    null_ratio = station.agg(
        F.avg(F.when(F.col("pm25").isNull(), F.lit(1.0)).otherwise(F.lit(0.0))).alias("pm25"),
        F.avg(F.when(F.col("coverage_pct").isNull(), F.lit(1.0)).otherwise(F.lit(0.0))).alias("coverage_pct"),
    ).first().asDict() if station_count else {"pm25": None, "coverage_pct": None}

    print(f"input_count={input_count}")
    print(f"pm25_candidate_count={pm25_candidate_count}")
    print(f"hanoi_filtered_count={hanoi_count}")
    print(f"station_output_count={station_count}")
    print(f"output_count={output_count}")
    print(f"duplicate_count={duplicate_count}")
    print(f"records_outside_bbox={outside_bbox_count}")
    print(f"min_time={bounds['min_time'] if bounds else None}")
    print(f"max_time={bounds['max_time'] if bounds else None}")
    print(f"pm25_min={pm25_bounds['pm25_min'] if pm25_bounds else None}")
    print(f"pm25_max={pm25_bounds['pm25_max'] if pm25_bounds else None}")
    print(f"null_ratio_by_important_columns={null_ratio}")
    station.groupBy(F.to_date("hour").alias("date")).agg(F.countDistinct("location_id").alias("station_count")).show(50, False)


def merge_iceberg(spark: SparkSession, df, table_name: str, view_name: str, key_expr: str, columns: list[str], full_refresh: bool) -> None:
    if full_refresh:
        spark.sql(f"DELETE FROM {table_name}")

    df.createOrReplaceTempView(view_name)
    assignments = ", ".join([f"t.{c} = s.{c}" for c in columns])
    insert_cols = ", ".join(columns)
    insert_vals = ", ".join([f"s.{c}" for c in columns])
    spark.sql(
        f"""
        MERGE INTO {table_name} t
        USING {view_name} s
        ON {key_expr}
        WHEN MATCHED THEN UPDATE SET {assignments}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
        """
    )


def main() -> None:
    args = parse_args()
    tables = get_table_names()
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    source_table = os.getenv("SOURCE_ICEBERG_TABLE", tables["openaq_bronze"])
    station_table = os.getenv("STATION_ICEBERG_TABLE", tables["openaq_station_silver"])
    hourly_table = os.getenv("ICEBERG_TABLE", tables["openaq_hourly_silver"])
    full_refresh = as_bool(args.full_refresh)

    ensure_tables(spark, station_table, hourly_table)
    raw, pm25, hanoi, station, duplicate_count, outside_bbox_count = build_station_silver(
        spark,
        source_table=source_table,
        start_date=args.start_date,
        end_date=args.end_date,
    )
    hourly = build_hourly_silver(station)
    log_metrics(raw, pm25, hanoi, station, hourly, duplicate_count, outside_bbox_count)

    merge_iceberg(
        spark,
        station,
        station_table,
        "openaq_station_silver_updates",
        "t.location_id <=> s.location_id AND t.sensor_id <=> s.sensor_id AND t.parameter = s.parameter AND t.hour = s.hour",
        STATION_COLUMNS,
        full_refresh=full_refresh,
    )
    merge_iceberg(
        spark,
        hourly,
        hourly_table,
        "openaq_hourly_silver_updates",
        "t.hour = s.hour",
        HOURLY_COLUMNS,
        full_refresh=full_refresh,
    )
    print(f"Saved station silver: {station_table}")
    print(f"Saved hourly silver: {hourly_table}")
    spark.stop()


if __name__ == "__main__":
    main()
