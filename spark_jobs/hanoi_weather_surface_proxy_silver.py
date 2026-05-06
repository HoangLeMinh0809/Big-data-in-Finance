from __future__ import annotations

import argparse
import os

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from hanoi_config import ICEBERG_CATALOG, ICEBERG_WAREHOUSE, get_hanoi_bbox, get_table_names


OUTPUT_COLUMNS = [
    "hour",
    "vis_km",
    "uv",
    "condition_code",
    "condition_text",
    "is_day",
    "will_it_rain",
    "chance_of_rain",
    "will_it_snow",
    "chance_of_snow",
    "source",
    "ingest_time",
    "spark_processed_at",
    "year",
    "month",
    "day",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Hanoi WeatherAPI surface proxy silver table")
    parser.add_argument("--start-date", default=os.getenv("START_DATE", ""))
    parser.add_argument("--end-date", default=os.getenv("END_DATE", ""))
    parser.add_argument("--full-refresh", default=os.getenv("FULL_REFRESH", "0"))
    return parser.parse_args()


def as_bool(raw: str) -> bool:
    return str(raw or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("HanoiWeatherSurfaceProxySilver")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.warehouse", ICEBERG_WAREHOUSE)
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
        .getOrCreate()
    )


def ensure_table(spark: SparkSession, table_name: str) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ICEBERG_CATALOG}.weather")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            hour TIMESTAMP,
            vis_km DOUBLE,
            uv DOUBLE,
            condition_code INT,
            condition_text STRING,
            is_day INT,
            will_it_rain INT,
            chance_of_rain INT,
            will_it_snow INT,
            chance_of_snow INT,
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


def apply_date_range(df, start_date: str, end_date: str):
    if start_date:
        df = df.filter(F.to_date("event_time") >= F.to_date(F.lit(start_date)))
    if end_date:
        df = df.filter(F.to_date("event_time") <= F.to_date(F.lit(end_date)))
    return df


def build_silver_df(spark: SparkSession, source_table: str, start_date: str, end_date: str):
    bbox = get_hanoi_bbox()
    raw = spark.table(source_table).filter(F.col("event_time").isNotNull())
    raw = apply_date_range(raw, start_date, end_date)

    location_text = F.lower(
        F.concat_ws(" ", F.col("province"), F.col("region"), F.col("location_name"))
    )
    name_match = location_text.rlike(r"hanoi|ha noi|ha_noi")
    bbox_match = (
        F.col("lat").isNotNull()
        & F.col("lon").isNotNull()
        & F.col("lat").between(bbox["south"], bbox["north"])
        & F.col("lon").between(bbox["west"], bbox["east"])
    )

    hanoi = raw.filter(name_match | bbox_match)
    with_hour = hanoi.withColumn("hour", F.date_trunc("hour", F.col("event_time")))

    duplicate_count = (
        with_hour.groupBy("hour")
        .count()
        .filter(F.col("count") > 1)
        .select(F.sum(F.col("count") - F.lit(1)).alias("duplicate_count"))
        .first()["duplicate_count"]
    )
    duplicate_count = int(duplicate_count or 0)

    window = Window.partitionBy("hour").orderBy(
        F.col("ingest_time").desc_nulls_last(),
        F.col("spark_processed_at").desc_nulls_last(),
        F.col("event_id").desc_nulls_last(),
    )

    silver = (
        with_hour
        .withColumn("rn", F.row_number().over(window))
        .filter(F.col("rn") == 1)
        .withColumn("spark_processed_at", F.current_timestamp())
        .withColumn("year", F.year("hour"))
        .withColumn("month", F.month("hour"))
        .withColumn("day", F.dayofmonth("hour"))
        .select(*OUTPUT_COLUMNS)
    )
    return raw, hanoi, silver, duplicate_count


def log_metrics(raw, hanoi, silver, duplicate_count: int) -> None:
    input_count = raw.count()
    hanoi_count = hanoi.count()
    output_count = silver.count()
    time_bounds = silver.agg(F.min("hour").alias("min_time"), F.max("hour").alias("max_time")).first()

    null_cols = ["vis_km", "uv", "condition_code"]
    null_exprs = [
        F.avg(F.when(F.col(c).isNull(), F.lit(1.0)).otherwise(F.lit(0.0))).alias(c)
        for c in null_cols
    ]
    null_ratios = silver.agg(*null_exprs).first().asDict() if output_count else {c: None for c in null_cols}

    print(f"input_count={input_count}")
    print(f"hanoi_filtered_count={hanoi_count}")
    print(f"duplicate_count={duplicate_count}")
    print(f"output_count={output_count}")
    print(f"min_time={time_bounds['min_time'] if time_bounds else None}")
    print(f"max_time={time_bounds['max_time'] if time_bounds else None}")
    print(f"null_ratio_by_important_columns={null_ratios}")


def write_iceberg(spark: SparkSession, df, table_name: str, full_refresh: bool) -> None:
    if full_refresh:
        spark.sql(f"DELETE FROM {table_name}")

    df.createOrReplaceTempView("weather_hanoi_surface_proxy_silver_updates")
    assignments = ", ".join([f"t.{c} = s.{c}" for c in OUTPUT_COLUMNS])
    insert_cols = ", ".join(OUTPUT_COLUMNS)
    insert_vals = ", ".join([f"s.{c}" for c in OUTPUT_COLUMNS])

    spark.sql(
        f"""
        MERGE INTO {table_name} t
        USING weather_hanoi_surface_proxy_silver_updates s
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

    target_table = os.getenv("ICEBERG_TABLE", tables["weather_proxy_silver"])
    source_table = os.getenv("SOURCE_ICEBERG_TABLE", tables["weather_bronze"])

    ensure_table(spark, target_table)
    raw, hanoi, silver, duplicate_count = build_silver_df(
        spark,
        source_table=source_table,
        start_date=args.start_date,
        end_date=args.end_date,
    )
    log_metrics(raw, hanoi, silver, duplicate_count)
    write_iceberg(spark, silver, target_table, full_refresh=as_bool(args.full_refresh))
    print(f"Saved: {target_table}")
    spark.stop()


if __name__ == "__main__":
    main()
