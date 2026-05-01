from __future__ import annotations

import os

from pyspark.sql import SparkSession

ICEBERG_CATALOG = os.getenv("ICEBERG_CATALOG", "ais")
ICEBERG_WAREHOUSE = os.getenv("ICEBERG_WAREHOUSE", "hdfs://namenode:9000/warehouse/iceberg")


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("AIS_EnsureIcebergTables")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.warehouse", ICEBERG_WAREHOUSE)
        .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
        .getOrCreate()
    )


def ensure_tables(spark: SparkSession) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ICEBERG_CATALOG}.weather")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ICEBERG_CATALOG}.air_quality")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ICEBERG_CATALOG}.satellite")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ICEBERG_CATALOG}.features")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ICEBERG_CATALOG}.models")

    # ---------------------------------------------------------------------
    # BRONZE
    # ---------------------------------------------------------------------
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {ICEBERG_CATALOG}.weather.weather_history_bronze (
            event_id STRING,
            province STRING,
            country STRING,
            region STRING,
            location_name STRING,
            lat DOUBLE,
            lon DOUBLE,
            tz_id STRING,
            query_date DATE,
            time STRING,
            event_time TIMESTAMP,
            time_epoch BIGINT,
            is_day INT,
            temp_c DOUBLE,
            temp_f DOUBLE,
            feelslike_c DOUBLE,
            feelslike_f DOUBLE,
            windchill_c DOUBLE,
            windchill_f DOUBLE,
            heatindex_c DOUBLE,
            heatindex_f DOUBLE,
            dewpoint_c DOUBLE,
            dewpoint_f DOUBLE,
            condition_text STRING,
            condition_code INT,
            condition_icon STRING,
            wind_mph DOUBLE,
            wind_kph DOUBLE,
            wind_degree INT,
            wind_dir STRING,
            gust_mph DOUBLE,
            gust_kph DOUBLE,
            pressure_mb DOUBLE,
            pressure_in DOUBLE,
            precip_mm DOUBLE,
            precip_in DOUBLE,
            snow_cm DOUBLE,
            humidity INT,
            cloud INT,
            vis_km DOUBLE,
            vis_miles DOUBLE,
            uv DOUBLE,
            will_it_rain INT,
            chance_of_rain INT,
            will_it_snow INT,
            chance_of_snow INT,
            source STRING,
            source_file STRING,
            ingest_time TIMESTAMP,
            window_mode STRING,
            window_start_utc TIMESTAMP,
            window_end_utc TIMESTAMP,
            window_now_utc TIMESTAMP,
            spark_processed_at TIMESTAMP,
            year INT,
            month INT
        )
        USING ICEBERG
        PARTITIONED BY (year, month)
        TBLPROPERTIES ('format-version'='2')
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {ICEBERG_CATALOG}.air_quality.openaq_hourly_bronze (
            location_id BIGINT,
            location_name STRING,
            city STRING,
            latitude DOUBLE,
            longitude DOUBLE,
            provider STRING,
            sensor_id BIGINT,
            parameter STRING,
            unit STRING,
            datetime_utc STRING,
            datetime_local STRING,
            value DOUBLE,
            min DOUBLE,
            max DOUBLE,
            sd DOUBLE,
            expected_count BIGINT,
            observed_count BIGINT,
            coverage_pct DOUBLE,
            source STRING,
            ingest_time TIMESTAMP,
            window_mode STRING,
            window_start_utc TIMESTAMP,
            window_end_utc TIMESTAMP,
            window_now_utc TIMESTAMP,
            event_id STRING,
            event_time TIMESTAMP,
            spark_processed_at TIMESTAMP,
            year INT,
            month INT,
            day INT,
            hour INT
        )
        USING ICEBERG
        PARTITIONED BY (year, month, day, hour)
        TBLPROPERTIES ('format-version'='2')
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {ICEBERG_CATALOG}.satellite.sentinel5p_summary_bronze (
            product STRING,
            collection STRING,
            content_start STRING,
            content_end STRING,
            bbox ARRAY<DOUBLE>,
            product_name STRING,
            product_id STRING,
            file_name STRING,
            stats_min DOUBLE,
            stats_max DOUBLE,
            stats_mean DOUBLE,
            stats_valid_pct DOUBLE,
            unit STRING,
            ingest_time TIMESTAMP,
            window_mode STRING,
            window_start_utc TIMESTAMP,
            window_end_utc TIMESTAMP,
            window_now_utc TIMESTAMP,
            event_id STRING,
            source STRING,
            event_time TIMESTAMP,
            spark_processed_at TIMESTAMP,
            year INT,
            month INT,
            day INT
        )
        USING ICEBERG
        PARTITIONED BY (product, year, month, day)
        TBLPROPERTIES ('format-version'='2')
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {ICEBERG_CATALOG}.satellite.maiac_summary_bronze (
            event_id STRING,
            granule_id STRING,
            granule_name STRING,
            producer_granule_id STRING,
            short_name STRING,
            version STRING,
            tile STRING,
            acquisition_date DATE,
            time_start STRING,
            time_end STRING,
            updated STRING,
            download_url STRING,
            bbox ARRAY<DOUBLE>,
            source STRING,
            ingest_time TIMESTAMP,
            window_mode STRING,
            window_start_utc TIMESTAMP,
            window_end_utc TIMESTAMP,
            window_now_utc TIMESTAMP,
            event_time TIMESTAMP,
            spark_processed_at TIMESTAMP,
            year INT,
            month INT,
            day INT
        )
        USING ICEBERG
        PARTITIONED BY (short_name, year, month, day, tile)
        TBLPROPERTIES ('format-version'='2')
        """
    )

    # New bronze table: ERA5 raw file metadata
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {ICEBERG_CATALOG}.weather.era5_files_bronze (
            event_id STRING,
            dataset_type STRING,
            year INT,
            month INT,
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            bbox ARRAY<DOUBLE>,
            file_path STRING,
            file_size BIGINT,
            checksum STRING,
            source STRING,
            ingest_time TIMESTAMP,
            spark_processed_at TIMESTAMP
        )
        USING ICEBERG
        PARTITIONED BY (dataset_type, year, month)
        TBLPROPERTIES ('format-version'='2')
        """
    )

    # ---------------------------------------------------------------------
    # SILVER (schemas are minimal placeholders; will be refined in later TODOs)
    # ---------------------------------------------------------------------
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {ICEBERG_CATALOG}.air_quality.openaq_hanoi_station_hourly_silver (
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
        CREATE TABLE IF NOT EXISTS {ICEBERG_CATALOG}.air_quality.openaq_hanoi_hourly_silver (
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

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {ICEBERG_CATALOG}.weather.weather_hanoi_surface_proxy_silver (
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

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {ICEBERG_CATALOG}.weather.era5_surface_hanoi_hourly_silver (
            hour TIMESTAMP,
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
            mean_sea_level_pressure DOUBLE,
            grid_point_count INT,
            source_file STRING,
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

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {ICEBERG_CATALOG}.satellite.sentinel5p_hanoi_daily_silver (
            product STRING,
            date DATE,
            overpass_time_utc TIMESTAMP,
            value_mean DOUBLE,
            value_min DOUBLE,
            value_max DOUBLE,
            value_std DOUBLE,
            valid_pixel_count BIGINT,
            total_pixel_count BIGINT,
            valid_pct DOUBLE,
            unit STRING,
            source_file STRING,
            year INT,
            month INT,
            day INT,
            spark_processed_at TIMESTAMP
        )
        USING ICEBERG
        PARTITIONED BY (product, year, month, day)
        TBLPROPERTIES ('format-version'='2')
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {ICEBERG_CATALOG}.satellite.maiac_hanoi_daily_silver (
            date DATE,
            aod_047_mean DOUBLE,
            aod_055_mean DOUBLE,
            aod_mean DOUBLE,
            aod_min DOUBLE,
            aod_max DOUBLE,
            aod_std DOUBLE,
            valid_pixel_count BIGINT,
            total_pixel_count BIGINT,
            valid_pct DOUBLE,
            tile_count INT,
            source_files STRING,
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

    # ---------------------------------------------------------------------
    # GOLD (schemas are minimal placeholders; will be refined in later TODOs)
    # ---------------------------------------------------------------------
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {ICEBERG_CATALOG}.features.hanoi_pm25_master_hourly_gold (
            hour TIMESTAMP,
            pm25_median DOUBLE,
            pm25_mean DOUBLE,
            station_count INT,
            coverage_avg DOUBLE,
            year INT,
            month_partition INT,
            spark_processed_at TIMESTAMP
        )
        USING ICEBERG
        PARTITIONED BY (year, month_partition)
        TBLPROPERTIES ('format-version'='2')
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {ICEBERG_CATALOG}.features.hanoi_pm25_training_dataset_gold (
            dataset_version STRING,
            feature_set_name STRING,
            split STRING,
            created_at TIMESTAMP,
            hour TIMESTAMP,
            pm25_next_6h DOUBLE,
            pm25_next_12h DOUBLE,
            pm25_next_24h DOUBLE,
            spark_processed_at TIMESTAMP,
            year INT,
            month_partition INT
        )
        USING ICEBERG
        PARTITIONED BY (year, month_partition)
        TBLPROPERTIES ('format-version'='2')
        """
    )

    # NOTE: Per request, do NOT create {ICEBERG_CATALOG}.models.hanoi_pm25_model_runs_gold yet.


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    ensure_tables(spark)
    print(
        "Ensured Iceberg namespaces and tables for weather/air_quality/satellite + Hanoi silver/gold placeholders"
    )
    spark.stop()


if __name__ == "__main__":
    main()
