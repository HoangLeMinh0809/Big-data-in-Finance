from __future__ import annotations

from pyspark.sql import SparkSession

from hanoi_config import ICEBERG_CATALOG, ICEBERG_WAREHOUSE, TABLES


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


def create_namespaces(spark: SparkSession) -> None:
    for namespace in ["weather", "air_quality", "satellite", "features", "models", "trajectory"]:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ICEBERG_CATALOG}.{namespace}")


def ensure_columns(spark: SparkSession, table_name: str, columns: dict[str, str]) -> None:
    existing = set(spark.table(table_name).columns)
    for column, dtype in columns.items():
        if column not in existing:
            spark.sql(f"ALTER TABLE {table_name} ADD COLUMN {column} {dtype}")


def ensure_tables(spark: SparkSession) -> None:
    create_namespaces(spark)

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLES["weather_bronze"]} (
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
        CREATE TABLE IF NOT EXISTS {TABLES["openaq_bronze"]} (
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
        CREATE TABLE IF NOT EXISTS {TABLES["sentinel5p_bronze"]} (
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
            day INT,
            download_url STRING,
            content_length BIGINT,
            s3_path STRING,
            raw_file_path STRING,
            raw_downloaded BOOLEAN,
            raw_download_error STRING
        )
        USING ICEBERG
        PARTITIONED BY (product, year, month, day)
        TBLPROPERTIES ('format-version'='2')
        """
    )
    existing_s5p_columns = set(spark.table(TABLES["sentinel5p_bronze"]).columns)
    for column_name, column_type in [
        ("download_url", "STRING"),
        ("content_length", "BIGINT"),
        ("s3_path", "STRING"),
        ("raw_file_path", "STRING"),
        ("raw_downloaded", "BOOLEAN"),
        ("raw_download_error", "STRING"),
    ]:
        if column_name not in existing_s5p_columns:
            spark.sql(f"ALTER TABLE {TABLES['sentinel5p_bronze']} ADD COLUMN {column_name} {column_type}")

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLES["maiac_bronze"]} (
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

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLES["era5_files_bronze"]} (
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
            spark_processed_at TIMESTAMP,
            surface_file_path STRING,
            surface_file_size BIGINT,
            surface_checksum STRING
        )
        USING ICEBERG
        PARTITIONED BY (dataset_type, year, month)
        TBLPROPERTIES ('format-version'='2')
        """
    )
    for column_name, column_type in [
        ("surface_file_path", "STRING"),
        ("surface_file_size", "BIGINT"),
        ("surface_checksum", "STRING"),
    ]:
        try:
            spark.sql(f"ALTER TABLE {TABLES['era5_files_bronze']} ADD COLUMN {column_name} {column_type}")
        except Exception:
            pass

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLES["openaq_station_silver"]} (
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
        CREATE TABLE IF NOT EXISTS {TABLES["openaq_hourly_silver"]} (
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
        CREATE TABLE IF NOT EXISTS {TABLES["weather_proxy_silver"]} (
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
        CREATE TABLE IF NOT EXISTS {TABLES["era5_surface_silver"]} (
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
        CREATE TABLE IF NOT EXISTS {TABLES["sentinel5p_silver"]} (
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
        CREATE TABLE IF NOT EXISTS {TABLES["maiac_silver"]} (
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
            source_files ARRAY<STRING>,
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
        CREATE TABLE IF NOT EXISTS {TABLES["master_gold"]} (
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
            hour_of_day INT,
            day_of_week INT,
            month INT,
            season STRING,
            is_weekend BOOLEAN,
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

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLES["training_gold"]} (
            dataset_version STRING,
            feature_set_name STRING,
            split STRING,
            feature_groups ARRAY<STRING>,
            created_at TIMESTAMP,
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
            hour_of_day INT,
            day_of_week INT,
            month INT,
            season STRING,
            is_weekend BOOLEAN,
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

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLES["era5_arl_bronze"]} (
            dataset_type STRING,
            year INT,
            month INT,
            source_nc STRING,
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            arl_path STRING,
            checksum STRING,
            created_at TIMESTAMP,
            spark_processed_at TIMESTAMP
        )
        USING ICEBERG
        PARTITIONED BY (dataset_type, year, month)
        TBLPROPERTIES ('format-version'='2')
        """
    )
    ensure_columns(
        spark,
        TABLES["era5_arl_bronze"],
        {"start_time": "TIMESTAMP", "end_time": "TIMESTAMP"},
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLES["hysplit_runs_bronze"]} (
            run_id STRING,
            direction STRING,
            init_time TIMESTAMP,
            duration_hours INT,
            init_lat DOUBLE,
            init_lon DOUBLE,
            init_alt_m DOUBLE,
            arl_path STRING,
            output_path STRING,
            status STRING,
            error_message STRING,
            spark_processed_at TIMESTAMP
        )
        USING ICEBERG
        PARTITIONED BY (direction)
        TBLPROPERTIES ('format-version'='2')
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLES["hysplit_traj_silver"]} (
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

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLES["hysplit_cluster_silver"]} (
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

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLES["openaq_gradient_silver"]} (
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

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLES["s5p_grid_silver"]} (
            product STRING,
            date DATE,
            lat DOUBLE,
            lon DOUBLE,
            value DOUBLE,
            valid_pct DOUBLE,
            source_file STRING,
            year INT,
            month INT,
            day INT,
            spark_processed_at TIMESTAMP
        )
        USING ICEBERG
        PARTITIONED BY (product, year, month)
        TBLPROPERTIES ('format-version'='2')
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLES["trajectory_path_silver"]} (
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

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLES["trajectory_hourly_silver"]} (
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

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {TABLES["model_runs_gold"]} (
            model_run_id STRING,
            dataset_version STRING,
            feature_set_name STRING,
            horizon_hour INT,
            model_type STRING,
            model_path STRING,
            train_start TIMESTAMP,
            train_end TIMESTAMP,
            validation_start TIMESTAMP,
            validation_end TIMESTAMP,
            test_start TIMESTAMP,
            test_end TIMESTAMP,
            mae DOUBLE,
            rmse DOUBLE,
            mape DOUBLE,
            feature_importance_path STRING,
            created_at TIMESTAMP
        )
        USING ICEBERG
        TBLPROPERTIES ('format-version'='2')
        """
    )


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    ensure_tables(spark)
    print("Ensured Iceberg namespaces and TODO_1/TODO_2 bronze/silver/gold/model tables")
    spark.stop()


if __name__ == "__main__":
    main()
