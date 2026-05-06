# TODO 1 - Hanoi PM2.5 Silver/Gold Pipeline

Muc tieu phase nay: dua blueprint trong `description/hanoi_trajectory_pipeline.md` vao pipeline hien tai den het tang silver va gold de san sang train mo hinh PM2.5 Ha Noi.

Pham vi phase nay:

- Lam silver tables cho OpenAQ, WeatherAPI, ERA5 surface, Sentinel-5P, MAIAC.
- Lam gold master feature table va gold training dataset.
- Them ingest ERA5 de lay file raw/metadata vao he thong.
- Dung Spark lam transform chinh.
- Dung Iceberg lam medallion storage.


## 0. Nguyen Tac Thiet Ke

- Bronze giu du lieu gan raw nhat, append-only, co schema va partition.
- Silver lam clean, filter, dedupe, QC, aggregate theo domain.
- Gold lam feature engineering va training dataset.
- Tat ca output trung gian va training data luu vao Iceberg.
- Khong ghi local parquet kieu `clean/*.parquet` hay `output/*.parquet` trong production job.
- Moi Spark job phai doc input tu Iceberg/raw path va ghi output vao Iceberg table.
- Moi job nen support batch rerun theo khoang thoi gian:
  - `--start-date YYYY-MM-DD`
  - `--end-date YYYY-MM-DD`
  - `--full-refresh 0|1`
- Moi job can in ra count input/output va cac chi so validation co ban.

## 1. Config Chung

### Tao File: `config/hanoi_pipeline.yaml`

Noi dung can co:

```yaml
hanoi:
  bbox:
    west: 105.25
    east: 106.10
    south: 20.55
    north: 21.40
  center:
    lat: 21.0285
    lon: 105.8542

pm25_qc:
  min_value: 0.0
  max_value: 1000.0
  min_coverage_pct: 50.0

era5:
  region:
    west: 95.0
    east: 115.0
    south: 5.0
    north: 35.0
  raw_base_path: "hdfs://namenode:9000/raw/era5"
  surface_variables:
    - "10m_u_component_of_wind"
    - "10m_v_component_of_wind"
    - "boundary_layer_height"
    - "surface_pressure"
    - "2m_temperature"
    - "2m_dewpoint_temperature"
    - "total_precipitation"
    - "mean_sea_level_pressure"

sentinel5p:
  raw_base_path: "hdfs://namenode:9000/raw/sentinel5p"
  products:
    - "NO2"
    - "CO"
    - "SO2"
    - "O3"
    - "AER_AI"

maiac:
  raw_base_path: "hdfs://namenode:9000/raw/maiac"
  local_fallback_path: "crawler/maiac_data"
  scale_factor: 0.001
  bands:
    - "AOD_047"
    - "AOD_055"

gold:
  horizons_hours:
    - 6
    - 12
    - 24
  lag_hours:
    - 1
    - 3
    - 6
    - 12
    - 24
  rolling_hours:
    - 3
    - 6
    - 24
```

Ghi chu:

- Bbox co the dieu chinh sau khi validate station OpenAQ.
- Phase nay dung bbox thay GeoJSON.
- ERA5 pressure-level va HYSPLIT config de phase sau.

### Tao File: `spark_jobs/hanoi_config.py`

Viec can lam:

- Load `config/hanoi_pipeline.yaml`.
- Expose helper:
  - `load_config()`
  - `get_hanoi_bbox()`
  - `get_hanoi_center()`
  - `get_table_names()`
  - `filter_hanoi_bbox(df, lat_col, lon_col)`
- Khai bao table names tap trung:

```python
TABLES = {
    "openaq_bronze": "ais.air_quality.openaq_hourly_bronze",
    "weather_bronze": "ais.weather.weather_history_bronze",
    "sentinel5p_bronze": "ais.satellite.sentinel5p_summary_bronze",
    "maiac_bronze": "ais.satellite.maiac_summary_bronze",
    "era5_files_bronze": "ais.weather.era5_files_bronze",
    "openaq_station_silver": "ais.air_quality.openaq_hanoi_station_hourly_silver",
    "openaq_hourly_silver": "ais.air_quality.openaq_hanoi_hourly_silver",
    "weather_proxy_silver": "ais.weather.weather_hanoi_surface_proxy_silver",
    "era5_surface_silver": "ais.weather.era5_surface_hanoi_hourly_silver",
    "sentinel5p_silver": "ais.satellite.sentinel5p_hanoi_daily_silver",
    "maiac_silver": "ais.satellite.maiac_hanoi_daily_silver",
    "master_gold": "ais.features.hanoi_pm25_master_hourly_gold",
    "training_gold": "ais.features.hanoi_pm25_training_dataset_gold",
}
```

Acceptance criteria:

- Tat ca Spark job moi dung chung file config nay.
- Khong hardcode bbox/table name rai rac trong tung job.

## 2. Iceberg Tables

### Sua File: `spark_jobs/ensure_iceberg_tables.py`

Bo sung namespace:

```text
ais.features
ais.models
```

Bo sung table bronze moi:

```text
ais.weather.era5_files_bronze
```

Schema de xuat:

```sql
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
```

Partition:

```sql
PARTITIONED BY (dataset_type, year, month)
```

Bo sung silver tables:

```text
ais.air_quality.openaq_hanoi_station_hourly_silver
ais.air_quality.openaq_hanoi_hourly_silver
ais.weather.weather_hanoi_surface_proxy_silver
ais.weather.era5_surface_hanoi_hourly_silver
ais.satellite.sentinel5p_hanoi_daily_silver
ais.satellite.maiac_hanoi_daily_silver
```

Bo sung gold tables:

```text
ais.features.hanoi_pm25_master_hourly_gold
ais.features.hanoi_pm25_training_dataset_gold
ais.models.hanoi_pm25_model_runs_gold
```

Ghi chu:

- `ais.models.hanoi_pm25_model_runs_gold` co the tao o phase nay de chuan bi, nhung chua bat buoc ghi neu chua train.
- Khong tao trajectory tables trong phase nay.
- Khong tao Cassandra serving tables trong phase nay.

Acceptance criteria:

- Chay `bash scripts/submit_spark.sh ensure-iceberg` tao duoc tat ca table.
- `SHOW TABLES IN ais.features` tra ve gold tables.
- Rerun idempotent, khong loi neu table da ton tai.

## 3. ERA5 Ingest

### Tao File: `ingest/era5_ingest.py`

Muc dich:

- Download ERA5 surface data ve raw storage.
- Publish metadata event vao Kafka topic `era5-files`.
- Khong gui grid ERA5 lon qua Kafka.

Input:

- CDS API credentials tu environment.
- `config/hanoi_pipeline.yaml`.
- CLI args:
  - `--start-date`
  - `--end-date`
  - `--dataset-type surface`
  - `--output-base-path`

Surface variables:

```text
10m_u_component_of_wind
10m_v_component_of_wind
boundary_layer_height
surface_pressure
2m_temperature
2m_dewpoint_temperature
total_precipitation
mean_sea_level_pressure
```

Output raw file:

```text
hdfs://namenode:9000/raw/era5/surface/year=YYYY/month=MM/era5_surface_YYYYMM.nc
```

Kafka event schema:

```json
{
  "event_id": "era5_surface_2026_03",
  "dataset_type": "surface",
  "year": 2026,
  "month": 3,
  "start_time": "2026-03-01T00:00:00Z",
  "end_time": "2026-03-31T23:00:00Z",
  "bbox": [95.0, 5.0, 115.0, 35.0],
  "file_path": "hdfs://namenode:9000/raw/era5/surface/year=2026/month=03/era5_surface_202603.nc",
  "file_size": 123456789,
  "checksum": "...",
  "source": "era5_cds",
  "ingest_time": "..."
}
```

Acceptance criteria:

- Download duoc NetCDF surface theo thang.
- Kafka co event trong topic `era5-files`.
- Job co retry va skip neu file da ton tai hop le.

### Tao File: `spark_jobs/era5_files_streaming.py`

Muc dich:

- Consume Kafka topic `era5-files`.
- Ghi metadata file ERA5 vao Iceberg bronze.

Input:

```text
Kafka topic: era5-files
```

Output:

```text
ais.weather.era5_files_bronze
```

Transform:

- Parse JSON.
- Cast timestamp.
- Dedupe theo `event_id`.
- Them `spark_processed_at`.
- Ghi Iceberg.
- Support `--stop-after-batch 1`.

Acceptance criteria:

- Backfill chay one-shot duoc.
- Streaming mode chay duoc neu can.
- Bang `era5_files_bronze` co record tracking file raw.

## 4. OpenAQ Silver

### Tao File: `spark_jobs/hanoi_openaq_silver.py`

Muc dich:

- Tao station-hourly PM2.5 silver cho Ha Noi.
- Tao city-hourly PM2.5 target silver cho Ha Noi.

Input:

```text
ais.air_quality.openaq_hourly_bronze
```

Output:

```text
ais.air_quality.openaq_hanoi_station_hourly_silver
ais.air_quality.openaq_hanoi_hourly_silver
```

Transform station-hourly:

- Parse `event_time` thanh `hour`.
- Loc `parameter = 'pm25'`.
- Loc theo bbox Ha Noi:
  - `latitude between south and north`
  - `longitude between west and east`
- Bo record loi:
  - `value is null`
  - `value < min_value`
  - `value > max_value`
  - `event_time is null`
  - `coverage_pct < min_coverage_pct` neu `coverage_pct` co gia tri.
- Dedupe theo:

```text
location_id, sensor_id, parameter, hour
```

- Neu co nhieu record cung key, giu record co `ingest_time` moi nhat.
- Them partition columns `year`, `month`, `day`.

Schema station silver can co:

```text
hour TIMESTAMP
location_id BIGINT
location_name STRING
city STRING
latitude DOUBLE
longitude DOUBLE
provider STRING
sensor_id BIGINT
parameter STRING
unit STRING
pm25 DOUBLE
coverage_pct DOUBLE
source STRING
ingest_time TIMESTAMP
spark_processed_at TIMESTAMP
year INT
month INT
day INT
```

Transform Hanoi hourly:

- Group by `hour`.
- Tinh:

```text
pm25_median
pm25_mean
pm25_min
pm25_max
pm25_std
station_count
coverage_avg
```

Schema hourly silver can co:

```text
hour TIMESTAMP
pm25_median DOUBLE
pm25_mean DOUBLE
pm25_min DOUBLE
pm25_max DOUBLE
pm25_std DOUBLE
station_count INT
coverage_avg DOUBLE
year INT
month INT
day INT
spark_processed_at TIMESTAMP
```

Acceptance criteria:

- Khong con record ngoai bbox Ha Noi.
- Khong co duplicate theo station-hour.
- `openaq_hanoi_hourly_silver` co mot row moi gio neu co du lieu.
- Co log min/max/count/null ratio.

## 5. WeatherAPI Silver

### Tao File: `spark_jobs/hanoi_weather_surface_proxy_silver.py`

Muc dich:

- Lay cac bien WeatherAPI co ich cho aerosol/PM2.5 nhung khong trung vai tro ERA5.

Input:

```text
ais.weather.weather_history_bronze
```

Output:

```text
ais.weather.weather_hanoi_surface_proxy_silver
```

Transform:

- Loc Ha Noi:
  - uu tien `province/location_name` chua `Ha Noi`, `Hanoi`, `Ha_Noi`
  - fallback bbox bang `lat/lon` neu co.
- Chuan hoa `event_time` thanh `hour`.
- Dedupe theo `hour`.
- Chi giu:

```text
hour
vis_km
uv
condition_code
condition_text
is_day
will_it_rain
chance_of_rain
will_it_snow
chance_of_snow
source
ingest_time
spark_processed_at
year
month
day
```

Khong dung lam feature chinh neu da co ERA5:

```text
temp_c
wind_kph
wind_degree
pressure_mb
humidity
```

Ghi chu:

- Cac cot tren co the giu trong bronze, nhung silver proxy chi expose subset dung cho model.

Acceptance criteria:

- Mot row moi gio cho Ha Noi neu co du lieu.
- Khong duplicate theo `hour`.
- Null ratio cho `vis_km`, `uv`, `condition_code` duoc log.

## 6. ERA5 Surface Silver

### Tao File: `spark_jobs/era5_surface_hanoi_silver.py`

Muc dich:

- Doc raw ERA5 surface NetCDF.
- Extract surface meteorology cho Ha Noi.
- Tao table silver hourly dung cho feature engineering.

Input:

```text
ais.weather.era5_files_bronze
raw ERA5 surface NetCDF files
```

Output:

```text
ais.weather.era5_surface_hanoi_hourly_silver
```

Transform:

- Loc `dataset_type = 'surface'`.
- Doc tung NetCDF file.
- Flatten theo `time x latitude x longitude`.
- Loc grid theo bbox Ha Noi hoac lay nearest grid quanh `HANOI_CENTER`.
- Aggregate ve mot row moi gio cho Ha Noi:
  - neu dung bbox: average cac grid cell trong bbox.
  - neu dung nearest: chon grid gan center nhat.
- Chuan hoa units:
  - Kelvin sang Celsius neu can.
  - precipitation meter sang mm neu can.
- Tinh:

```text
hour
wind_u10
wind_v10
wind_speed
wind_dir
pbl_height_m
low_pbl
surface_pressure
temperature_2m_c
dewpoint_2m_c
total_precipitation_mm
mean_sea_level_pressure
grid_point_count
source_file
year
month
day
spark_processed_at
```

Cong thuc:

```text
wind_speed = sqrt(wind_u10^2 + wind_v10^2)
wind_dir = atan2-based meteorological direction, degree 0-360
low_pbl = pbl_height_m < 300
```

Luu y:

- Khong copy cong thuc wind direction dang draft trong markdown neu thay sai.
- Can validate output bang min/max wind speed va pbl height.

Acceptance criteria:

- Co hourly records theo range ERA5 ingest.
- Khong duplicate theo `hour`.
- `wind_speed >= 0`.
- `wind_dir` nam trong `[0, 360)`.
- `pbl_height_m` co gia tri hop ly va log null ratio.

## 7. Sentinel-5P Silver

### Tao File: `spark_jobs/sentinel5p_hanoi_silver.py`

Muc dich:

- Extract gia tri khoa hoc Sentinel-5P cho bbox Ha Noi.
- Tao daily satellite features cho gold.

Input:

```text
ais.satellite.sentinel5p_summary_bronze
raw Sentinel-5P NetCDF files
```

Output:

```text
ais.satellite.sentinel5p_hanoi_daily_silver
```

Transform:

- Map metadata bronze den raw NetCDF file path.
- Doc NetCDF theo product:
  - NO2
  - CO
  - SO2
  - O3
  - AER_AI
- Extract lat/lon/value/qa.
- Apply QA mask theo product.
- Clip bbox Ha Noi.
- Tinh daily stats theo `product, date`:

```text
product
date
overpass_time_utc
value_mean
value_min
value_max
value_std
valid_pixel_count
total_pixel_count
valid_pct
unit
source_file
year
month
day
spark_processed_at
```

Acceptance criteria:

- Khong chi copy metadata bronze. Phai doc NetCDF va tinh stats that.
- Co `valid_pct` de biet ngay nao satellite dung duoc.
- Co mot row moi `product/date` neu co pixel hop le.
- Product khong co pixel hop le van nen log ro, khong fail silent.

## 8. MAIAC Silver

### Tao File: `spark_jobs/maiac_hanoi_silver.py`

Muc dich:

- Extract MAIAC/MODIS AOD tu HDF.
- Tao daily AOD features cho gold.

Input:

```text
ais.satellite.maiac_summary_bronze
raw MAIAC HDF files
```

Raw fallback hien co:

```text
crawler/maiac_data/*.hdf
```

Output:

```text
ais.satellite.maiac_hanoi_daily_silver
```

Transform:

- Parse filename de lay:
  - product
  - acquisition date
  - tile
  - collection
  - processing timestamp
- Doc HDF4/HDF5 bang GDAL/rasterio/pyhdf.
- Lay bands:

```text
AOD_047
AOD_055
```

- Apply scale factor, mac dinh `0.001`.
- Apply QA mask.
- Reproject/geolocate sinusoidal grid sang lat/lon neu can.
- Clip bbox Ha Noi.
- Tinh daily stats:

```text
date
aod_047_mean
aod_055_mean
aod_mean
aod_min
aod_max
aod_std
valid_pixel_count
total_pixel_count
valid_pct
tile_count
source_files
year
month
day
spark_processed_at
```

Acceptance criteria:

- Dung duoc voi cac file HDF hien co trong `crawler/maiac_data`.
- Co output daily AOD.
- Log tile_count va valid_pct.
- Neu QA strict lam mat het pixel, can co warning va cau hinh relaxed QA ro rang.

## 9. Gold Master Feature Table

### Tao File: `spark_jobs/hanoi_pm25_master_features_gold.py`

Muc dich:

- Join toan bo silver tables thanh mot bang feature hourly.
- Moi row la mot gio cua Ha Noi.
- Bang nay la input chinh cho training.

Input:

```text
ais.air_quality.openaq_hanoi_hourly_silver
ais.weather.weather_hanoi_surface_proxy_silver
ais.weather.era5_surface_hanoi_hourly_silver
ais.satellite.sentinel5p_hanoi_daily_silver
ais.satellite.maiac_hanoi_daily_silver
```

Output:

```text
ais.features.hanoi_pm25_master_hourly_gold
```

Transform:

1. Tao hourly grid:

- Lay min/max hour tu OpenAQ target.
- Tao sequence hourly lien tuc.
- Left join OpenAQ vao grid.
- Muc dich: tranh `lag()` sai khi missing hour.

2. Join hourly features theo `hour`:

```text
OpenAQ hourly target
WeatherAPI proxy
ERA5 surface
```

3. Join daily satellite features:

- Tao `date = to_date(hour)`.
- Pivot Sentinel-5P product thanh cot:

```text
s5p_no2_mean
s5p_co_mean
s5p_so2_mean
s5p_o3_mean
s5p_aer_ai_mean
s5p_no2_valid_pct
s5p_aer_ai_valid_pct
```

- Join MAIAC:

```text
aod_047_mean
aod_055_mean
aod_mean
aod_max
aod_valid_pct
```

4. Tranh leakage satellite:

- Neu `hour` som hon `overpass_time_utc` cua cung ngay, khong dung satellite observation cua ngay do.
- Dung latest available satellite observation tai thoi diem feature.
- Neu chua implement full latest-as-of join, phai ghi ro trong code va docs.

5. Tao time features:

```text
hour_of_day
day_of_week
month
season
is_weekend
```

6. Tao PM2.5 lag features:

```text
pm25_lag_1h
pm25_lag_3h
pm25_lag_6h
pm25_lag_12h
pm25_lag_24h
```

7. Tao rolling features:

```text
pm25_roll_mean_3h
pm25_roll_mean_6h
pm25_roll_mean_24h
pm25_roll_max_24h
pm25_roll_std_24h
```

8. Tao targets:

```text
pm25_next_6h
pm25_next_12h
pm25_next_24h
```

9. Schema chinh can co:

```text
hour TIMESTAMP
pm25_median DOUBLE
pm25_mean DOUBLE
station_count INT
coverage_avg DOUBLE
vis_km DOUBLE
uv DOUBLE
condition_code INT
is_day INT
will_it_rain INT
chance_of_rain INT
wind_u10 DOUBLE
wind_v10 DOUBLE
wind_speed DOUBLE
wind_dir DOUBLE
pbl_height_m DOUBLE
low_pbl BOOLEAN
surface_pressure DOUBLE
temperature_2m_c DOUBLE
dewpoint_2m_c DOUBLE
total_precipitation_mm DOUBLE
s5p_no2_mean DOUBLE
s5p_co_mean DOUBLE
s5p_so2_mean DOUBLE
s5p_o3_mean DOUBLE
s5p_aer_ai_mean DOUBLE
aod_047_mean DOUBLE
aod_055_mean DOUBLE
aod_mean DOUBLE
aod_max DOUBLE
hour_of_day INT
day_of_week INT
month INT
season STRING
is_weekend BOOLEAN
pm25_lag_1h DOUBLE
pm25_lag_3h DOUBLE
pm25_lag_6h DOUBLE
pm25_lag_12h DOUBLE
pm25_lag_24h DOUBLE
pm25_roll_mean_3h DOUBLE
pm25_roll_mean_6h DOUBLE
pm25_roll_mean_24h DOUBLE
pm25_roll_max_24h DOUBLE
pm25_roll_std_24h DOUBLE
pm25_next_6h DOUBLE
pm25_next_12h DOUBLE
pm25_next_24h DOUBLE
year INT
month_partition INT
spark_processed_at TIMESTAMP
```

Acceptance criteria:

- Mot row moi gio trong hourly grid.
- Lag/rolling khong bi shift qua missing hour.
- Target future dung thoi gian thuc, khong random.
- Satellite khong leak tu tuong lai.
- Output co du feature de train baseline model.

## 10. Gold Training Dataset

### Tao File: `spark_jobs/hanoi_pm25_training_dataset_gold.py`

Muc dich:

- Tao dataset train/validation/test reproducible tu master feature table.

Input:

```text
ais.features.hanoi_pm25_master_hourly_gold
```

Output:

```text
ais.features.hanoi_pm25_training_dataset_gold
```

Transform:

- Chon danh sach feature columns chinh thuc.
- Drop rows thieu target tuong ung.
- Khong random split.
- Split theo thoi gian:
  - train: 70%
  - validation: 15%
  - test: 15%
- Gan:

```text
dataset_version
feature_set_name
split
created_at
```

Feature groups:

```text
aq_lag
weather_proxy
era5_surface
satellite_s5p
satellite_maiac
time
```

Targets:

```text
pm25_next_6h
pm25_next_12h
pm25_next_24h
```

Acceptance criteria:

- Dataset train co day du feature + target.
- Validation/test nam sau train theo thoi gian.
- Co `dataset_version` de train lai model cung mot snapshot.
- Log row count theo split va horizon.

## 11. Model Metadata Table

### Sua File: `spark_jobs/ensure_iceberg_tables.py`

Tao san table:

```text
ais.models.hanoi_pm25_model_runs_gold
```

Schema de xuat:

```text
model_run_id STRING
dataset_version STRING
feature_set_name STRING
horizon_hour INT
model_type STRING
model_path STRING
train_start TIMESTAMP
train_end TIMESTAMP
validation_start TIMESTAMP
validation_end TIMESTAMP
test_start TIMESTAMP
test_end TIMESTAMP
mae DOUBLE
rmse DOUBLE
mape DOUBLE
feature_importance_path STRING
created_at TIMESTAMP
```

Ghi chu:

- Phase nay co the chi tao table, chua can train model neu team chua toi buoc ML.
- Neu train baseline luon, tao file `ml/train_hanoi_pm25.py` o muc optional ben duoi.

## 12. Optional Sau Khi Gold San Sang: Training Script

### Tao File: `ml/train_hanoi_pm25.py`

Muc dich:

- Train baseline LightGBM/XGBoost tu gold training dataset.

Input:

```text
ais.features.hanoi_pm25_training_dataset_gold
```

Output:

```text
models/hanoi_pm25/lgb_pm25_6h.txt
models/hanoi_pm25/lgb_pm25_12h.txt
models/hanoi_pm25/lgb_pm25_24h.txt
ais.models.hanoi_pm25_model_runs_gold
```

Viec can lam:

- Train rieng cho moi horizon:
  - 6h
  - 12h
  - 24h
- Dung split co san trong gold dataset.
- Log metrics:
  - MAE
  - RMSE
  - MAPE neu hop ly
- Luu feature importance.
- Ghi metadata vao `hanoi_pm25_model_runs_gold`.

Acceptance criteria:

- Train duoc baseline model.
- Metrics ghi lai duoc.
- Model artifact co path ro rang.

## 13. Airflow DAGs

### Tao File: `airflow/dags/ais_era5_ingestion_dag.py`

Task flow:

```text
ensure_kafka_topic_era5_files
download_era5_surface
process_era5_files_to_iceberg
process_era5_surface_hanoi_silver
```

Ghi chu:

- `download_era5_surface` goi `ingest/era5_ingest.py`.
- `process_era5_files_to_iceberg` goi `spark_jobs/era5_files_streaming.py --stop-after-batch 1`.
- `process_era5_surface_hanoi_silver` goi `spark_jobs/era5_surface_hanoi_silver.py`.

### Tao File: `airflow/dags/ais_hanoi_silver_gold_dag.py`

Task flow:

```text
ensure_iceberg_tables
hanoi_openaq_silver
hanoi_weather_surface_proxy_silver
era5_surface_hanoi_silver
sentinel5p_hanoi_silver
maiac_hanoi_silver
hanoi_pm25_master_features_gold
hanoi_pm25_training_dataset_gold
```

Dependency:

```text
ensure_iceberg_tables >> [
  hanoi_openaq_silver,
  hanoi_weather_surface_proxy_silver,
  era5_surface_hanoi_silver,
  sentinel5p_hanoi_silver,
  maiac_hanoi_silver
]

[
  hanoi_openaq_silver,
  hanoi_weather_surface_proxy_silver,
  era5_surface_hanoi_silver,
  sentinel5p_hanoi_silver,
  maiac_hanoi_silver
] >> hanoi_pm25_master_features_gold

hanoi_pm25_master_features_gold >> hanoi_pm25_training_dataset_gold
```

Acceptance criteria:

- DAG chay duoc end-to-end cho mot khoang ngay nho.
- Moi task co log input/output count.
- Rerun khong tao duplicate neu chay cung khoang ngay.

## 14. Submit Script

### Sua File: `scripts/submit_spark.sh`

Them job cases:

```text
era5-files
era5-surface-hanoi-silver
hanoi-openaq-silver
hanoi-weather-silver
sentinel5p-hanoi-silver
maiac-hanoi-silver
hanoi-master-features-gold
hanoi-training-dataset-gold
```

Map job file:

```text
era5-files -> /opt/spark-jobs/era5_files_streaming.py
era5-surface-hanoi-silver -> /opt/spark-jobs/era5_surface_hanoi_silver.py
hanoi-openaq-silver -> /opt/spark-jobs/hanoi_openaq_silver.py
hanoi-weather-silver -> /opt/spark-jobs/hanoi_weather_surface_proxy_silver.py
sentinel5p-hanoi-silver -> /opt/spark-jobs/sentinel5p_hanoi_silver.py
maiac-hanoi-silver -> /opt/spark-jobs/maiac_hanoi_silver.py
hanoi-master-features-gold -> /opt/spark-jobs/hanoi_pm25_master_features_gold.py
hanoi-training-dataset-gold -> /opt/spark-jobs/hanoi_pm25_training_dataset_gold.py
```

Acceptance criteria:

- Moi job co the submit bang mot command.
- Job streaming metadata co support `STOP_AFTER_BATCH=true`.
- Job batch silver/gold co the truyen date range.

## 15. Dependencies Va Docker

### Sua File: `ingest/requirements.txt`

Them neu chua co:

```text
cdsapi
pyyaml
```

### Cap Nhat Spark Runtime

Can dam bao Spark container co cac dependency sau:

```text
pyyaml
xarray
netCDF4
h5netcdf
h5py
rasterio
GDAL
pyproj
shapely
numpy
pandas
scikit-learn
lightgbm
xgboost
```

Ghi chu:

- MAIAC HDF4 co the can `pyhdf` hoac GDAL build co HDF4 support.
- Neu cai GDAL trong Spark image kho, co the tach satellite extraction thanh container/job rieng roi ghi Iceberg.
- Phase nay chua can HYSPLIT binary.

Acceptance criteria:

- Spark job doc duoc NetCDF ERA5.
- Spark/satellite job doc duoc Sentinel-5P NetCDF.
- MAIAC job doc duoc HDF hien co.

## 16. Data Quality Checks

Moi silver/gold job can log toi thieu:

```text
input_count
output_count
duplicate_count
min_time
max_time
null_ratio_by_important_columns
```

OpenAQ checks:

```text
pm25_min
pm25_max
station_count_by_day
records_outside_bbox
```

Weather checks:

```text
vis_km_null_ratio
uv_null_ratio
condition_code_null_ratio
```

ERA5 checks:

```text
wind_speed_min
wind_speed_max
wind_dir_min
wind_dir_max
pbl_height_null_ratio
```

Satellite checks:

```text
valid_pct_by_product_date
valid_pixel_count
source_file_count
```

Gold checks:

```text
feature_row_count
target_non_null_count_by_horizon
lag_null_count_by_lag
train_validation_test_counts
```
## Out Of Scope Phase Nay

Khong lam trong `TODO_1`:

```text
GeoJSON silver
district-level aggregation
HYSPLIT install
ERA5 pressure-level to ARL
trajectory run plan
HYSPLIT CONTROL files
trajectory parse
trajectory features
trajectory satellite sampling
Cassandra serving forecast
UI map/plume rendering
```

Ly do:

- Phase nay can hoan thanh silver/gold de train model truoc.
- GeoJSON khong bat buoc neu chi loc Ha Noi bang bbox.
- Trajectory/HYSPLIT se la `TODO_2`.
- Cassandra chi can sau khi co forecast/inference output.

