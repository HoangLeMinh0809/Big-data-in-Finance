# TODO 2 - Tier 2 Trajectory (HYSPLIT + Feature mở rộng + Log huấn luyện)

Mục tiêu pha này: triển khai **Tier 2** đúng blueprint trong [description/hanoi_trajectory_pipeline.md], bám theo kiến trúc hệ thống (Ingest -> Kafka -> Spark -> Iceberg), và **cập nhật feature + training** để có log kết quả huấn luyện. **Chưa làm trực quan hóa** (không vẽ map, không export GeoJSON/CSV cho UI).

## 0. Nguyên tắc thiết kế

- Bám kiến trúc hiện tại: Ingest -> Kafka -> Spark -> Iceberg; batch job đọc Iceberg/Raw và ghi lại Iceberg.
- Không ghi file local kiểu `clean/*.parquet` hay `output/*.parquet` trong job production.
- Mọi job batch phải hỗ trợ:
  - `--start-date YYYY-MM-DD`
  - `--end-date YYYY-MM-DD`
  - `--full-refresh 0|1`
- Mọi job phải in log:
  - `input_count`, `output_count`, `duplicate_count`, `min_time`, `max_time`.
- Tránh data leakage khi join satellite theo ngày; dùng logic “as-of” theo giờ overpass.

## 1. Cấu hình chung (mở rộng)

### Sửa file: `config/hanoi_pipeline.yaml`

Bổ sung cấu hình Tier 2:

```yaml
hysplit:
  backward_hours: 72
  forward_hours: 24
  backward_altitudes_m: [100, 500, 1000]
  forward_altitudes_m: [50, 200, 500]
  init_offsets_deg:
    lat: [-0.2, 0.0, 0.2]
    lon: [-0.2, 0.0, 0.2]
  run_hours_utc: [0, 6, 12, 18]
  pm25_trigger_threshold: 75  # chỉ chạy backward cho giờ ô nhiễm cao

trajectory:
  anchor_hours: [0, -6, -12, -24, -36, -48, -60, -72]
  cluster_k_min: 3
  cluster_k_max: 10
  cluster_k_default: 6

sampling:
  path_window_start_h: -72
  path_window_end_h: -24
  max_distance_deg: 0.5

era5:
  pressure_level_variables:
    - u_component_of_wind
    - v_component_of_wind
    - vertical_velocity
    - geopotential
    - temperature
    - specific_humidity
  pressure_levels:
    - 1000
    - 925
    - 850
    - 700
    - 600
    - 500
    - 400
  pressure_levels_time_utc: ["00:00", "06:00", "12:00", "18:00"]
```

### Sửa file: `spark_jobs/hanoi_config.py`

- Expose helper mới:
  - `get_hysplit_config()`
  - `get_trajectory_config()`
  - `get_sampling_config()`
  - `get_era5_pressure_levels()`
  - `get_era5_pressure_level_variables()`

- Bổ sung table names:

```python
TABLES.update({
    "era5_arl_bronze": "ais.weather.era5_arl_files_bronze",
    "hysplit_runs_bronze": "ais.trajectory.hysplit_runs_bronze",
    "hysplit_traj_silver": "ais.trajectory.hysplit_trajectories_silver",
    "hysplit_cluster_silver": "ais.trajectory.hysplit_trajectories_clustered_silver",
    "openaq_gradient_silver": "ais.features.openaq_spatial_gradient_silver",
    "s5p_grid_silver": "ais.satellite.sentinel5p_grid_silver",
    "trajectory_path_silver": "ais.features.trajectory_path_satellite_silver",
    "trajectory_hourly_silver": "ais.features.trajectory_hourly_features_silver",
})
```

## 2. Iceberg tables mới

### Sửa file: `spark_jobs/ensure_iceberg_tables.py`

Bổ sung namespace:

```text
ais.trajectory
```

Bổ sung bảng mới (schema gợi ý):

**2.1) ERA5 ARL files (bronze)**

```sql
era5_arl_files_bronze (
  dataset_type STRING,        -- pressure_levels
  year INT,
  month INT,
  source_nc STRING,           -- đường dẫn NetCDF gốc
  arl_path STRING,            -- đường dẫn ARL
  checksum STRING,
  created_at TIMESTAMP,
  spark_processed_at TIMESTAMP
)
PARTITIONED BY (dataset_type, year, month)
```

**2.2) HYSPLIT runs metadata (bronze)**

```sql
hysplit_runs_bronze (
  run_id STRING,
  direction STRING,           -- backward | forward
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
PARTITIONED BY (direction)
```

**2.3) HYSPLIT trajectories (silver)**

```sql
hysplit_trajectories_silver (
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
PARTITIONED BY (direction, year, month)
```

**2.4) Trajectory clustered (silver)**

```sql
hysplit_trajectories_clustered_silver (
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
PARTITIONED BY (direction)
```

**2.5) OpenAQ spatial gradient (silver)**

```sql
openaq_spatial_gradient_silver (
  hour TIMESTAMP,
  pm25_grad_N DOUBLE,
  pm25_grad_S DOUBLE,
  pm25_grad_E DOUBLE,
  pm25_grad_W DOUBLE,
  pm25_spatial_std DOUBLE,
  pm25_grad_mag DOUBLE,
  spark_processed_at TIMESTAMP
)
PARTITIONED BY (year, month)
```

**2.6) Sentinel-5P grid (silver)**

```sql
sentinel5p_grid_silver (
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
PARTITIONED BY (product, year, month)
```

**2.7) Path sampling features (silver)**

```sql
trajectory_path_satellite_silver (
  traj_id STRING,
  path_no2_mean DOUBLE,
  path_aer_mean DOUBLE,
  path_no2_max DOUBLE,
  path_no2_std DOUBLE,
  path_no2_aer_ratio DOUBLE,
  spark_processed_at TIMESTAMP
)
```

**2.8) Trajectory hourly features (silver)**

```sql
trajectory_hourly_features_silver (
  hour TIMESTAMP,
  dominant_cluster INT,
  n_traj INT,
  source_lat DOUBLE,
  source_lon DOUBLE,
  path_no2_mean DOUBLE,
  path_aer_mean DOUBLE,
  path_no2_aer_ratio DOUBLE,
  spark_processed_at TIMESTAMP
)
PARTITIONED BY (year, month)
```

Acceptance criteria:

- `ensure-iceberg` tạo đủ namespace/bảng mới.
- Rerun idempotent, không lỗi nếu bảng đã tồn tại.

## 3. ERA5 pressure-level ingest

### Sửa file: `ingest/era5_ingest.py`

- Hỗ trợ `--dataset-type pressure_levels`.
- Dùng `reanalysis-era5-pressure-levels`.
- Variables lấy theo config (u, v, vertical_velocity, geopotential, temperature, specific_humidity).
- Time list 6-hourly.
- HDFS output:

```text
hdfs://namenode:9000/raw/era5/pressure_levels/year=YYYY/month=MM/era5_pressure_levels_YYYYMM.nc
```

- Publish event vào topic `era5-files` với `dataset_type=pressure_levels`.

Acceptance criteria:

- Có NetCDF pressure-level trên HDFS.
- `era5-files` nhận event metadata cho pressure_levels.

## 4. ERA5 pressure-level -> ARL

### Tạo file: `spark_jobs/era5_pressure_levels_to_arl.py`

Mục đích:

- Đọc `era5_files_bronze` lọc `dataset_type=pressure_levels`.
- Copy NetCDF từ HDFS về local tạm.
- Gọi `/opt/hysplit/exec/era5_2arl` để tạo ARL.
- Đẩy ARL lên HDFS.
- Ghi metadata vào `era5_arl_files_bronze`.

Acceptance criteria:

- Có ARL file theo tháng trên HDFS.
- Log số file convert thành công/thất bại.

## 5. HYSPLIT trajectory run

### Tạo file: `spark_jobs/hysplit_trajectory_run.py`

Mục đích:

- Chạy backward/forward trajectories dựa trên ARL.
- Backward chỉ chạy cho giờ có PM2.5 cao (dựa trên `pm25_trigger_threshold`).
- Forward chạy theo lịch (vd 4 giờ/ngày) hoặc theo `run_hours_utc`.

Input:

- `ais.weather.era5_arl_files_bronze`.
- `ais.air_quality.openaq_hanoi_hourly_silver`.

Output:

- Trajectory text files trên HDFS.
- Ghi metadata vào `ais.trajectory.hysplit_runs_bronze`.

Acceptance criteria:

- Run được backward/forward với ARL.
- Log số trajectory thành công/lỗi.

## 6. Parse HYSPLIT output -> Iceberg

### Tạo file: `spark_jobs/hysplit_trajectory_parse_silver.py`

- Đọc file output HYSPLIT từ HDFS.
- Parse theo format trong blueprint.
- Ghi `ais.trajectory.hysplit_trajectories_silver`.

Acceptance criteria:

- Có dữ liệu trajectory theo hour/age_h.
- Không duplicate theo `traj_id` + `age_h`.

## 7. Trajectory clustering

### Tạo file: `spark_jobs/hysplit_trajectory_cluster_silver.py`

- Pivot trajectory theo `anchor_hours`.
- Chuẩn hóa và chạy KMeans.
- Chọn k theo config; cho phép sweep k=3..10 (log WCSS).
- Ghi output `ais.trajectory.hysplit_trajectories_clustered_silver`.

Acceptance criteria:

- Có `cluster_id`, `source_lat/lon` cho mỗi `traj_id`.
- Log số lượng cluster và phân bố.

## 8. OpenAQ spatial gradient

### Tạo file: `spark_jobs/openaq_spatial_gradient_silver.py`

- Đọc `openaq_hanoi_station_hourly_silver`.
- Tính gradient N/S/E/W bằng IDW.
- Ghi `openaq_spatial_gradient_silver`.

Acceptance criteria:

- 1 row/hour nếu đủ station.
- Log null ratio và độ phủ.

## 9. Sentinel-5P grid for path sampling

### Tạo file: `spark_jobs/sentinel5p_grid_silver.py`

- Đọc raw Sentinel-5P NetCDF theo product.
- Extract lat/lon/value, apply QA.
- Ghi grid theo date + product.

Acceptance criteria:

- Có pixel-level table cho S5P.
- `valid_pct` được log theo product/date.

## 10. Trajectory path sampling

### Tạo file: `spark_jobs/trajectory_path_sampling_silver.py`

- Input: `hysplit_trajectories_silver` (backward) + `sentinel5p_grid_silver`.
- Lấy path segment từ `-72h` đến `-24h`.
- Join theo nearest pixel trong bán kính `max_distance_deg`.
- Output `trajectory_path_satellite_silver`.

Acceptance criteria:

- Có `path_no2_mean`, `path_aer_mean`, `path_no2_aer_ratio` cho mỗi `traj_id`.
- Log số trajectory có/không có matched pixel.

## 11. Trajectory hourly features

### Tạo file: `spark_jobs/trajectory_hourly_features_silver.py`

- Join `hysplit_trajectories_clustered_silver` + `trajectory_path_satellite_silver`.
- Aggregate theo `hour`:
  - `dominant_cluster`
  - `n_traj`
  - `source_lat/lon`
  - `path_no2_mean`, `path_aer_mean`, `path_no2_aer_ratio`
- Ghi `trajectory_hourly_features_silver`.

Acceptance criteria:

- 1 row/hour nếu có trajectory.
- Log coverage theo hour.

## 12. Mở rộng Gold Master Feature

### Sửa file: `spark_jobs/hanoi_pm25_master_features_gold.py`

- Join thêm:
  - `openaq_spatial_gradient_silver`.
  - `trajectory_hourly_features_silver`.
- Bổ sung time features (sin/cos + rush hour):
  - `hour_sin`, `hour_cos`, `dow_sin`, `dow_cos`, `month_sin`, `month_cos`, `is_rush_hour`.
- Update schema `master_gold` trong `ensure_iceberg_tables.py`.

Acceptance criteria:

- Gold master có thêm cột trajectory + gradient + time sin/cos.
- Log `feature_row_count`, `lag_null_count_by_lag`, `target_non_null_count_by_horizon`.

## 13. Cập nhật Training Dataset + Log huấn luyện

### Sửa file: `spark_jobs/hanoi_pm25_training_dataset_gold.py`

- Bổ sung nhóm feature mới:
  - `trajectory`
  - `gradient`
  - `time_sincos`

### Sửa file: `ml/train_hanoi_pm25.py`

- Cập nhật `FEATURE_COLUMNS` để dùng feature Tier 2.
- Đảm bảo log in ra MAE/RMSE/MAPE theo từng horizon.
- Ghi metadata vào `ais.models.hanoi_pm25_model_runs_gold` (đã có sẵn).

Acceptance criteria:

- Log metrics xuất hiện trong stdout.
- `model_runs_gold` có thêm dòng mới sau mỗi run.

## 14. Airflow DAGs

### Tạo file: `airflow/dags/ais_trajectory_tier2_dag.py`

Task flow gợi ý:

```text
ensure_iceberg_tables
era5_pressure_ingest
era5_pressure_to_arl
hysplit_trajectory_run
hysplit_trajectory_parse
hysplit_trajectory_cluster
openaq_spatial_gradient
sentinel5p_grid_silver
trajectory_path_sampling
trajectory_hourly_features
hanoi_pm25_master_features_gold
hanoi_pm25_training_dataset_gold
train_hanoi_pm25
```

Acceptance criteria:

- DAG chạy được trên 1 range ngày nhỏ.
- Log count/metrics ở mỗi task.

## 15. Submit scripts

### Sửa file: `scripts/submit_spark.sh`

Bổ sung case:

```text
era5-pressure-arl
hysplit-run
hysplit-parse
hysplit-cluster
openaq-gradient
s5p-grid-silver
traj-path-sampling
traj-hourly-features
```

## 16. Dependencies & Runtime

- HYSPLIT binaries trong image chạy Spark/batch (`/opt/hysplit/exec`).
- Thư viện cần có:
  - `netCDF4`, `numpy`, `pandas`, `scipy`, `scikit-learn`.
  - `pyhdf` hoặc GDAL nếu cần đọc MAIAC grid sau này.

## 17. Data Quality Checks (bổ sung)

- HYSPLIT:
  - `trajectory_count`, `error_count`, `min_time`, `max_time`.
- Clustering:
  - `cluster_distribution`, `missing_cluster_ratio`.
- Path sampling:
  - `matched_pixel_ratio`, `path_no2_mean_min/max`.
- Gradient:
  - `pm25_grad_mag_min/max`, `null_ratio`.

## Out Of Scope (pha này)

```text
Trực quan hóa (map, plume UI)
Export GeoJSON/CSV phục vụ UI
Cassandra serving cho plume
```
