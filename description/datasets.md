# Datasets Inventory & Data Dictionary

> Tài liệu này liệt kê các dataset trong repo và mô tả **schema (các trường) + vai trò** của từng trường.
>
> Notes:
> - Nhiều dataset là file lớn (ví dụ `data/weather/**`, `crawler/maiac_data/**`, `data/sentinel5p_data/**`), nên tài liệu ưu tiên **data dictionary** hơn là liệt kê mọi file.
> - Với dataset không có schema “phẳng” (GeoJSON/HDF/NetCDF), tài liệu mô tả **cấu trúc** và **trường/biến quan trọng** đang được pipeline sử dụng.

## Mục lục
- [1. Air quality (OpenAQ)](#1-air-quality-openaq)
  - [1.1. Raw CSV](#11-data-crawlingopenaq_vietnam_hourlycsv--openaq-hourly-raw-extract)
  - [1.2. Kafka events (`openaq-hourly`)](#12-kafka-events-openaq-hourly--openaq-normalized-events)
  - [1.3. HDFS Parquet sink](#13-hdfs-dataset-hdfsnamenode9000dataopenaq_hourly--openaq-parquet)
- [2. Weather (WeatherAPI)](#2-weather-weatherapi)
  - [2.1. Raw JSON](#21-data-weatherprovinceyyyy-mm-ddjson--weather-history-raw-per-day)
  - [2.2. Kafka events](#22-kafka-events-weather-history-hoặc-weather_history--weather-hourly-normalized-events)
  - [2.3. HDFS Parquet sink](#23-hdfs-dataset-hdfsnamenode9000dataweather_history--weather-parquet)
- [3. Sentinel-5P (Copernicus Data Space)](#3-sentinel-5p-copernicus-data-space)
  - [3.1. Raw NetCDF downloads](#31-netcdf-downloads-data-sentinel5p_data-nc)
  - [3.2. Kafka events (`sentinel5p-summary`)](#32-kafka-events-sentinel5p-summary--sentinel-5p-summary-stats)
  - [3.3. Optional crawler metadata export](#33-crawler-metadata-export-optional-data-crawlingsentinel5p_hanoi_crawlerpy)
- [4. Geospatial boundaries (GeoJSON)](#4-geospatial-boundary-datasets-geojson)
- [5. Satellite aerosol (MAIAC/MODIS)](#5-satellite-aerosol-datasets-maiacmodis)

---

## 1. Air quality (OpenAQ)

### 1.1. `data/crawling/openaq_vietnam_hourly.csv` — OpenAQ hourly raw extract
- **Vị trí (theo code)**: `data/crawling/openaq_vietnam_hourly.csv` (tạo bởi `data/crawling/crawl.py`)
- **Nguồn**: OpenAQ API v3
- **Định dạng**: CSV
- **Ý nghĩa**: dữ liệu theo giờ cho sensor tại VN trong `HOURS_BACK` giờ gần nhất.

**Schema CSV (tạo bởi `data/crawling/crawl.py`)**
| Field | Type | Vai trò / ý nghĩa |
|---|---:|---|
| `location_id` | int | ID trạm đo (OpenAQ location) |
| `location_name` | string | Tên trạm đo |
| `city` | string | Tỉnh/thành (hoặc locality/city trả về) |
| `latitude` | float | Vĩ độ trạm |
| `longitude` | float | Kinh độ trạm |
| `provider` | string | Nhà cung cấp dữ liệu |
| `sensor_id` | int | ID sensor tại location |
| `parameter` | string | Chỉ số (PM2.5/PM10/NO2/O3/CO/SO2) |
| `unit` | string | Đơn vị đo |
| `datetime_utc` | datetime string | Thời gian (UTC) |
| `datetime_local` | datetime string | Thời gian (local) |
| `value` | float | Giá trị đo tại giờ đó |
| `min` | float | Min trong khoảng tổng hợp (nếu API trả) |
| `max` | float | Max trong khoảng tổng hợp |
| `sd` | float | Độ lệch chuẩn |
| `expected_count` | int | Số điểm kỳ vọng trong khoảng |
| `observed_count` | int | Số điểm quan sát được |
| `coverage_pct` | float | % coverage = observed/expected * 100 |

### 1.2. Kafka events (`openaq-hourly`) — OpenAQ normalized events
- **Producer**: `ingest/openaq_ingest.py` (CSV → Kafka)
- **Consumer/Sink**: `spark_jobs/openaq_hourly_streaming.py` (Kafka → Parquet on HDFS)

**Schema JSON message (theo `ingest/openaq_ingest.py::build_event`)**
| Field | Type | Vai trò / ý nghĩa |
|---|---:|---|
| `location_id` | int | ID trạm |
| `location_name` | string | Tên trạm |
| `city` | string | Thành phố/tỉnh |
| `latitude` | float | Vĩ độ |
| `longitude` | float | Kinh độ |
| `provider` | string | Provider |
| `sensor_id` | int | Sensor ID |
| `parameter` | string | Chỉ số |
| `unit` | string | Đơn vị |
| `datetime_utc` | string | Timestamp UTC (string) |
| `datetime_local` | string | Timestamp local (string) |
| `value` | float | Giá trị |
| `min` | float | Min |
| `max` | float | Max |
| `sd` | float | Standard deviation |
| `expected_count` | int | Expected count |
| `observed_count` | int | Observed count |
| `coverage_pct` | float | % coverage |
| `source` | string | Cố định: `openaq` |
| `ingest_time` | string (ISO-8601) | Thời điểm gửi Kafka |
| `event_id` | string | ID ổn định: `openaq_{location_id}_{sensor_id}_{parameter}_{datetime_utc}` |

### 1.3. HDFS dataset: `hdfs://namenode:9000/data/openaq_hourly/` — OpenAQ Parquet
- **Sink job**: `spark_jobs/openaq_hourly_streaming.py`
- **Định dạng**: Parquet
- **Partition**: `year/month/day/hour` (tách từ `datetime_utc`)

**Schema Parquet**
- Các field như Kafka JSON + field Spark bổ sung:
  - `event_time`: timestamp (parse từ `datetime_utc`)
  - `year`, `month`, `day`, `hour`: int (partition columns)
  - `spark_processed_at`: timestamp Spark xử lý

---

## 2. Weather (WeatherAPI)

### 2.1. `data/weather/<Province>/<YYYY-MM-DD>.json` — Weather history raw (per day)
- **Vị trí**: `data/weather/**.json`
- **Nguồn**: WeatherAPI `history.json`
- **Định dạng**: JSON
- **Ý nghĩa**: theo từng tỉnh / từng ngày, chứa `location` + `forecast.forecastday[].hour[]`.

**Cấu trúc JSON chính (WeatherAPI)**
| Node | Vai trò |
|---|---|
| `location` | Thông tin vị trí (name, region, country, lat, lon, tz_id, localtime, …) |
| `forecast.forecastday[]` | Mỗi phần tử tương ứng 1 ngày |
| `forecast.forecastday[].hour[]` | Dữ liệu theo giờ trong ngày: nhiệt độ, gió, áp suất, mưa, ẩm… |

### 2.2. Kafka events (`weather-history` / `weather_history`) — Weather hourly normalized events
- **Producer**: `ingest/ingest_weather.py` (file/API → normalize → Kafka)

#### 2.2.1. Trường định danh & vị trí
| Field | Type | Vai trò |
|---|---:|---|
| `event_id` | string | ID ổn định: `{province}_{event_time}` |
| `province` | string | Tỉnh/thành (derive từ folder) |
| `country` | string | Quốc gia (từ payload) |
| `region` | string | Region (từ payload) |
| `location_name` | string | Tên location trong payload |
| `lat` | float | Vĩ độ |
| `lon` | float | Kinh độ |
| `tz_id` | string | Timezone ID |

#### 2.2.2. Trường thời gian
| Field | Type | Vai trò |
|---|---:|---|
| `query_date` | string (YYYY-MM-DD) | Ngày (forecastday.date) |
| `time` | string | Thời gian giờ (format `YYYY-MM-DD HH:MM`) |
| `time_epoch` | long | Epoch seconds |
| `is_day` | int (0/1) | Ban ngày hay ban đêm |

#### 2.2.3. Nhiệt độ & cảm giác
| Field | Type | Vai trò |
|---|---:|---|
| `temp_c`, `temp_f` | float | Nhiệt độ |
| `feelslike_c`, `feelslike_f` | float | Nhiệt độ cảm nhận |
| `windchill_c`, `windchill_f` | float | Wind chill |
| `heatindex_c`, `heatindex_f` | float | Heat index |
| `dewpoint_c`, `dewpoint_f` | float | Dew point |

#### 2.2.4. Thời tiết mô tả
| Field | Type | Vai trò |
|---|---:|---|
| `condition_text` | string | Mô tả (Sunny/Cloudy/…) |
| `condition_code` | int | Mã điều kiện |
| `condition_icon` | string | URL icon |

#### 2.2.5. Gió
| Field | Type | Vai trò |
|---|---:|---|
| `wind_mph`, `wind_kph` | float | Tốc độ gió |
| `wind_degree` | int | Hướng gió (degrees) |
| `wind_dir` | string | Hướng gió (N/NE/…) |
| `gust_mph`, `gust_kph` | float | Gió giật |

#### 2.2.6. Áp suất, mưa/tuyết, độ ẩm (tổng hợp)
> Dựa theo schema Spark `spark_jobs/weather_streaming.py`.

| Field | Type | Vai trò |
|---|---:|---|
| `pressure_mb`, `pressure_in` | float | Áp suất |
| `precip_mm`, `precip_in` | float | Lượng mưa |
| `snow_cm` | float | Tuyết |
| `humidity` | int | Độ ẩm (%) |
| `cloud` | int | Mây (%) |
| `vis_km`, `vis_miles` | float | Tầm nhìn |
| `uv` | float | Chỉ số UV |
| `will_it_rain`, `chance_of_rain` | int | Có mưa + xác suất |
| `will_it_snow`, `chance_of_snow` | int | Có tuyết + xác suất |

#### 2.2.7. Metadata ingest
| Field | Type | Vai trò |
|---|---:|---|
| `source` | string | `weatherapi` / source mode |
| `source_file` | string | Đường dẫn file local hoặc URL tham chiếu |
| `ingest_time` | string | Thời điểm ingest |

### 2.3. HDFS dataset: `hdfs://namenode:9000/data/weather_history/` — Weather Parquet
- **Sink job**: `spark_jobs/weather_streaming.py`
- **Partition**: `year/month` từ `query_date`

**Schema Parquet**
- Các field như Kafka message +
  - `event_time`: timestamp (parse từ `time`)
  - `spark_processed_at`: timestamp
  - `year`, `month`: int

---

## 3. Sentinel-5P (Copernicus Data Space)

### 3.1. NetCDF downloads: `data/sentinel5p_data/*.nc`
- **Vị trí**: `data/sentinel5p_data/`
- **Nguồn**: CDSE download endpoint
- **Định dạng**: NetCDF4 `.nc`
- **Ý nghĩa**: file sản phẩm Sentinel-5P L2 (grid/pixel rất lớn).

**Cấu trúc NetCDF (tóm tắt theo pipeline ingest)**
- Script ingest ( `ingest/sentinel5p_ingest.py` ) sử dụng:
  - `group = "PRODUCT"`
  - `variable` theo product:
    - `NO2`: `nitrogendioxide_tropospheric_column`
    - `CO`: `carbonmonoxide_total_column`
    - `O3`: `ozone_total_vertical_column`
    - `SO2`: `sulfurdioxide_total_vertical_column`
    - `CH4`: `methane_mixing_ratio_bias_corrected`
    - `AER`: `aerosol_index_354_388`
  - optional mask:
    - `qa_value` (lọc QA < 0.5)
    - `_FillValue` (mask missing)

### 3.2. Kafka events (`sentinel5p-summary`) — Sentinel-5P summary stats
- **Producer**: `ingest/sentinel5p_ingest.py`
- **Định dạng**: JSON

**Schema JSON message (theo `ingest/sentinel5p_ingest.py`)**
| Field | Type | Vai trò |
|---|---:|---|
| `product` | string | Key sản phẩm: `NO2/CO/O3/SO2/CH4/AER` |
| `collection` | string | Cố định: `SENTINEL-5P` |
| `content_start` | string | Thời gian bắt đầu (metadata) |
| `content_end` | string | Thời gian kết thúc (metadata) |
| `bbox` | array[float] | Bounding box truy vấn `[lon_min, lat_min, lon_max, lat_max]` |
| `file_name` | string | Tên file `.nc` đã download |
| `stats.min` | float/null | Min sau mask QA/fill |
| `stats.max` | float/null | Max |
| `stats.mean` | float/null | Mean |
| `stats.valid_pct` | float | % pixel hợp lệ (non-NaN) |
| `unit` | string | Đơn vị theo sản phẩm |
| `ingest_time` | string | Thời điểm ingest |
| `event_id` | string | ID ổn định: `s5p_{product}_{Id}_{DATE_START}_{DATE_END}` |
| `source` | string | Cố định: `cdse` |

### 3.3. Crawler metadata export (optional): `data/crawling/sentinel5p_hanoi_crawler.py`
- Script này tạo JSON/CSV metadata sản phẩm (không phải pipeline Kafka).

**Schema CSV xuất ra (theo `save_csv()` trong script)**
| Field | Type | Vai trò |
|---|---:|---|
| `id` | string | Product Id |
| `name` | string | Product Name |
| `product_type` | string | Loại khí: `L2__NO2___`, ... |
| `start_time_utc` | string | ContentDate.Start |
| `end_time_utc` | string | ContentDate.End |
| `origin_date` | string | OriginDate |
| `publication_date` | string | PublicationDate |
| `online` | bool | Trạng thái online |
| `s3_path` | string | path trên object storage |

---

## 4) Geospatial boundary datasets (GeoJSON)

### 4.1. `crawler/geoBoundaries-*.geojson` — Ranh giới hành chính VN
- **Vị trí**:
  - `crawler/geoBoundaries-VNM-ADM1_simplified.geojson`
  - `crawler/geoBoundaries-VNM-ADM2_simplified.geojson`
  - `crawler/hanoi_districts_clean.geojson`
- **Nguồn**: geoboundaries.org
- **Định dạng**: GeoJSON

**Cấu trúc chung GeoJSON**
- `type`: `FeatureCollection`
- `features[]`: mỗi feature có:
  - `properties`: metadata hành chính (tên, mã ISO, cấp ADM…)
  - `geometry`: Polygon/MultiPolygon


## 5) Satellite aerosol datasets (MAIAC/MODIS)

### 5.1. `crawler/maiac_data/*.hdf` — MAIAC aerosol product files
- **Vị trí**: `crawler/maiac_data/`
- **Định dạng**: HDF (thường là HDF4-EOS) – MODIS/MAIAC
- **Ghi chú**: Trong scope hiện tại, mình mô tả **"trường" suy ra từ naming convention của file HDF** (là thứ bạn nói “lấy trường trong file cào cũng được”).
  - Việc bóc toàn bộ SDS/variables bên trong HDF có thể bổ sung sau nếu cần.

#### 5.1.1. Schema từ tên file (filename convention)
Ví dụ file trong repo:
- `MCD19A2.A2026088.h28v07.061.2026090155322.hdf`

Bóc tách theo pattern tổng quát:
- `<product>.<acquisition>.h<hTile>v<vTile>.<collection>.<processingTimestamp>.hdf`

| Field | Type | Lấy từ | Vai trò / ý nghĩa |
|---|---:|---|---|
| `product` | string | `MCD19A2` | Mã product MAIAC. `MCD19A2` thường là **MAIAC AOD (Aerosol Optical Depth)** (Terra+Aqua merged). |
| `acquisition_year` | int | `A2026088` → `2026` | Năm quan sát (theo Julian day). |
| `acquisition_doy` | int | `A2026088` → `088` | Ngày trong năm (Day Of Year). |
| `acquisition_date_approx` | date (derived) | year+doy | Ngày quan sát (xấp xỉ, cần convert DOY → date nếu muốn hiển thị). |
| `tile_h` | int | `h28` | MODIS sinusoidal tile index theo trục “h”. |
| `tile_v` | int | `v07` | MODIS sinusoidal tile index theo trục “v”. |
| `tile_id` | string | `h28v07` | Tile id gộp. |
| `collection` | int/string | `061` | Collection/version của NASA LP DAAC (ví dụ Collection 6.1). |
| `processing_timestamp` | string | `2026090155322` | Thời điểm xử lý/đóng gói sản phẩm (YYYY + DOY + HHMMSS kiểu nội bộ). |
| `file_ext` | string | `.hdf` | Định dạng file. |
| `file_path` | string | path | Đường dẫn trong repo/HDFS (nếu ingest). |

#### 5.1.2. Vai trò trong pipeline
- Dataset này hiện ở dạng **raw** (chưa có ingest Kafka / Spark sink trong repo context).
- Dùng để:
  - ghép với GeoJSON boundaries (ADM1/ADM2) để tính AOD theo vùng;
  - tạo time-series aerosol theo ngày/tile.

#### 5.1.3. (Tuỳ chọn) Bước tiếp theo nếu cần “schema bên trong HDF”
- Dùng GDAL hoặc `pyhdf`/`h5py` để liệt kê SDS/variables như (tùy product):
  - AOD các bước sóng, QA flags, angles, uncertainty…
