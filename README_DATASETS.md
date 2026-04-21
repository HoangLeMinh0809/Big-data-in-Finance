# README DATASETS - OpenAQ, Weather, Sentinel-5P, Mosaic

Tài liệu tổng hợp nhanh các bộ dữ liệu (datasets) đang và dự kiến sử dụng cho bài toán dự báo ô nhiễm không khí.

---

## 1) OpenAQ Dataset

### Mục đích
- Dữ liệu quan trắc mặt đất theo trạm hoặc cảm biến.
- Dùng làm mục tiêu dự báo (target forecasting: PM2.5/PM10) và các đặc trưng (features) thời gian ngắn hạn.

### Định dạng trong dự án
- **CSV:** `openaq_vietnam_hourly.csv`
- **JSON:** `openaq_vietnam_hourly.json`
- **Event JSON qua Kafka:** Topic `openaq-hourly`
- **Parquet trên HDFS:** Sau khi xử lý qua Spark Streaming.

### Lưu ý dữ liệu mẫu
- File tên là `openaq_vietnam_hourly.*` nhưng mẫu hiện tại có thể chứa nhiều điểm ngoài Việt Nam.
- Cần áp dụng bộ lọc quốc gia hoặc tọa độ địa lý Việt Nam trước khi huấn luyện mô hình.

### Trường dữ liệu chính
- **Định danh và vị trí:**
  - `location_id`, `location_name`, `city`, `latitude`, `longitude`, `provider`
- **Cảm biến và chất ô nhiễm:**
  - `sensor_id`, `parameter`, `unit`
- **Thời gian:**
  - `datetime_utc`, `datetime_local`
- **Giá trị đo:**
  - `value`, `min`, `max`, `sd` (độ lệch chuẩn)
- **Chất lượng độ phủ:**
  - `expected_count`, `observed_count`, `coverage_pct`
- **Siêu dữ liệu đường ống (Metadata pipeline):**
  - `source`, `ingest_time`, `event_id`

### Trường weather bổ sung (khuyến nghị khi join với weather_history)
- Để tăng chất lượng dự báo, nên join theo không gian-thời gian và bổ sung các trường weather vào bảng feature của OpenAQ.
- Các trường weather thường dùng:
  - `weather_temp_c`, `weather_humidity`, `weather_wind_kph`, `weather_pressure_mb`
  - `weather_precip_mm`, `weather_cloud`, `weather_uv`, `weather_vis_km`
  - `weather_condition_text`

### Cách sử dụng
- Lọc theo `parameter` để tạo bài toán riêng (ví dụ: chỉ dự báo PM2.5).
- Chuẩn hóa toàn bộ thời gian về múi giờ UTC.
- Đặt ngưỡng `coverage_pct` (ví dụ > 90%) để giữ lại dữ liệu có chất lượng tốt.

---

## 2) Sentinel-5P Dataset

### Mục đích
- Bổ sung đặc trưng khí quyển từ vệ tinh cho từng khu vực cụ thể.
- Giảm thiểu hạn chế của việc chỉ sử dụng dữ liệu từ các trạm quan trắc mặt đất (vốn có mật độ thưa thớt).

### Định dạng trong dự án
- **Metadata JSON/CSV:** Trích xuất từ truy vấn CDSE OData.
- **File khoa học NetCDF (`.nc`):** Khi thực hiện tải xuống (download).

### Ghi chú định dạng
- Sentinel-5P xử lý chính trên file NetCDF (`.nc`).

### Trường metadata trong crawler
- `id`: Định danh sản phẩm (product id) để truy xuất/download.
- `name`: Tên sản phẩm.
- `product_type`: Các loại sản phẩm như L2__NO2___, L2__SO2___, L2__CO____, L2__O3____, L2__CH4___, L2__HCHO__, L2__AER_AI.
- `start_time_utc`, `end_time_utc`: Cửa sổ thời gian quan sát.
- `origin_date`, `publication_date`: Ngày tạo và ngày công bố dữ liệu.
- `online`: Trạng thái sẵn sàng cho phép tải xuống.
- `s3_path`: Đường dẫn lưu trữ đối tượng trên S3.
- `geofootprint`: Vùng bao phủ không gian (vùng đa giác).

### Trường khoa học trong file .nc (Tổng quát)
- **Cột khí/Chỉ số quang học:** NO2, SO2, CO, O3, CH4, HCHO, chỉ số bụi mịn (aerosol index).
- **Trường chất lượng:** `qa_value` (tên cụ thể có thể khác nhau tùy theo từng loại sản phẩm).
- **Trường hình học/thời gian:** Tùy thuộc vào cấu trúc hỗ trợ của từng sản phẩm.

### Cách sử dụng
- Tìm kiếm sản phẩm bằng OData theo vùng bao (bbox) + thời gian + loại sản phẩm (product_type).
- Tải trực tiếp qua Product ID và OAuth token.
- Giải mã file `.nc` bằng thư viện `xarray` hoặc `netCDF4`, lọc theo chỉ số `qa_value` trước khi thực hiện liên kết không gian (spatial join) với các trạm đo.

### Khai thác NetCDF nhanh (Sentinel-5P)
```python
import xarray as xr

ds = xr.open_dataset("S5P_sample.nc", group="PRODUCT")
var = ds["nitrogendioxide_tropospheric_column"]
qa = ds["qa_value"]
var_clean = var.where(qa >= 0.75)
df = var_clean.to_dataframe(name="no2_col").reset_index()
```

---

## 3) Mosaic Dataset (HDF4)

### Trạng thái trong repo
- Đã có file HDF4 trong thư mục `crawler/maiac_data/`.
- Hiện có 21 file `.hdf` (MAIAC, pattern `MCD19A2.*.hdf`).
- Có thể dùng ngay để trích xuất đặc trưng aerosol cho bài toán dự báo.

### Vị trí dữ liệu hiện có
- Thư mục: `crawler/maiac_data/`
- Ví dụ file:
  - `MCD19A2.A2026087.h27v06.061.2026090153844.hdf`
  - `MCD19A2.A2026088.h28v07.061.2026090155322.hdf`
  - `MCD19A2.A2026089.h29v08.061.2026090162809.hdf`

### Mosaic là gì?
- Là ảnh tổng hợp địa không gian (theo ngày/tuần/tháng) được ghép từ nhiều cảnh chụp khác nhau.
- Thường dùng để giảm nhiễu do mây che phủ, giúp phủ kín khu vực và tạo ra các đặc trưng (features) ổn định hơn.

### Định dạng sử dụng
- **HDF4 (`.hdf`, `.h4`)** là định dạng gốc để khai thác.
- Có thể chuyển đổi sang **GeoTIFF/COG** hoặc bảng **Parquet** sau khi trích xuất.

### Trường/Gói đặc trưng thường dùng
- Giá trị trung bình (mean), tối đa (max), hoặc phân vị (percentile) theo ô lưới và theo cửa sổ thời gian.
- Xu hướng ngắn hạn (3 ngày, 7 ngày) và biến động theo mùa vụ.
- Chỉ số chất lượng theo sản phẩm (quality flags, cloud mask, valid pixels) nếu có.

### Khai thác HDF4 nhanh (Mosaic)
```python
from pyhdf.SD import SD, SDC

hdf = SD("mosaic_sample.hdf", SDC.READ)
print(list(hdf.datasets().keys()))

sds = hdf.select("AOD_550_Dark_Target_Deep_Blue_Combined")
arr = sds.get().astype("float32")
attrs = sds.attributes()
scale = attrs.get("scale_factor", 1.0)
offset = attrs.get("add_offset", 0.0)
fill = attrs.get("_FillValue")

if fill is not None:
  arr[arr == fill] = float("nan")
arr = arr * scale + offset
```

---

## 4) Weather History Dataset (WeatherAPI)

### Mục đích
- Bổ sung biến khí tượng theo giờ làm biến ngoại sinh cho mô hình dự báo chất lượng không khí.
- Hỗ trợ giải thích dao động ngắn hạn của PM2.5/PM10.

### Định dạng trong dự án
- **Raw JSON theo tỉnh/ngày:** `data/weather/<Province>/YYYY-MM-DD.json`
- **Event JSON qua Kafka:** Topic `weather_history`
- **Parquet trên HDFS:** `/data/weather_history/` sau Spark Structured Streaming

### Trường dữ liệu chính (sau normalize)
- **Định danh và thời gian:**
  - `province`, `location_name`, `datetime_utc`, `datetime_local`, `tz_id`
- **Nhiệt độ và cảm giác nhiệt:**
  - `temp_c`, `temp_f`, `feelslike_c`, `feelslike_f`, `heatindex_c`, `windchill_c`
- **Gió và áp suất:**
  - `wind_kph`, `wind_mph`, `wind_degree`, `wind_dir`, `pressure_mb`, `pressure_in`
- **Độ ẩm và tầm nhìn:**
  - `humidity`, `dewpoint_c`, `vis_km`, `cloud`, `uv`
- **Mưa và thời tiết mô tả:**
  - `precip_mm`, `precip_in`, `condition_text`, `condition_code`
- **Metadata pipeline:**
  - `source`, `ingest_time`, `event_id`, `source_file`

## 5) Gợi ý hợp nhất dữ liệu phục vụ Forecasting (Dự báo)

1. **Chuẩn hóa:** Đồng bộ hóa lược đồ dữ liệu (schema) và đơn vị đo lường.
2. **Đồng bộ thời gian:** Chuyển tất cả nguồn dữ liệu về chuẩn UTC.
3. **Lọc chất lượng:** Sử dụng `coverage_pct` cho dữ liệu trạm và `qa_value` cho dữ liệu vệ tinh.
4. **Spatial Join:** Liên kết Trạm -> Pixel (sử dụng phương pháp láng giềng gần nhất - nearest hoặc trung bình trong vùng đệm - buffer average).
5. **Temporal Join:** Khớp dữ liệu theo cửa sổ thời gian gần nhất.
6. **Feature Engineering:** Tạo các đặc trưng trễ (lag), trung bình trượt (rolling), và các đặc trưng lịch (thứ, ngày, tháng).
7. **Phân tách dữ liệu:** Chia tập Train/Test theo thời gian (Time-series split) để tránh rò rỉ dữ liệu (data leakage).

### Mục tiêu đề xuất (Target)
- **Target chính:** Chỉ số PM2.5 hoặc PM10 từ OpenAQ.
- **Biến ngoại sinh (Exogenous features):** Các biến weather theo giờ, chỉ số từ Sentinel-5P và (nếu có) Mosaic.