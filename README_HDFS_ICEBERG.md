# HDFS & Iceberg Runbook

Tài liệu này mô tả cách khởi tạo HDFS layout, kiểm tra trạng thái HDFS/Iceberg và chạy các Spark jobs để đưa dữ liệu từ Kafka vào Iceberg trong hệ thống AIS.

## 1. Mục đích

Trong hệ thống AIS, HDFS được dùng làm storage layer cho Apache Iceberg.

Luồng dữ liệu chính:

```text
Ingest Service
    ↓
Kafka Topics
    ↓
Spark Structured Streaming
    ↓
Iceberg Tables on HDFS
```

Các Spark jobs đọc dữ liệu từ Kafka và ghi vào các Iceberg tables nằm trong HDFS tại:

```text
/warehouse/iceberg
```

Spark Structured Streaming sử dụng checkpoint để lưu trạng thái xử lý tại:

```text
/checkpoints
```

---

## 2. HDFS layout chuẩn

Cấu trúc HDFS được chuẩn hóa như sau:

```text
/warehouse
/warehouse/iceberg
/warehouse/iceberg/weather
/warehouse/iceberg/weather/weather_history_bronze
/warehouse/iceberg/weather/era5_hourly_bronze

/warehouse/iceberg/air_quality
/warehouse/iceberg/air_quality/openaq_hourly_bronze

/warehouse/iceberg/satellite
/warehouse/iceberg/satellite/sentinel5p_summary_bronze
/warehouse/iceberg/satellite/maiac_summary_bronze

/checkpoints
/checkpoints/weather_history
/checkpoints/openaq_hourly
/checkpoints/sentinel5p_summary
/checkpoints/maiac_summary
/checkpoints/era5_hourly

/tmp
/tmp/spark

/logs
/logs/spark
/logs/ingest
```

| Path | Ý nghĩa |
|---|---|
| `/warehouse/iceberg` | Nơi lưu Iceberg warehouse |
| `/checkpoints` | Nơi Spark lưu checkpoint/offset khi đọc Kafka |
| `/tmp/spark` | Vùng tạm cho Spark |
| `/logs` | Vùng log nếu cần mở rộng sau này |

---

## 3. Lưu ý khi chạy trên Windows Git Bash

Khi dùng Git Bash trên Windows, cần tắt cơ chế tự động convert path của Git Bash.

Chạy trước các lệnh sau:

```bash
export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL="*"
```

Nếu không, các path như `/warehouse`, `/checkpoints`, `/tmp` có thể bị Git Bash chuyển thành path Windows, làm Docker/HDFS chạy sai.

---

## 4. Khởi tạo HDFS layout

Sau khi bật Docker services:

```bash
docker compose up -d
```

chạy:

```bash
export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL="*"

bash scripts/init_hdfs_layout.sh
```

Script này sẽ:

```text
- kiểm tra HDFS safemode
- tạo các thư mục warehouse/checkpoint/log/tmp
- cấp quyền ghi cho môi trường local/demo
```

---

## 5. Kiểm tra HDFS layout

Sau khi init, kiểm tra bằng:

```bash
bash scripts/check_hdfs_layout.sh
```

Kết quả mong muốn là thấy các thư mục:

```text
/warehouse/iceberg
/checkpoints
/logs
```

Nếu đã chạy Spark jobs, các Iceberg tables sẽ có:

```text
data/
metadata/
metadata/*.metadata.json
metadata/snap-*.avro
metadata/version-hint.text
```

---

## 6. Chạy Spark jobs để ghi vào Iceberg

Trước khi chạy Spark jobs, cần đảm bảo Kafka đã có dữ liệu từ các ingest services.

Ví dụ chạy ingest:

```bash
bash scripts/submit_spark.sh weather-ingest
bash scripts/submit_spark.sh openaq-ingest
bash scripts/submit_spark.sh sentinel5p-ingest
bash scripts/submit_spark.sh maiac-ingest
```

Sau đó chạy từng Spark job với chế độ đọc lại từ đầu Kafka và xử lý một batch rồi dừng:

```bash
KAFKA_STARTING_OFFSETS=earliest STOP_AFTER_BATCH=true bash scripts/submit_spark.sh weather
```

```bash
KAFKA_STARTING_OFFSETS=earliest STOP_AFTER_BATCH=true bash scripts/submit_spark.sh openaq
```

```bash
KAFKA_STARTING_OFFSETS=earliest STOP_AFTER_BATCH=true bash scripts/submit_spark.sh sentinel5p
```

```bash
KAFKA_STARTING_OFFSETS=earliest STOP_AFTER_BATCH=true bash scripts/submit_spark.sh maiac
```

| Biến | Ý nghĩa |
|---|---|
| `KAFKA_STARTING_OFFSETS=earliest` | Spark đọc từ message cũ nhất trong Kafka |
| `STOP_AFTER_BATCH=true` | Spark xử lý một batch rồi dừng, phù hợp để debug |
| `KAFKA_STARTING_OFFSETS=latest` | Spark chỉ đọc message mới phát sinh sau khi job chạy |

---

## 7. Kiểm tra dữ liệu đã vào Iceberg chưa

Sau khi chạy Spark jobs, kiểm tra:

```bash
bash scripts/check_hdfs_layout.sh
```

Hoặc kiểm tra trực tiếp:

```bash
docker exec namenode hdfs dfs -ls -R /warehouse/iceberg
```

Các bảng mong muốn:

```text
/warehouse/iceberg/weather/weather_history_bronze
/warehouse/iceberg/air_quality/openaq_hourly_bronze
/warehouse/iceberg/satellite/sentinel5p_summary_bronze
/warehouse/iceberg/satellite/maiac_summary_bronze
```

Một bảng Iceberg hợp lệ thường có dạng:

```text
table_name/
  data/
    *.parquet
  metadata/
    v1.metadata.json
    v2.metadata.json
    snap-*.avro
    version-hint.text
```

| Thành phần | Ý nghĩa |
|---|---|
| `data/*.parquet` | Dữ liệu thật |
| `metadata/*.metadata.json` | Metadata của Iceberg table |
| `snap-*.avro` | Snapshot/manifest của Iceberg |
| `version-hint.text` | Version metadata hiện tại |

---

## 8. Kiểm tra riêng từng source

### Weather

```bash
docker exec namenode hdfs dfs -ls -R /warehouse/iceberg/weather/weather_history_bronze
```

### OpenAQ

```bash
docker exec namenode hdfs dfs -ls -R /warehouse/iceberg/air_quality/openaq_hourly_bronze
```

### Sentinel-5P

```bash
docker exec namenode hdfs dfs -ls -R /warehouse/iceberg/satellite/sentinel5p_summary_bronze
```

### MAIAC

```bash
docker exec namenode hdfs dfs -ls -R /warehouse/iceberg/satellite/maiac_summary_bronze
```

---

## 9. Một số lỗi thường gặp

### 9.1. HDFS not writable yet

Nếu gặp:

```text
[WAIT] HDFS not writable yet
```

kiểm tra HDFS:

```bash
docker exec namenode hdfs dfsadmin -safemode get
docker exec namenode hdfs dfs -ls /
```

Nếu safemode đang bật:

```bash
docker exec namenode hdfs dfsadmin -safemode leave
```

Nếu dùng Git Bash trên Windows, đảm bảo đã chạy:

```bash
export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL="*"
```

### 9.2. Spark job bị WAITING trong Spark UI

Nếu Spark UI hiển thị job mới ở trạng thái `WAITING`, có thể các job streaming cũ đang chiếm hết core.

Kiểm tra Spark UI:

```text
http://localhost:8080
```

Dừng các job cũ nếu cần:

```bash
docker compose exec spark-master pkill -f weather_streaming.py || true
docker compose exec spark-master pkill -f openaq_hourly_streaming.py || true
docker compose exec spark-master pkill -f sentinel5p_summary_streaming.py || true
docker compose exec spark-master pkill -f maiac_summary_streaming.py || true
```

### 9.3. Không thấy dữ liệu mới trong Iceberg

Nếu Spark chạy nhưng không thấy file mới, kiểm tra Kafka có data không:

```bash
docker exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic weather_history \
  --from-beginning \
  --max-messages 5
```

Nếu Kafka có data nhưng Spark không ghi, thử chạy với:

```bash
KAFKA_STARTING_OFFSETS=earliest STOP_AFTER_BATCH=true bash scripts/submit_spark.sh weather
```

Nếu Spark đang dùng checkpoint cũ, có thể cần xóa checkpoint tương ứng khi debug:

```bash
docker exec namenode hdfs dfs -rm -r -f /checkpoints/weather_history
```

Chỉ xóa checkpoint khi muốn Spark đọc lại dữ liệu từ đầu.

---

## 10. Kết luận

Sau khi chuẩn hóa HDFS layout và chạy Spark jobs thành công, hệ thống có storage layer rõ ràng hơn:

```text
Kafka → Spark Structured Streaming → Iceberg on HDFS
```

Các bảng Iceberg được tổ chức theo domain:

```text
weather
air_quality
satellite
```

Mỗi streaming job có checkpoint riêng, giúp pipeline dễ debug, dễ vận hành và dễ mở rộng thêm nguồn dữ liệu ERA5 sau này.
