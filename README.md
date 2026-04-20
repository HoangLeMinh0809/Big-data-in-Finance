# Atmospheric Intelligence System (AIS)

Pipeline Big Data cho dữ liệu khí quyển: WeatherAPI history, OpenAQ hourly, Sentinel-5P và MAIAC. Kiến trúc vận hành chuẩn: Python ingest adapters -> Kafka -> Spark (realtime/batch) -> Iceberg/HDFS -> Cassandra, với Airflow chỉ giữ vai trò batch orchestration.

## Mục lục

1. [Kiến trúc tổng thể](#1-kiến-trúc-tổng-thể)
2. [Cấu trúc thư mục](#2-cấu-trúc-thư-mục)
3. [Docker Compose](#3-docker-compose)
4. [Ingest services](#4-ingest-services)
5. [Spark và storage](#5-spark-và-storage)
6. [Hướng dẫn chạy](#6-hướng-dẫn-chạy)
7. [Kiểm tra kết quả](#7-kiểm-tra-kết-quả)
8. [Lưu ý thiết kế cho Kubernetes](#8-lưu-ý-thiết-kế-cho-kubernetes)

## Tài liệu dataset liên quan

- Xem `README_DATASETS.md` để biết chi tiết schema, định dạng và cách khai thác OpenAQ, WeatherAPI, Sentinel-5P (NetCDF) và Mosaic MAIAC (HDF4).

---

## 1. Kiến trúc tổng thể

### Pipeline Flow

```text
Weather JSON/API ─┐
OpenAQ CSV/API  ──┼──> Python Ingest ──> Kafka ──> Spark Structured Streaming ──> Iceberg/HDFS
Sentinel-5P API ──┘                               │
                                                   └──> Spark batch load ──> Cassandra

Airflow điều phối các bước batch ingest và batch load serving (khong chay long-running realtime jobs).
Monitoring UI đọc Kafka/HDFS để theo dõi throughput và trạng thái lưu trữ.
```

### Vai trò từng service

| Service | Vai trò |
|---------|---------|
| `zookeeper` | Quản lý cluster metadata cho Kafka |
| `kafka` | Message broker nhận events từ ingest và cung cấp cho Spark |
| `namenode`, `datanode` | HDFS storage cho warehouse Iceberg và checkpoint |
| `spark-master`, `spark-worker` | Chạy Spark Structured Streaming và batch jobs |
| `ingest`, `openaq-ingest`, `sentinel5p-ingest`, `maiac-ingest` | Python source adapters đẩy dữ liệu về Kafka |
| `cassandra` | Serving layer cho truy vấn latency thấp |
| `airflow-*` | Airflow metadata DB, webserver, scheduler, triggerer và DAG orchestration |
| `monitoring-ui` | Dashboard theo dõi Kafka, HDFS/DataNode và pipeline status |

---

## 2. Cấu trúc thư mục

```text
Atmospheric_intelligence_sys---AIS/
├── docker-compose.yml              # Orchestration local cho Kafka, HDFS, Spark, Cassandra, Airflow, monitoring
├── airflow/
│   ├── Dockerfile
│   └── dags/
│       └── ais_pipeline_dag.py     # DAG batch orchestration cho ingest + load Cassandra
├── ingest/
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── ingest_weather.py           # Weather history producer
│   ├── openaq_ingest.py            # OpenAQ hourly producer
│   ├── sentinel5p_ingest.py        # Sentinel-5P summary producer
│   └── maiac_ingest.py             # MAIAC metadata producer
├── spark_jobs/
│   ├── weather_streaming.py        # Kafka weather_history -> Iceberg
│   ├── openaq_hourly_streaming.py  # Kafka openaq-hourly -> Iceberg
│   ├── sentinel5p_summary_streaming.py # Kafka sentinel5p-summary -> HDFS parquet
│   ├── maiac_summary_streaming.py  # Kafka maiac-summary -> HDFS parquet
│   └── iceberg_to_cassandra.py     # Iceberg -> Cassandra serving tables
├── data/
│   ├── weather/                    # Weather history JSON theo tỉnh/thành
│   └── crawling/                   # Script crawl WeatherAPI, OpenAQ, Sentinel-5P
├── crawler/                        # GeoJSON, notebook và dữ liệu MODIS MAIAC
├── monitoring/                     # Monitoring UI
├── scripts/                        # Helper scripts tạo topic, submit Spark, health check
├── hadoop/
│   └── hadoop.env                  # Cấu hình Hadoop/HDFS
└── checkpoints/                    # Runtime state/checkpoint
```

---

## 3. Docker Compose

File `docker-compose.yml` chạy các service trên network chung `bigdata-net`.

**Các cổng chính:**

| Port | Service | Mô tả |
|------|---------|-------|
| 2181 | Zookeeper | Client connections |
| 9092 / 29092 | Kafka | Internal / external listeners |
| 9870 | HDFS Namenode | Web UI |
| 9864 | HDFS Datanode | Web UI |
| 8080 | Spark Master | Web UI |
| 7077 | Spark Master | RPC |
| 8088 | Airflow Webserver | Airflow UI |
| 9042 | Cassandra | CQL |
| 8501 | Monitoring UI | Pipeline dashboard |

**Persistent volumes:**

- `namenode_data`, `datanode_data`: HDFS data
- `cassandra_data`: Cassandra data
- `airflow_postgres_data`: Airflow metadata database

NiFi khong con nam trong runtime path chinh. Folder `nifi/` duoc giu lai cho future work.

---

## 4. Ingest services

### Weather history

- File: `ingest/ingest_weather.py`
- Kafka topic mặc định: `weather_history`
- Input:
  - Local JSON trong `./data/weather/<province>/<date>.json`, hoặc
  - WeatherAPI history khi cấu hình mode/API key phù hợp
- Output: mỗi bản ghi theo giờ là một JSON event gồm `event_id`, `province`, `query_date`, `event_time`, nhiệt độ, độ ẩm, gió, mưa, điều kiện thời tiết, tọa độ và metadata ingest.

### OpenAQ hourly

- File: `ingest/openaq_ingest.py`
- Kafka topic mặc định: `openaq-hourly`
- Input mặc định trong container: `/data/crawling/openaq_vietnam_hourly.csv`
- Output: mỗi dòng hourly measurement thành một JSON event gồm location, sensor, parameter, unit, value, min/max/sd, coverage và metadata ingest.

### Sentinel-5P summary

- File: `ingest/sentinel5p_ingest.py`
- Service Compose: `sentinel5p-ingest`
- Kafka topic mặc định: `sentinel5p-summary`
- Input: CDSE credentials từ biến môi trường `CDSE_USERNAME`, `CDSE_PASSWORD`
- Output: summary statistics cho các product `NO2`, `CO`, `O3`, `SO2`, `CH4`, `AER` trong bbox cấu hình.

---

## 5. Spark và storage

### Streaming jobs

| Job | Kafka topic | Iceberg table | Checkpoint |
|-----|-------------|---------------|------------|
| `weather_streaming.py` | `weather_history` | `ais.weather.weather_history_bronze` | `hdfs://namenode:9000/checkpoints/weather_history/` |
| `openaq_hourly_streaming.py` | `openaq-hourly` | `ais.air_quality.openaq_hourly_bronze` | `hdfs://namenode:9000/checkpoints/openaq_hourly/` |

Summary streaming jobs (realtime path):

| Job | Kafka topic | Sink |
|-----|-------------|------|
| `sentinel5p_summary_streaming.py` | `sentinel5p-summary` | `hdfs://namenode:9000/data/sentinel5p_summary/` |
| `maiac_summary_streaming.py` | `maiac-summary` | `hdfs://namenode:9000/data/maiac_summary/` |

Iceberg warehouse:

```text
hdfs://namenode:9000/warehouse/iceberg
```

### Cassandra serving

`spark_jobs/iceberg_to_cassandra.py` đọc từ Iceberg và ghi sang keyspace `ais_serving`:

- `weather_hourly_by_province_day`
- `openaq_hourly_by_city_parameter_day`

---

## 6. Hướng dẫn chạy

### Yêu cầu

- Docker Desktop hoặc Docker Engine
- Docker Compose v2
- Tối thiểu 8 GB RAM cho Docker
- Tối thiểu 10 GB disk trống

### 1. Khởi động infrastructure

```bash
docker-compose up -d zookeeper kafka namenode datanode spark-master spark-worker cassandra
docker-compose ps
```

UI hữu ích:

- HDFS Namenode: http://localhost:9870
- Spark Master: http://localhost:8080
- Cassandra: `localhost:9042`

### 2. Tạo Kafka topics

```bash
docker exec kafka kafka-topics --create --bootstrap-server kafka:9092 --replication-factor 1 --partitions 3 --topic weather_history --if-not-exists
docker exec kafka kafka-topics --create --bootstrap-server kafka:9092 --replication-factor 1 --partitions 3 --topic openaq-hourly --if-not-exists
docker exec kafka kafka-topics --create --bootstrap-server kafka:9092 --replication-factor 1 --partitions 3 --topic sentinel5p-summary --if-not-exists
docker exec kafka kafka-topics --create --bootstrap-server kafka:9092 --replication-factor 1 --partitions 3 --topic maiac-summary --if-not-exists
docker exec kafka kafka-topics --list --bootstrap-server kafka:9092
```

### 3. Tạo HDFS paths

```bash
docker exec namenode hdfs dfs -mkdir -p /warehouse/iceberg
docker exec namenode hdfs dfs -mkdir -p /checkpoints/weather_history
docker exec namenode hdfs dfs -mkdir -p /checkpoints/openaq_hourly
docker exec namenode hdfs dfs -chmod -R 777 /warehouse
docker exec namenode hdfs dfs -chmod -R 777 /checkpoints
```

### 4. Chạy ingest

```bash
docker compose build ingest openaq-ingest sentinel5p-ingest maiac-ingest
docker compose run --rm -e WINDOW_MODE=batch -e BATCH_LOOKBACK_DAYS=7 ingest
docker compose run --rm -e WINDOW_MODE=batch -e BATCH_LOOKBACK_DAYS=7 openaq-ingest
```

Sentinel-5P va MAIAC co the chay rieng:

```bash
docker compose run --rm -e WINDOW_MODE=batch -e BATCH_LOOKBACK_DAYS=7 sentinel5p-ingest
docker compose run --rm -e WINDOW_MODE=batch -e BATCH_LOOKBACK_DAYS=30 maiac-ingest
```

### 5. Submit Spark jobs

```bash
bash scripts/submit_spark.sh weather
bash scripts/submit_spark.sh openaq
bash scripts/submit_spark.sh sentinel5p
bash scripts/submit_spark.sh maiac
```

Sau khi có dữ liệu trong Iceberg, load sang Cassandra:

```bash
bash scripts/submit_spark.sh cassandra-weather
bash scripts/submit_spark.sh cassandra-openaq
```

### 6. Chạy Airflow orchestration

```bash
docker-compose up -d airflow-postgres airflow-init airflow-webserver airflow-scheduler airflow-triggerer
```

Airflow UI: http://localhost:8088

Đăng nhập mặc định:

- Username: `admin`
- Password: `admin`

DAG chính: `ais_batch_orchestration`

### 7. Mở Monitoring UI

```bash
docker-compose up -d --build monitoring-ui
```

Monitoring UI: http://localhost:8501

---

## 7. Kiểm tra kết quả

### Kafka

```bash
docker exec kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list kafka:9092 --topic weather_history --time -1
docker exec kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list kafka:9092 --topic openaq-hourly --time -1
docker exec kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list kafka:9092 --topic sentinel5p-summary --time -1
docker exec kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list kafka:9092 --topic maiac-summary --time -1

docker exec kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic weather_history --from-beginning --max-messages 5
docker exec kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic openaq-hourly --from-beginning --max-messages 5
```

### Spark

```bash
docker logs spark-master -f --tail 50
```

Mở Spark UI tại http://localhost:8080 và kiểm tra các application:

- `WeatherHistory_Streaming`
- `OpenAQHourly_Streaming`
- `Sentinel5PSummary_Streaming`
- `MAIACSummary_Streaming`
- `IcebergToCassandra_Weather`
- `IcebergToCassandra_OpenAQ`

### HDFS / Iceberg

```bash
docker exec namenode hdfs dfs -ls -R /warehouse/iceberg
docker exec namenode hdfs dfs -ls -R /checkpoints/weather_history
docker exec namenode hdfs dfs -ls -R /checkpoints/openaq_hourly
```

Mở HDFS Web UI: http://localhost:9870 -> Utilities -> Browse the file system -> `/warehouse/iceberg`.

### Cassandra

---

## 8. Lưu ý thiết kế cho Kubernetes

- Config qua environment variables để có thể chuyển sang `ConfigMap` và `Secret`.
- HDFS, Cassandra và Airflow Postgres dùng named volumes; khi lên Kubernetes cần thay bằng `PVC`/StatefulSet phù hợp.
- Ingest và Spark processing tách riêng, giao tiếp qua Kafka, nên có thể chuyển ingest thành CronJob và Spark thành Spark Operator job.
- Service discovery hiện dùng Docker Compose service names; khi lên Kubernetes cần map sang Service DNS.

---

## Dọn dẹp

```bash
docker-compose down
docker-compose down -v
docker-compose down --rmi local
```
