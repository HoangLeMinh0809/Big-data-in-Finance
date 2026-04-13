# Big Data Pipeline — Xử lý dữ liệu chứng khoán Việt Nam

## Mục lục
1. [Kiến trúc tổng thể](#1-kiến-trúc-tổng-thể)
2. [Cấu trúc thư mục](#2-cấu-trúc-thư-mục)
3. [Docker Compose](#3-docker-compose)
4. [Python Ingest Service](#4-python-ingest-service)
5. [Spark Structured Streaming](#5-spark-structured-streaming)
6. [Hướng dẫn chạy toàn bộ hệ thống](#6-hướng-dẫn-chạy-toàn-bộ-hệ-thống)
7. [Kiểm tra kết quả](#7-kiểm-tra-kết-quả)
8. [Lưu ý thiết kế cho Kubernetes](#8-lưu-ý-thiết-kế-cho-kubernetes)

---

## 1. Kiến trúc tổng thể

### Pipeline Flow

```
┌──────────────┐    ┌──────────────┐    ┌────────┐    ┌────────────────┐    ┌──────────┐
│  Raw Excel   │───>│ Python       │───>│ Kafka  │───>│ Spark          │───>│  HDFS    │
│  Files       │    │ Ingest       │    │ Broker │    │ Structured     │    │ (Parquet)│
│  (.xlsx)     │    │ Service      │    │        │    │ Streaming      │    │          │
└──────────────┘    └──────────────┘    └────────┘    └────────────────┘    └──────────┘
                          │                                    │
                   Normalize schema                     Parse JSON
                   Convert VN numbers                   Cast types
                   Add metadata                         Add year/month
                   → JSON events                        → Parquet files
```

### Vai trò từng service

| Service | Image | Vai trò |
|---------|-------|---------|
| **Zookeeper** | `confluentinc/cp-zookeeper:7.5.0` | Quản lý cluster metadata cho Kafka |
| **Kafka** | `confluentinc/cp-kafka:7.5.0` | Message broker — nhận events từ ingest, cung cấp cho Spark |
| **HDFS Namenode** | `bde2020/hadoop-namenode` | Quản lý metadata của HDFS (file → block mapping) |
| **HDFS Datanode** | `bde2020/hadoop-datanode` | Lưu trữ dữ liệu thực tế (data blocks) |
| **Spark Master** | `bitnami/spark:3.5` | Điều phối Spark jobs, phân phối tasks |
| **Spark Worker** | `bitnami/spark:3.5` | Thực thi tasks từ Master |
| **Python Ingest** | Custom (python:3.9-slim) | Đọc file Excel → normalize → gửi Kafka |

### Tại sao dùng Docker Compose?

- **Giai đoạn phát triển/prototype**: không cần Kubernetes overhead
- **Chạy local**: 1 lệnh `docker-compose up` khởi động toàn bộ stack
- **Reproducible**: mọi thành viên nhóm chạy cùng 1 môi trường
- **Dễ debug**: logs tập trung, dễ exec vào container
- **Upgrade path rõ ràng**: mỗi service = 1 container, dễ migrate sang K8s sau

---

## 2. Cấu trúc thư mục

```
Big-data-in-Finance/
├── docker-compose.yml              # Orchestration chính
├── .gitignore                      # Git ignore rules
├── company.xls.csv                 # Danh sách mã chứng khoán
│
├── data/                           # DỮ LIỆU THÔ (mounted read-only vào container)
│   └── stock_prices/
│       └── data_raw/
│           ├── ACB.xlsx            # Mỗi file = 1 mã CK
│           ├── VNM.xlsx
│           └── ...
│
├── hadoop/                         # CẤU HÌNH HDFS
│   └── hadoop.env                  # Environment vars cho namenode/datanode
│
├── ingest/                         # PYTHON INGEST SERVICE
│   ├── Dockerfile                  # Docker image definition
│   ├── requirements.txt            # Python dependencies
│   └── ingest.py                   # Main ingest logic
│
├── spark_jobs/                     # SPARK JOBS
│   └── stock_prices_streaming.py   # Structured Streaming job
│
├── scripts/                        # HELPER SCRIPTS
│   ├── create_topics.sh            # Tạo Kafka topics
│   ├── submit_spark.sh             # Submit Spark job
│   └── check_pipeline.sh           # Kiểm tra pipeline health
│
├── checkpoints/                    # SPARK CHECKPOINTS (local fallback)
│   └── .gitkeep
│
└── README.md                       # File này
```

---

## 3. Docker Compose

File `docker-compose.yml` gồm **7 services** trên 1 network chung `bigdata-net`:

- **Health checks**: Zookeeper, Kafka, Namenode đều có healthcheck
- **Dependency chain**: Kafka → Zookeeper, Datanode → Namenode, Ingest → Kafka
- **Volumes**: HDFS data dùng Docker named volumes (persistent)
- **Raw data**: mount read-only (`./data:/data:ro`) vào ingest container

**Ports mở ra host:**

| Port | Service | Mô tả |
|------|---------|-------|
| 2181 | Zookeeper | Client connections |
| 9092 / 29092 | Kafka | Internal / External listeners |
| 9870 | HDFS Namenode | Web UI |
| 9864 | HDFS Datanode | Web UI |
| 8080 | Spark Master | Web UI |
| 7077 | Spark Master | RPC |

---

## 4. Python Ingest Service

### Luồng xử lý

1. **Đọc file**: Duyệt `*.xlsx` trong `/data/stock_prices/data_raw/`
2. **Extract symbol**: Tên file = mã CK (ví dụ `ACB.xlsx` → `ACB`)
3. **Normalize schema**:
   - Map cột tiếng Việt → tiếng Anh
   - Convert số VN (`23,95` → `23.95`)
   - Parse `ThayDoi`: `"-0,6(-2,44 %)"` → `price_change=-0.6`, `price_change_pct=-2.44`
   - Chuẩn hóa ngày: `02/03/2026` → `2026-03-02`
4. **Thêm metadata**: `symbol`, `source`, `ingest_time`, `event_id`
5. **Gửi Kafka**: Mỗi record = 1 JSON message, key = `event_id`

### Schema output (JSON)

```json
{
  "symbol": "ACB",
  "trade_date": "2026-03-02",
  "adjusted_close": 23.95,
  "close_price": 23.95,
  "price_change": -0.6,
  "price_change_pct": -2.44,
  "matched_volume": 19337800,
  "matched_value": 464950795000,
  "negotiated_volume": 0,
  "negotiated_value": 0,
  "open_price": 24.0,
  "high_price": 24.2,
  "low_price": 23.9,
  "source": "sample_excel",
  "ingest_time": "2026-03-05T10:30:00+00:00",
  "event_id": "ACB_2026-03-02"
}
```

### Biến môi trường

| Biến | Default | Mô tả |
|------|---------|-------|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Kafka broker address |
| `KAFKA_TOPIC` | `stock-prices-daily` | Topic để gửi message |
| `DATA_DIR` | `/data/stock_prices/data_raw` | Thư mục chứa file xlsx |
| `MAX_FILES` | `5` | Giới hạn file xử lý (0 = tất cả) |
| `SEND_DELAY_MS` | `10` | Delay giữa mỗi message (ms) |

---

## 5. Spark Structured Streaming

### Luồng xử lý

1. **Đọc từ Kafka**: Subscribe topic `stock-prices-daily`, bắt đầu từ `earliest`
2. **Parse JSON**: Sử dụng `from_json()` với schema cố định
3. **Cast types**: `trade_date` → DateType
4. **Thêm partition cols**: `year`, `month` từ `trade_date`
5. **Ghi Parquet**: Output ra HDFS, partition theo `year/month`
6. **Trigger**: Micro-batch mỗi 30 giây
7. **Checkpoint**: Lưu trên HDFS tại `/checkpoints/stock_prices_daily/`

### HDFS output structure

```
/data/stock_prices_daily/
├── year=2024/
│   ├── month=1/
│   │   └── part-00000-xxx.parquet
│   ├── month=2/
│   │   └── ...
│   └── ...
├── year=2025/
│   └── ...
└── year=2026/
    └── ...
```

---

## 6. Hướng dẫn chạy toàn bộ hệ thống

### Yêu cầu

- **Docker Desktop** (Windows/Mac) hoặc **Docker Engine** (Linux)
- **Docker Compose v2** (đi kèm Docker Desktop)
- Tối thiểu **8 GB RAM** cho Docker
- Tối thiểu **10 GB disk** trống

### Bước 1 — Khởi động infrastructure

```bash
# Di chuyển vào thư mục project
cd Big-data-in-Finance

# Khởi động tất cả services (trừ ingest)
docker-compose up -d zookeeper kafka namenode datanode spark-master spark-worker

# Đợi tất cả services healthy (~30-60 giây)
docker-compose ps
```

Kiểm tra services đã sẵn sàng:
```bash
# Kafka phải hiện "Up (healthy)"
docker-compose ps kafka

# Namenode Web UI: mở browser → http://localhost:9870
# Spark Master UI: mở browser → http://localhost:8080
```

### Bước 2 — Tạo Kafka topic

```bash
# Tạo topic với 3 partitions
docker exec kafka kafka-topics \
  --create \
  --bootstrap-server kafka:9092 \
  --replication-factor 1 \
  --partitions 3 \
  --topic stock-prices-daily \
  --if-not-exists

# Xác nhận topic đã tạo
docker exec kafka kafka-topics --list --bootstrap-server kafka:9092
```

Hoặc chạy script:
```bash
bash scripts/create_topics.sh
```

### Bước 3 — Tạo thư mục HDFS

```bash
docker exec namenode hdfs dfs -mkdir -p /data/stock_prices_daily
docker exec namenode hdfs dfs -mkdir -p /checkpoints/stock_prices_daily
docker exec namenode hdfs dfs -chmod -R 777 /data
docker exec namenode hdfs dfs -chmod -R 777 /checkpoints
```

### Bước 4 — Build và chạy Ingest service

```bash
# Build image
docker-compose build ingest

# Chạy ingest (xử lý 5 file đầu tiên)
docker-compose run --rm ingest

# Hoặc chạy tất cả file:
docker-compose run --rm -e MAX_FILES=0 ingest
```

Xem logs ingest:
```bash
# Nếu chạy ở foreground, logs hiện trực tiếp
# Nếu chạy background:
docker-compose logs -f ingest
```

### Bước 5 — Submit Spark Streaming job

```bash
docker exec spark-master spark-submit \
  --master spark://spark-master:7077 \
  --deploy-mode client \
  --name "StockPricesDaily_Streaming" \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  --conf "spark.hadoop.fs.defaultFS=hdfs://namenode:9000" \
  --conf "spark.driver.memory=1g" \
  --conf "spark.executor.memory=1g" \
  /opt/spark-jobs/stock_prices_streaming.py
```

Hoặc chạy script:
```bash
bash scripts/submit_spark.sh
```

> **Lưu ý**: Lần đầu chạy sẽ mất ~2-3 phút để Spark download Kafka connector JAR. Các lần sau sẽ nhanh hơn nhờ cache.

### Bước 6 — Kiểm tra dữ liệu trên HDFS

```bash
# Liệt kê file Parquet
docker exec namenode hdfs dfs -ls -R /data/stock_prices_daily/

# Xem dung lượng
docker exec namenode hdfs dfs -du -h /data/stock_prices_daily/
```

### Bước 7 — Mở Monitoring UI (Realtime)

```bash
# Build và chạy dashboard service
docker-compose up -d --build monitoring-ui

# Mở UI
# http://localhost:8501
```

Dashboard hiển thị:

- Kafka throughput (messages/second)
- Kafka total messages (tổng offset)
- Số file Parquet + tổng dung lượng trong HDFS
- Trạng thái DataNode (live/dead)
- Cờ `Persisted To DataNode`:
  - `YES`: đã có file parquet trong `/data/stock_prices_daily` và có DataNode live
  - `NO`: chưa có file parquet hoặc DataNode chưa live

### Quick Start — Chạy tất cả

```bash
# 1. Khởi động infrastructure
docker-compose up -d zookeeper kafka namenode datanode spark-master spark-worker

# 2. Đợi services sẵn sàng
echo "Đợi 45 giây cho services khởi động..." && sleep 45

# 3. Tạo topic + thư mục HDFS
bash scripts/create_topics.sh
docker exec namenode hdfs dfs -mkdir -p /data/stock_prices_daily
docker exec namenode hdfs dfs -mkdir -p /checkpoints/stock_prices_daily
docker exec namenode hdfs dfs -chmod -R 777 /data
docker exec namenode hdfs dfs -chmod -R 777 /checkpoints

# 4. Build + chạy ingest
docker-compose build ingest
docker-compose run --rm ingest

# 5. Submit Spark job (mở terminal riêng vì nó block)
bash scripts/submit_spark.sh

# 6. Mở monitoring dashboard
docker-compose up -d --build monitoring-ui
```

### Quick Start (Windows) — 1 lệnh chạy full pipeline + UI

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\start_all.ps1
```

Script sẽ tự động:

- Start tất cả container cần thiết: zookeeper, kafka, namenode, datanode, spark-master, spark-worker, monitoring-ui, ingest
- Tạo Kafka topic `stock-prices-daily` nếu chưa có
- Tạo HDFS directories + cấp quyền
- Submit Spark Structured Streaming job ở background
- Mở sẵn UI monitor tại `http://localhost:8501`

Nếu cần tăng thời gian chờ service healthy:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\start_all.ps1 -WaitSeconds 180
```

---

## 7. Kiểm tra kết quả

### 7.1 Kiểm tra Kafka có message

```bash
# Đếm offset (= số message) trong topic
docker exec kafka kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list kafka:9092 \
  --topic stock-prices-daily \
  --time -1

# Đọc 5 messages đầu tiên
docker exec kafka kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic stock-prices-daily \
  --from-beginning \
  --max-messages 5
```

### 7.2 Kiểm tra Spark đang consume

```bash
# Xem logs Spark
docker logs spark-master -f --tail 50

# Mở Spark UI: http://localhost:8080
# → Xem "Running Applications" → có job "StockPricesDaily_Streaming"
```

### 7.3 Kiểm tra file Parquet trên HDFS

```bash
# Liệt kê cây thư mục output
docker exec namenode hdfs dfs -ls -R /data/stock_prices_daily/

# Xem dung lượng
docker exec namenode hdfs dfs -du -s -h /data/stock_prices_daily/
```

Mở HDFS Web UI: **http://localhost:9870** → Utilities → Browse the file system → `/data/stock_prices_daily/`

### 7.4 Debug khi không thấy dữ liệu

| Vấn đề | Cách kiểm tra | Giải pháp |
|--------|---------------|-----------|
| Kafka không có message | `kafka-console-consumer --from-beginning` | Kiểm tra ingest logs, kafka connectivity |
| Spark không consume | Xem Spark UI → Jobs tab | Kiểm tra Kafka bootstrap servers, topic name |
| HDFS không có file | `hdfs dfs -ls /data/` | Kiểm tra Spark logs, HDFS permissions |
| Ingest lỗi kết nối Kafka | `docker logs ingest` | Đảm bảo Kafka healthy trước khi chạy ingest |
| Spark lỗi download packages | `docker logs spark-master` | Kiểm tra internet, thử pre-download jars |

```bash
# Script kiểm tra toàn bộ pipeline
bash scripts/check_pipeline.sh
```

### 7.5 Theo dõi realtime trên UI

- Mở: **http://localhost:8501**
- Nếu throughput = 0 trong thời gian dài: ingest có thể chưa đẩy thêm message.
- Nếu Kafka total tăng nhưng `Persisted To DataNode = NO`: Spark chưa ghi ra HDFS hoặc HDFS path chưa có file parquet.
- Nếu có lỗi kết nối, dashboard sẽ hiển thị ở dòng cuối phần `Cluster Status`.


---

## 8. Lưu ý thiết kế cho Kubernetes

### 8.1 Config qua Environment Variables

✅ **Đã áp dụng**: Tất cả config đều qua `os.getenv()` / docker-compose `environment`.

**Khi chuyển K8s**: Chuyển thành `ConfigMap` + `Secret`.

### 8.2 Persistent Storage

✅ **Đã áp dụng**: HDFS dùng Docker named volumes, tách biệt data và compute.

**Khi chuyển K8s**: HDFS → `StatefulSet` + `PVC`; Raw data → PVC hoặc S3/MinIO.

### 8.3 Container hóa từng service

✅ **Đã áp dụng**: Mỗi service = 1 container riêng biệt.

**Khi chuyển K8s**: Kafka → Strimzi Operator; Spark → Spark Operator; Ingest → CronJob.

### 8.4 Tách Ingest và Processing

✅ **Đã áp dụng**: Ingest (Python) và Processing (Spark) tách biệt, giao tiếp qua Kafka.

### 8.5 Không hard-code path/network

✅ **Đã áp dụng**: Dùng service names (DNS), env vars cho mọi endpoint.

---

## Dọn dẹp

```bash
# Dừng tất cả
docker-compose down

# Dừng + xóa volumes (XÓA DATA!)
docker-compose down -v

# Xóa images
docker-compose down --rmi local
```
