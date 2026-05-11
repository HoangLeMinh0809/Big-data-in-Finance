# AIS - Quick Start with On-Demand Backfill

## 🚀 Khởi động hệ thống (Infrastructure only)

```bash
bash scripts/run_infrastructure_only.sh
```

**Điều này sẽ:**
- ✅ Start Kafka, HDFS, Spark, Cassandra
- ✅ Tạo Kafka topics
- ✅ Ensure Iceberg catalog/tables
- ✅ Submit 4 long-running Spark streaming jobs
- ✅ Start Airflow + Monitoring UI
- ❌ **KHÔNG** backfill dữ liệu tự động (chỉ 1-2 phút)

## 📊 Truy cập Monitoring UI

Mở trình duyệt:
- **Monitoring Dashboard:** http://localhost:8501
- **HDFS:** http://localhost:9870
- **Spark Master:** http://localhost:8080
- **Airflow:** http://localhost:8088

## 📥 Trigger Backfill Data

### Cách 1: Từ Monitoring UI (khuyến nghị)
1. Mở http://localhost:8501
2. Bấm nút "Start 7-Day Backfill DAG"
3. Monitoring UI sẽ gọi Airflow API để unpause + trigger DAG `ais_batch_orchestration` (historical bootstrap)
4. Theo dõi progress trên Airflow UI và dashboard

### Cách 2: Từ API
```bash
# Trigger Airflow DAG (lookback mặc định 7 ngày)
curl -X POST 'http://localhost:8501/api/airflow/start-backfill'
```

### Cách 3: Script manual
```bash
# Backfill all sources với 7 days (default)
bash scripts/backfill_all_sources.sh

# Custom lookback days
LOOKBACK_DAYS=14 bash scripts/backfill_all_sources.sh
```

## ⏱️ Timeline

| Bước | Thời gian |
|------|----------|
| Start infrastructure | ~2-3 phút |
| Backfill 7 days (1 source) | ~5-15 phút (tùy nguồn) |
| Backfill 7 days (all 4 sources) | ~30-60 phút total |

## 📋 Kiểm tra Pipeline Status

```bash
bash scripts/check_pipeline.sh weather
bash scripts/check_pipeline.sh openaq
bash scripts/check_pipeline.sh sentinel5p
bash scripts/check_pipeline.sh maiac
```

## 🛑 Dừng hệ thống

```bash
docker compose down
```

## 📌 Ghi chú

- **Lookback mặc định:** 7 ngày cho mỗi vòng DAG
- **DAGs sau refactor:**
	- `ais_batch_orchestration` (historical bootstrap)
	- `ais_streaming_supervision` (stream supervision)
	- `ais_maiac_backfill` (delayed batch backfill)
	- `ais_maintenance` (maintenance + reconciliation)
- **Cassandra serving tables:** Tự tạo khi startup (weather + openaq)
- **Monitoring API trigger:** `/api/airflow/start-backfill`

## 🔗 Liên quan

- [run_infrastructure_only.sh](run_infrastructure_only.sh) - Script khởi động chính
- [backfill_all_sources.sh](backfill_all_sources.sh) - Script backfill manual
- [monitoring/app.py](../monitoring/app.py) - Backend API trigger Airflow DAG
