# AIS Project Full Notes (Detailed)

Updated: 2026-04-19
Project: Atmospheric Intelligence System (AIS)

## 1) Project goal and current achievement

AIS da xay dung duoc mot he thong du lieu khi quyen end-to-end gom:
- Thu thap du lieu da nguon (Weather, OpenAQ, Sentinel-5P, MAIAC).
- Dua du lieu vao Kafka theo su kien chuan hoa.
- Xu ly bang Spark Structured Streaming va luu lich su vao Iceberg tren HDFS.
- Dong bo mot phan du lieu phuc vu truy van nhanh sang Cassandra.
- Dieu phoi va giam sat bang Airflow + Monitoring dashboard.
- Co UI React de demo realtime/historical (dang dung mock data phia frontend).

Kien truc duoc refactor theo huong:
- Iceberg la source-of-truth lich su.
- Cassandra la serving layer do tre thap.
- Airflow tach DAG theo trach nhiem: bootstrap, supervision, backfill, maintenance.

## 2) Dataflow and control flow

Dataflow chinh:
1. Nguon du lieu -> ingest Python adapters.
2. Ingest adapters -> Kafka topics.
3. Spark streaming jobs doc Kafka -> ghi Iceberg tables.
4. Job batch phu tro -> dong bo weather/openaq tu Iceberg sang Cassandra.
5. Monitoring/API + Airflow trigger -> dieu khien backfill va theo doi health.

Control flow chinh:
1. Khoi dong ha tang bang docker compose.
2. Tao topics Kafka, ensure Iceberg tables.
3. Chay streaming sinks (detached).
4. Trigger backfill (Monitoring API hoac script).
5. Airflow supervision kiem tra stream jobs + lag.
6. Airflow maintenance chay compact/expire/orphan/reconcile.

## 3) Full file map and what each area has done

### 3.1 Root files

- `README.md`
  - Tong quan kien truc refactor.
  - Mo ta 4 luong chinh: bootstrap, supervision, MAIAC backfill, maintenance.
  - Mo ta module mapping va runbook.

- `QUICKSTART.md`
  - Huong dan quick run infrastructure-only.
  - Huong dan trigger DAG backfill qua Monitoring UI/API.
  - Huong dan check pipeline status theo tung source.

- `docker-compose.yml`
  - Dinh nghia full stack service:
    - Zookeeper, Kafka
    - HDFS (namenode/datanode)
    - Spark master/worker
    - Ingest services: weather/openaq/sentinel5p/maiac
    - Cassandra
    - Airflow postgres + airflow init/webserver/scheduler/triggerer
    - Monitoring UI backend
  - Truyen env cho window mode, lookback, realtime loop.
  - Mount checkpoints va volumes de persist state.

- `.env.example`
  - Khai bao env cho NiFi/Weather/OpenAQ/CDSE/MAIAC/runner defaults.
  - Da cau hinh bo tham so window mode/realtime/batch cho tung nguon.
  - Luu y van hanh: can de gia tri secrets an toan, khong de key that trong repo.

- `.gitignore`
  - Bo qua artifacts: volumes, checkpoints generated, pycache, logs, env local.

## 3.2 Airflow orchestration (`airflow/`)

- `airflow/Dockerfile`
  - Build image Airflow co Docker CLI + docker compose plugin.
  - Cho phep BashOperator goi docker exec/compose trong DAG.

- `airflow/dags/ais_dag_utils.py`
  - Chua helper command chung:
    - `spark_submit_command`
    - `ensure_topics_command`
    - `ensure_iceberg_tables_command`
    - `ensure_cassandra_schema_command`
    - `compose_ingest_command`
    - `ensure_streaming_job_command`
    - `kafka_lag_check_command`
    - `reconcile_serving_command`
    - `iceberg_maintenance_command`
  - Dinh nghia template lookback cho dag_run.conf.

- `airflow/dags/ais_pipeline_dag.py`
  - DAG `ais_batch_orchestration` (schedule 7 ngay, paused on creation).
  - Chuc nang:
    - ensure topics + Iceberg + Cassandra schema
    - run 4 ingest batch jobs
    - run 4 Spark one-shot catchup (`--stop-after-batch 1`)
    - load weather/openaq sang Cassandra

- `airflow/dags/ais_streaming_supervision_dag.py`
  - DAG supervision moi 15 phut.
  - Chuc nang:
    - ensure topics/tables/schema
    - auto-start stream jobs neu down (`ensure_stream_job.sh`)
    - check kafka lag cho 4 group.

- `airflow/dags/ais_maiac_backfill_dag.py`
  - DAG backfill MAIAC theo ngay.
  - ingest MAIAC batch + spark one-shot MAIAC vao Iceberg.
  - Co hook refresh serving MAIAC nhung hien chua co Cassandra table cho MAIAC.

- `airflow/dags/ais_maintenance_dag.py`
  - DAG maintenance theo ngay.
  - Chay Iceberg maintenance + reconcile Iceberg vs Cassandra.

- `airflow/logs/`
  - Log run cua Airflow scheduler/dag processor/task instances.
  - Day la artifacts runtime, khong phai source code.

- `airflow/plugins/`
  - Hien dang trong (placeholder cho plugin Airflow tuong lai).

## 3.3 Ingestion layer (`ingest/`)

- `ingest/Dockerfile`
  - Build Python image cho ingest services.

- `ingest/requirements.txt`
  - Core deps ingest: pandas, kafka-python, requests, numpy, netCDF4, python-dotenv.

- `ingest/kafka_utils.py`
  - Utility chung Kafka producer:
    - retry ket noi broker
    - serializer JSON UTF-8
    - send single event / bulk events co optional ack.

- `ingest/window_utils.py`
  - Co che cua so thoi gian cho tat ca ingest:
    - `WindowConfig`, `WindowRange`
    - parse mode batch/realtime
    - resolve start/end window
    - persist runtime window state ra file JSON.

- `ingest/ingest_weather.py`
  - Ho tro source mode `api` va `local` (auto switch theo key).
  - Doc WeatherAPI history hoac local weather JSON.
  - Normalize hourly records day du field khi tuong.
  - Gan metadata window va day vao topic Kafka weather.
  - Ho tro realtime continuous mode theo poll interval.

- `ingest/openaq_ingest.py`
  - Lay locations/sensors/hourly tu OpenAQ v3 (Vietnam).
  - Filter bo parameter quan tam (pm25/pm10/no2/o3/co/so2).
  - Build event OpenAQ chuan hoa + coverage + event_id.
  - Push vao topic `openaq-hourly`.

- `ingest/sentinel5p_ingest.py`
  - Authenticate CDSE va query OData metadata theo bbox+window.
  - Ho tro bo product NO2/CO/O3/SO2/CH4/AER.
  - Build summary event theo product (metadata-level, khong bat buoc download full file).
  - Push vao topic `sentinel5p-summary`.

- `ingest/maiac_ingest.py`
  - Crawl NASA CMR granule metadata cho MAIAC (MCD19A2).
  - Parse granule id/tile/acquisition date/download url.
  - Push summary metadata vao topic `maiac-summary`.

- `ingest/batch/`, `ingest/realtime/`
  - Hien dang trong (reserved structure).

## 3.4 Spark processing and storage (`spark_jobs/`)

- `spark_jobs/runtime_utils.py`
  - Parse runtime args cho streaming jobs:
    - `--stop-after-batch`
    - `--processing-time`
  - Chuyen trigger availableNow vs processingTime.

- `spark_jobs/ensure_iceberg_tables.py`
  - Ensure namespace/tables:
    - `ais.weather.weather_history_bronze`
    - `ais.air_quality.openaq_hourly_bronze`
    - `ais.satellite.sentinel5p_summary_bronze`
    - `ais.satellite.maiac_summary_bronze`
  - Dinh nghia partition theo source.

- `spark_jobs/weather_streaming.py`
  - Kafka weather -> parse schema -> cast time -> partition columns -> Iceberg append.
  - Ho tro one-shot catchup va long-running stream.

- `spark_jobs/openaq_hourly_streaming.py`
  - Kafka OpenAQ -> event_time/year/month/day/hour -> Iceberg append.

- `spark_jobs/sentinel5p_summary_streaming.py`
  - Kafka Sentinel summary -> flatten stats -> derive event_time -> Iceberg append.

- `spark_jobs/maiac_summary_streaming.py`
  - Kafka MAIAC summary -> parse date/time -> partition by short_name/year/month/day/tile -> Iceberg append.

- `spark_jobs/sentinel5p_streaming.py`
  - Compatibility wrapper, redirect sang `sentinel5p_summary_streaming.py`.

- `spark_jobs/iceberg_to_cassandra.py`
  - Batch projection tu Iceberg sang Cassandra serving tables cho weather va openaq.

- `spark_jobs/reconcile_iceberg_cassandra.py`
  - Reconciliation check theo lookback window:
    - count distinct key recent records
    - ratio Cassandra/Iceberg >= tolerance.

- `spark_jobs/iceberg_maintenance.py`
  - Chay Iceberg procedures:
    - rewrite_data_files
    - expire_snapshots
    - remove_orphan_files.

- `spark_jobs/entrypoints/`
  - Hien chua co file executable.

## 3.5 Operations scripts (`scripts/`)

- `scripts/run_infrastructure_only.sh`
  - Khoi dong full infrastructure + streaming + Airflow + Monitoring.
  - Khong auto backfill data.

- `scripts/run_full_historical_realtime.sh`
  - One-command runner:
    - start infra
    - ensure topics/tables
    - start streams detached
    - backfill weather/openaq/sentinel/maiac
    - chuyen weather/openaq sang realtime continuous.

- `scripts/run_full_historical_realtime.ps1`
  - Ban PowerShell tuong duong cho Windows.

- `scripts/backfill_all_sources.sh`
  - Manual backfill 4 sources theo LOOKBACK_DAYS.
  - Optional refresh Cassandra serving.

- `scripts/submit_spark.sh`
  - Unified submit script cho:
    - weather/openaq/sentinel5p/maiac streams
    - cassandra-weather/cassandra-openaq loaders
    - ensure-iceberg
    - maintenance-iceberg
    - reconcile-serving
  - Ho tro `DETACH=true` va `STOP_AFTER_BATCH=true`.

- `scripts/check_pipeline.sh`
  - Health check pipeline theo source:
    - Kafka topic/messages sample
    - HDFS data/checkpoint
    - Spark app active
    - consumer lag.

- `scripts/create_topics.sh`
  - Tao 4 topics: weather_history, openaq-hourly, sentinel5p-summary, maiac-summary.

- `scripts/create_topics_openaq.sh`
  - Legacy helper tao rieng topic OpenAQ.

- `scripts/submit_spark_openaq.sh`
  - Legacy helper submit rieng OpenAQ stream.

- `scripts/airflow/ensure_stream_job.sh`
  - Tu dong start lai stream app neu khong thay tren Spark master API.

- `scripts/airflow/check_kafka_lag.sh`
  - Kiem tra lag theo group/topic va fail neu vuot nguong.

## 3.6 Monitoring backend (`monitoring/`)

- `monitoring/app.py`
  - Flask app + HTML dashboard tu render_template_string.
  - API chinh:
    - `GET /api/metrics`: Kafka throughput, message total, HDFS parquet stats, DataNode health.
    - `POST /api/airflow/start-backfill`: unpause + trigger DAG `ais_batch_orchestration`.
    - `POST /api/ingest/trigger?source=...&lookback_days=...`: trigger ingest container background thread.
    - `GET /api/ingest/status`: trang thai ingest background.
    - `GET /healthz`.
  - Co logic fallback `docker compose` vs `docker-compose` khi goi ingest subprocess.

- `monitoring/Dockerfile`
  - Container image cho monitoring app.

- `monitoring/requirements.txt`
  - Flask, kafka-python, requests.

## 3.7 Frontend UI (`ui/`)

### Build and tooling

- `ui/package.json`
  - React + Vite + D3 stack.
- `ui/package-lock.json`
  - Lock dependency tree.
- `ui/vite.config.js`
  - Vite config co plugin React.
- `ui/eslint.config.js`
  - ESLint config cho JS/JSX.
- `ui/index.html`
  - HTML entrypoint.
- `ui/README.md`
  - README mac dinh template Vite.

### Public assets and mock datasets

- `ui/public/favicon.svg`, `ui/public/icons.svg`
  - Static icons.
- `ui/public/mock/realtime-openaq.json`
  - Mock realtime pollution summary + province series.
- `ui/public/mock/realtime-weather.json`
  - Mock realtime weather summary + province series.
- `ui/public/mock/historical-openaq.json`
  - Mock historical pollution records.
- `ui/public/mock/historical-weather.json`
  - Mock historical weather records.
- `ui/public/mock/historical-sentinel.json`
  - Mock historical sentinel product rows.

### Source app

- `ui/src/main.jsx`
  - React root mounting.
- `ui/src/App.jsx`
  - Sidebar navigation giua Realtime/Historical dashboards.
- `ui/src/index.css`
  - Global styling/layout/cards/charts/themes.

Pages:
- `ui/src/pages/RealtimeDashboard.jsx`
  - Polling mock APIs moi 30s.
  - Filter province + metric.
  - Render stat cards, bar charts, line charts.
- `ui/src/pages/HistoricalDashboard.jsx`
  - Chon source OpenAQ/Weather/Sentinel.
  - Filter date range + metric + province.
  - Hien thi bar/line/table tu mock data.

Services:
- `ui/src/services/api.js`
  - Fetch mock JSON endpoints (`/mock/*.json`).

Components:
- `ui/src/components/layout/PageContainer.jsx`
- `ui/src/components/cards/StatCard.jsx`
- `ui/src/components/filters/SourceFilter.jsx`
- `ui/src/components/filters/ProvinceFilter.jsx`
- `ui/src/components/filters/MetricFilter.jsx`
- `ui/src/components/filters/DateRangeFilter.jsx`
- `ui/src/components/charts/SimpleBarChart.jsx`
- `ui/src/components/charts/RealtimeLineChart.jsx`
- `ui/src/components/charts/HistoricalLineChart.jsx`
- `ui/src/components/charts/MultiMetricLineChart.jsx`

Utils:
- `ui/src/utils/constants.js`
  - Metric options cho pollution/weather.
- `ui/src/utils/provinceMap.js`
  - Map province key -> ten hien thi.
- `ui/src/utils/format.js`
  - Number/time format helper.
- `ui/src/utils/dataAdapters.js`
  - Adapter/merge helper cho du lieu weather/openaq.

Current status UI:
- UI frontend hien o trang thai demo + mock data.
- Chua ket noi truc tiep API backend production data cua AIS.

## 3.8 Crawler and exploratory area (`crawler/` and `data/crawling/`)

### `crawler/`

- `crawler/crawl.py`
  - GeoBoundaries crawler cho Vietnam ADM1/ADM2.
  - Tach district Hanoi va export GeoJSON/Shapefile.

- `crawler/dataprocessing.ipynb`
- `crawler/test.ipynb`
  - Notebook phan tich/thu nghiem.

- `crawler/geoBoundaries-VNM-ADM1_simplified.geojson`
- `crawler/geoBoundaries-VNM-ADM2_simplified.geojson`
- `crawler/hanoi_districts_clean.geojson`
  - Geospatial datasets cho bo ranh gioi hanh chinh.

- `crawler/maiac_data/*.hdf`
  - Raw MAIAC granule files (HDF) phuc vu nghien cuu/scan inventory.

### `data/crawling/` (legacy + helper crawlers)

- `data/crawling/crawl.py`
  - OpenAQ crawler xuat `openaq_vietnam_hourly.csv`.
- `data/crawling/crawl-weather.py`
  - WeatherAPI crawler xuat JSON theo tinh/ngay.
- `data/crawling/sentinel5p_hanoi_crawler.py`
  - Sentinel-5P metadata crawler cho Vietnam bbox, xuat JSON/CSV, optional download.
- `data/crawling/sentinnel5p.py`
  - Script visual hoa Sentinel-5P nhieu products (co download + plotting).
- `data/crawling/requirements_sentinel5p.txt`
  - Dependency cho crawler sentinel helper.
- `data/crawling/outputs/sentinel5p_vietnam_last_3d.json`
  - Output metadata crawl mau.

## 3.9 Data and artifacts directories

- `data/weather/`
  - Raw weather JSON theo pattern:
    - `data/weather/<Province>/YYYY-MM-DD.json`
  - Da co bo du lieu lich su nhieu tinh/thanh theo ngay.

- `data/crawling/`
  - Chua script crawl + output crawl.

- `data/sentinel5p_data/` (duoc nhac trong docs mo ta)
  - Khu vuc du kien cho raw Sentinel NetCDF (neu download local).

- `data/weather/` va `crawler/maiac_data/`
  - Dong vai tro nguon raw/artifacts cho pipeline + research.

- `checkpoints/`
  - Runtime state va Spark checkpoints.
  - Ingest window state files duoc persist theo source.

- `airflow/logs/`
  - Runtime execution logs cho Airflow tasks.

## 3.10 Documentation and design notes

- `docs/architecture/refactored_pipeline.md`
  - Tai lieu kien truc refactor va so do.

- `description/datasets.md`
  - Data dictionary chi tiet cho OpenAQ/Weather/Sentinel/MAIAC.

- `description/hanoi_trajectory_pipeline.md`
  - Ghi chep pipeline nghien cuu trajectory cho Hanoi AQ forecast.

## 3.11 Notebooks (`notebooks/`)

- `notebooks/maiac_hdf_locator.ipynb`
  - Notebook tim vi tri/metadata MAIAC HDF.
- `notebooks/clean/maiac_file_inventory.parquet`
  - Artifact inventory cho MAIAC file scan.

## 3.12 Other directories and scaffolding status

- `infra/` (kafka/cassandra/iceberg)
  - Hien chua co file cau hinh cu the trong subtree nay (reserved layout).

- `spark/` (batch/shared/streaming)
  - Hien dang la scaffold, source processing hien tap trung o `spark_jobs/`.

- `orchestration/airflow/`
  - Scaffold folder, DAG production dang nam trong `airflow/dags/`.

- `config/`
  - Hien dang trong.

- `.idea/`
  - IDE metadata.

## 4) Runtime defaults and behavior summary

Windowing defaults (ap dung ingestion):
- `WINDOW_MODE`: `batch` or `realtime`
- `BATCH_LOOKBACK_DAYS`: default 7 (MAIAC co the 30 tuy context)
- `REALTIME_LOOKBACK_MINUTES`: default 10
- `REALTIME_CONTINUOUS`: true/false
- `REALTIME_POLL_SECONDS`: default 600
- `WINDOW_STATE_FILE`: file luu state cua so thoi gian

Kafka topics currently standardized:
- `weather_history`
- `openaq-hourly`
- `sentinel5p-summary`
- `maiac-summary`

Spark streaming app names:
- `WeatherHistory_Streaming`
- `OpenAQHourly_Streaming`
- `Sentinel5PSummary_Streaming`
- `MAIACSummary_Streaming`

Cassandra serving tables:
- `ais_serving.weather_hourly_by_province_day`
- `ais_serving.openaq_hourly_by_city_parameter_day`

## 5) Endpoints and operator UX

Monitoring and control endpoints:
- Monitoring UI: `http://localhost:8501`
- Airflow UI: `http://localhost:8088`
- Spark Master: `http://localhost:8080`
- HDFS NameNode UI: `http://localhost:9870`

Monitoring API:
- `GET /api/metrics`
- `POST /api/airflow/start-backfill`
- `POST /api/ingest/trigger?source=weather|openaq|sentinel5p|maiac&lookback_days=N`
- `GET /api/ingest/status`
- `GET /healthz`

## 6) Key strengths achieved

- Da co full pipeline đa-nguon chay duoc tren local docker stack.
- Da refactor ro trach nhiem orchestration theo 4 DAG.
- Da thong nhat co che window batch/realtime cho ingest.
- Da thong nhat luu lich su vao Iceberg thay vi sink roi rac.
- Da co supervision stream va check Kafka lag.
- Da co maintenance Iceberg va reconcile voi serving layer.
- Da co bo script van hanh nhanh va script health check.
- Da co dashboard monitoring va API trigger backfill one-click.

## 7) Important notes / risks discovered from files

- Secrets hygiene:
  - Trong mot so file mau/legacy dang xuat hien gia tri credentials API/email dang ro.
  - Khuyen nghi rotate keys va thay bang placeholder truoc khi chia se/cong khai.

- UI data source:
  - Frontend React hien dung mock files (`ui/public/mock/*`).
  - Neu can dashboard production, can noi API that toi backend data layer.

- Legacy scripts overlap:
  - Ton tai script crawler/legacy (`data/crawling/*`, `scripts/*_openaq.sh`) song song voi luong refactor.
  - Nen dinh danh script canonical de giam nham lan van hanh.

- Scaffold folders:
  - `infra/`, `spark/`, `orchestration/airflow/`, `config/` chu yeu la khung, chua duoc dung day du trong runtime hien tai.

## 8) Suggested next cleanup path (optional)

1. Tach ro `production path` va `research/legacy path` trong README chinh.
2. Loai bo secrets khoi `.env.example`, tao placeholders an toan.
3. Ket noi `ui/` vao real backend API (thay mock).
4. Gom scripts legacy vao `legacy/` de tranh van hanh nham.
5. Bo sung test matrix cho ingest/spark jobs quan trong.

---

This file is a detailed repository-level implementation note for AIS, covering architecture, modules, workflows, file responsibilities, data artifacts, and operational status.
