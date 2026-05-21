# TODO 3 - Kubernetes Compute Layer for Spark, ML Inference, and PM2.5 Serving

Mục tiêu của TODO3 là chuyển **tầng thực thi compute** của AIS sang Kubernetes, trong khi tầng lưu trữ và dữ liệu trạng thái vẫn nằm bên ngoài Kubernetes trong pha này.

Kiến trúc đích của TODO3:

- Tầng dưới chỉ giữ vai trò storage/data infrastructure: Kafka/Zookeeper, HDFS hoặc object storage nếu đã cấu hình, Iceberg warehouse/catalog, Cassandra nếu còn dùng làm serving/storage backend, Airflow metadata database, model artifact storage, checkpoint/warehouse volumes.
- Kubernetes là runtime đích cho compute: Spark driver/executor pods, Spark batch jobs, HYSPLIT/trajectory jobs nếu container hóa được, feature engineering, ML training, ML inference Job/CronJob, PM2.5 API, data quality/check jobs, và các workload monitoring/check phù hợp.
- Airflow vẫn là control plane: định nghĩa dependency, rerun, backfill, schedule policy; Kubernetes chỉ thực thi workload.
- Docker Compose Spark master/worker chỉ còn là fallback local/dev trong quá trình migration, không còn là runtime đích.

Quy tắc request-time bắt buộc:

```text
API request -> đọc prediction mới nhất đã materialize -> JSON response
```

API không được chạy Spark, HYSPLIT, feature engineering, training, hoặc model inference trong request handling.

## 0. Architecture decision

TODO3 được đổi từ phạm vi cũ “serving/inference trên K8s” sang “compute workloads trên K8s” vì phạm vi cũ vẫn để Spark batch chạy trên Docker Compose và chưa đặt Spark-on-K8s làm target bắt buộc. Cách đó làm kiến trúc bị lệch tầng: API/inference nằm trên Kubernetes, nhưng feature engineering, training và HYSPLIT vẫn phụ thuộc Spark standalone trong Compose. Kết quả là Airflow, Spark, ML và API có nhiều runtime khác nhau, khó backfill, khó kiểm soát tài nguyên và khó triển khai nhất quán.

Quyết định mới:

- [ ] Docker Compose được phép tiếp tục chạy **storage/data dependencies** trong pha này: Kafka, Zookeeper, Namenode, Datanode, Cassandra, Airflow metadata DB, và các volume warehouse/checkpoint.
- [ ] Docker Compose Spark master/worker không còn là runtime đích. Chúng chỉ được giữ làm fallback local/dev để không phá TODO1/TODO2 trong lúc migration.
- [ ] Spark-on-Kubernetes là yêu cầu bắt buộc của TODO3, không còn là optional/future item.
- [ ] Spark driver và Spark executor phải chạy dưới dạng Kubernetes pods.
- [ ] Không migrate Kafka/HDFS/Iceberg/Cassandra/Airflow metadata DB vào Kubernetes trong TODO3.
- [ ] Không đổi business logic TODO1/TODO2 ngoài các thay đổi cần thiết để submit workload compute lên Kubernetes.

Tiêu chí chấp nhận:

- TODO3 nhất quán rằng Spark-on-K8s là target runtime bắt buộc.
- Mọi workload compute mới hoặc được migrate đều có đường chạy Kubernetes.
- Tài liệu rollback/dev fallback nêu rõ Compose Spark chỉ là fallback, không phải target runtime.

## 1. Current-state inventory

Inventory sau khi scan repo hiện tại:

### 1.1 Docker Compose services

File hiện có: `docker-compose.yml` - **modify** nếu cần thêm fallback/dev config hoặc tách profile, nhưng không migrate storage vào K8s trong TODO3.

Services hiện có:

- `zookeeper`
- `kafka`
- `namenode`
- `datanode`
- `spark-master`
- `spark-worker`
- `ingest`
- `openaq-ingest`
- `maiac-ingest`
- `sentinel5p-ingest`
- `monitoring-ui`
- `cassandra`
- `airflow-postgres`
- `airflow-init`
- `airflow-webserver`
- `airflow-scheduler`
- `airflow-triggerer`

Volumes hiện có:

- `namenode_data`
- `datanode_data`
- `cassandra_data`
- `airflow_postgres_data`
- `spark_ivy_cache`

### 1.2 Existing Spark runtime

- `spark/Dockerfile` - **modify** hoặc dùng làm base cho `ais-spark-runtime`.
- Docker Compose đang build image `ais-spark:3.5.3`.
- `docker-compose.yml` chạy `spark-master` và `spark-worker`.
- `scripts/submit_spark.sh` - **modify** hoặc tách thêm script/template K8s, vì hiện submit vào `spark://spark-master:7077`.
- `airflow/dags/ais_dag_utils.py` - **modify**, vì hiện tạo command `docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077`.

### 1.3 Existing Spark jobs

Thư mục hiện có: `spark_jobs/`

Các job đã có:

- `spark_jobs/ensure_iceberg_tables.py`
- `spark_jobs/weather_streaming.py`
- `spark_jobs/openaq_hourly_streaming.py`
- `spark_jobs/sentinel5p_summary_streaming.py`
- `spark_jobs/maiac_summary_streaming.py`
- `spark_jobs/era5_files_streaming.py`
- `spark_jobs/hanoi_openaq_silver.py`
- `spark_jobs/hanoi_weather_surface_proxy_silver.py`
- `spark_jobs/era5_surface_hanoi_silver.py`
- `spark_jobs/sentinel5p_hanoi_silver.py`
- `spark_jobs/maiac_hanoi_silver.py`
- `spark_jobs/era5_pressure_levels_to_arl.py`
- `spark_jobs/hysplit_trajectory_run.py`
- `spark_jobs/hysplit_trajectory_parse_silver.py`
- `spark_jobs/hysplit_trajectory_cluster_silver.py`
- `spark_jobs/openaq_spatial_gradient_silver.py`
- `spark_jobs/sentinel5p_grid_silver.py`
- `spark_jobs/trajectory_path_sampling_silver.py`
- `spark_jobs/trajectory_hourly_features_silver.py`
- `spark_jobs/hanoi_pm25_master_features_gold.py`
- `spark_jobs/hanoi_pm25_training_dataset_gold.py`
- `spark_jobs/iceberg_to_cassandra.py`
- `spark_jobs/reconcile_iceberg_cassandra.py`
- `spark_jobs/iceberg_maintenance.py`
- `spark_jobs/runtime_utils.py`

Job cần tạo trong TODO3:

- `spark_jobs/hanoi_pm25_serving_features_gold.py` - **create**
- `spark_jobs/hanoi_pm25_serving_quality_checks.py` - **create optional** nếu check cần chạy bằng Spark.

### 1.4 Existing ML training

- `ml/train_hanoi_pm25.py` - **modify** để chạy tốt trong Kubernetes Job và xuất artifact URI/registry metadata rõ hơn.
- Script hiện có `FEATURE_COLUMNS`, `TARGETS` 6/12/24, đọc `ais.features.hanoi_pm25_training_dataset_gold`, ghi `ais.models.hanoi_pm25_model_runs_gold`.
- Script hiện ghi model artifact mặc định vào `models/hanoi_pm25` hoặc `/opt/models/hanoi_pm25`.

Files cần tạo:

- `ml/Dockerfile` - **create** nếu chưa có.
- `ml/predict_hanoi_pm25.py` - **create** vì hiện chưa có inference script.
- `ml/promote_hanoi_pm25_model.py` - **create optional** để promote model run sang production registry.

### 1.5 Existing inference/API

- `ml/predict_hanoi_pm25.py` - **create**, hiện chưa có.
- `serving/pm25_api/` - **create**, hiện chưa thấy thư mục `serving/`.
- `deploy/k8s/api/` - **create**, vì `deploy/` hiện chưa tồn tại.

### 1.6 Existing Airflow DAGs

Thư mục hiện có: `airflow/dags/`

Files hiện có:

- `airflow/dags/ais_dag_utils.py` - **modify** để thêm submit K8s/Spark-on-K8s.
- `airflow/dags/ais_pipeline_dag.py`
- `airflow/dags/ais_streaming_supervision_dag.py`
- `airflow/dags/ais_maintenance_dag.py`
- `airflow/dags/ais_era5_ingestion_dag.py`
- `airflow/dags/ais_hanoi_silver_gold_dag.py` - **modify** hoặc giữ làm dev fallback, vì hiện gọi `scripts/submit_spark.sh`.
- `airflow/dags/ais_trajectory_tier2_dag.py` - **modify** hoặc giữ làm dev fallback, vì hiện gọi `scripts/submit_spark.sh`.
- `airflow/dags/ais_maiac_backfill_dag.py`

File cần tạo cho TODO3:

- `airflow/dags/ais_pm25_k8s_compute_dag.py` - **create**

### 1.7 Existing config files

- `config/hanoi_pipeline.yaml` - **modify** để thêm serving, registry, inference, K8s runtime config nếu cần.
- `spark_jobs/hanoi_config.py` - **modify** để thêm table names/config keys cho serving features, prediction, model registry.
- `.env.example` - **modify optional** để ghi biến môi trường K8s/storage endpoint mẫu, không chứa secret thật.

### 1.8 Deploy/infra/monitoring

- `deploy/` - **create**, hiện chưa tồn tại.
- `deploy/k8s/` - **create**.
- `infra/` - hiện tồn tại nhưng chưa có file runtime rõ ràng; **verify existing file/path** trước khi dùng làm nơi đặt manifest.
- `monitoring/app.py` - **modify optional** để thêm trạng thái serving feature/prediction/API/K8s job.
- `monitoring/Dockerfile`, `monitoring/requirements.txt` - hiện có.

### 1.9 Cassandra serving usage

Hiện Cassandra được dùng cho serving weather/openaq:

- `spark_jobs/iceberg_to_cassandra.py` - hardcode `CASSANDRA_HOST = "cassandra"`; **modify later** nếu đưa job này lên K8s.
- `spark_jobs/reconcile_iceberg_cassandra.py` - hardcode `CASSANDRA_HOST = "cassandra"`; **modify later** nếu đưa check này lên K8s.
- `airflow/dags/ais_dag_utils.py` tạo Cassandra schema cho `ais_serving.weather_hourly_by_province_day` và `ais_serving.openaq_hourly_by_city_parameter_day`.

Chưa thấy Cassandra forecast serving table cho PM2.5. TODO3 mặc định dùng Iceberg prediction table làm PM2.5 serving contract; Cassandra chỉ dùng nếu có quyết định riêng.

### 1.10 Existing model/training output tables

Đã có:

- `ais.features.hanoi_pm25_master_hourly_gold`
- `ais.features.hanoi_pm25_training_dataset_gold`
- `ais.models.hanoi_pm25_model_runs_gold`

Cần tạo:

- `ais.features.hanoi_pm25_serving_features_gold`
- `ais.predictions.hanoi_pm25_forecast_gold`
- `ais.models.hanoi_pm25_model_registry_gold`
- Namespace `ais.predictions`

## 2. Target architecture boundary

| Component | Layer | Runtime sau TODO3 | Notes |
|---|---|---|---|
| Kafka | Storage/data infrastructure | Docker Compose hoặc external endpoint | Không migrate vào K8s trong TODO3. Endpoint qua `KAFKA_BOOTSTRAP_SERVERS`, không hardcode `kafka:9092` trong source. |
| Zookeeper | Storage/data infrastructure | Docker Compose hoặc external endpoint | Chỉ phục vụ Kafka hiện tại. Không migrate. |
| HDFS Namenode | Storage/data infrastructure | Docker Compose hoặc external endpoint | Không migrate. K8s pod kết nối qua `HDFS_NAMENODE`/`HDFS_WEBHDFS_BASE`. |
| HDFS Datanode | Storage/data infrastructure | Docker Compose hoặc external endpoint | Không migrate. Phải verify pod có thể đọc/ghi đường dẫn warehouse/checkpoint. |
| Iceberg warehouse/catalog | Storage/data infrastructure | External to K8s | Warehouse hiện là Hadoop catalog trên HDFS theo `ICEBERG_WAREHOUSE`. Catalog config qua env. |
| Cassandra | Storage/serving backend | Docker Compose hoặc external endpoint | Chỉ dùng nếu job cần weather/openaq serving hoặc nếu sau này chọn Cassandra cho PM2.5. Không migrate trong TODO3. |
| Airflow metadata DB | Storage/control-plane state | Docker Compose Postgres | Không migrate. |
| Airflow scheduler/webserver/triggerer | Control plane | Docker Compose trong pha này | Airflow orchestrates, Kubernetes executes compute. Có thể submit K8s workload từ Airflow. |
| Spark driver/executors | Compute | Kubernetes pods | Bắt buộc. Không dùng `spark-master`/`spark-worker` làm target runtime. |
| Spark batch jobs | Compute | Spark-on-K8s | Gồm ensure table, silver/gold, trajectory, serving features, checks. |
| HYSPLIT jobs | Compute | Kubernetes pods qua Spark-on-K8s hoặc Kubernetes Job | Nếu chạy trong Spark job thì driver/executor pod phải có binary/dependency. |
| ML training | Compute | Kubernetes Job | `ml/train_hanoi_pm25.py` chạy trong `ais-ml-runtime`, artifact lưu ngoài K8s. |
| ML inference | Compute | Kubernetes Job/CronJob | `ml/predict_hanoi_pm25.py` đọc serving features + production registry, ghi prediction table. |
| PM2.5 API | Compute/serving | Kubernetes Deployment + Service | Chỉ đọc prediction table. Không chạy compute nặng trong request. |
| monitoring/check jobs | Compute/check | Kubernetes Job/CronJob khi phù hợp | Có thể giữ monitoring UI hiện tại; check jobs mới nên có K8s path. |
| model artifact storage | Storage/data infrastructure | External volume/HDFS/object storage | Không bake model vào image; không lưu artifact chỉ trong ephemeral pod. |

Tiêu chí chấp nhận:

- Spark driver/executors hiện diện trong `kubectl get pods`.
- Không có checkpoint hoàn thành nào yêu cầu Spark standalone Compose làm runtime đích.
- Tài liệu nêu rõ thành phần nào nằm ngoài K8s và vì sao.

## 3. Target data flow

Luồng end-to-end sau TODO3:

```text
External sources
-> ingest producers
-> Kafka/HDFS/Iceberg storage layer
-> Spark jobs on Kubernetes
-> Iceberg Bronze/Silver/Gold
-> Hanoi PM2.5 master hourly gold
-> PM2.5 serving features gold
-> ML inference Job/CronJob on Kubernetes
-> PM2.5 prediction table
-> PM2.5 API Deployment on Kubernetes
-> dashboard/user
```

Chi tiết:

- Ingest producers hiện có có thể tiếp tục chạy bằng Compose trong pha này.
- Kafka/HDFS/Iceberg là tầng dữ liệu bên dưới, không phải workload compute của TODO3.
- Spark jobs chạy trên Kubernetes tạo/refresh Bronze/Silver/Gold và serving feature table.
- `ais.features.hanoi_pm25_master_hourly_gold` là source chính cho serving features.
- `ais.features.hanoi_pm25_serving_features_gold` chỉ chứa feature biết tại `base_hour`, không chứa target tương lai.
- `ml/predict_hanoi_pm25.py` chạy bằng Kubernetes Job/CronJob, dùng production model registry, ghi `ais.predictions.hanoi_pm25_forecast_gold`.
- `serving/pm25_api/` chạy bằng Kubernetes Deployment, đọc `ais.predictions.hanoi_pm25_forecast_gold`.

Luồng request-time:

```text
API request
-> prediction table lookup
-> JSON response
```

Cấm trong request path:

- Không submit Spark.
- Không chạy HYSPLIT.
- Không build serving features.
- Không train model.
- Không load raw Kafka/HDFS để tự tính feature.
- Không chạy model inference trong handler.

Tiêu chí chấp nhận:

- API có thể trả forecast khi prediction table đã có row production.
- API trả lỗi 404 rõ ràng khi chưa có prediction.
- Không có code path API gọi Spark, HYSPLIT, training hoặc inference runtime.

## 4. Kubernetes base

Manifest layout đích:

```text
deploy/k8s/
  namespace.yaml
  configmap.yaml
  secret.example.yaml
  serviceaccount.yaml
  rbac.yaml
  kustomization.yaml

deploy/k8s/spark/
  spark-serviceaccount.yaml
  spark-rbac.yaml
  spark-submit-template.yaml
  spark-application-template.yaml        # optional nếu chọn Spark Operator sau này
  README.md

deploy/k8s/ml/
  pm25-train-job.yaml
  pm25-predict-cronjob.yaml
  pm25-predict-job.yaml                  # optional/manual run
  README.md

deploy/k8s/api/
  pm25-api-deployment.yaml
  pm25-api-service.yaml
  README.md

deploy/k8s/checks/
  pm25-serving-check-job.yaml
  README.md
```

### 4.1 Base manifests

Files:

- `deploy/k8s/namespace.yaml` - **create**
- `deploy/k8s/configmap.yaml` - **create**
- `deploy/k8s/secret.example.yaml` - **create**
- `deploy/k8s/serviceaccount.yaml` - **create**
- `deploy/k8s/rbac.yaml` - **create**
- `deploy/k8s/kustomization.yaml` - **create**

Mục đích:

- Tạo namespace `ais`.
- Tạo ConfigMap cho endpoint storage, table names, runtime defaults.
- Tạo Secret mẫu không chứa secret thật.
- Tạo ServiceAccount/RBAC tối thiểu cho Job/API đọc ConfigMap/Secret và ghi log.

Env vars bắt buộc:

```text
KAFKA_BOOTSTRAP_SERVERS
HDFS_NAMENODE
HDFS_WEBHDFS_BASE
ICEBERG_CATALOG
ICEBERG_CATALOG_URI
ICEBERG_WAREHOUSE
CASSANDRA_HOST
MODEL_ARTIFACT_BASE_URI
HANOI_PIPELINE_CONFIG
FEATURE_VERSION
FEATURE_SET_NAME
LOCATION_ID
LOCATION_NAME
```

Secrets:

- `CDS_KEY` nếu K8s chạy ERA5 ingest sau này.
- `OPENAQ_API_KEY` nếu K8s chạy ingest sau này.
- `CDSE_USERNAME`, `CDSE_PASSWORD` nếu K8s chạy Sentinel ingest sau này.
- Object storage credentials nếu `MODEL_ARTIFACT_BASE_URI` là S3/GCS/Azure.
- Không đưa secret thật vào `secret.example.yaml`.

Resource defaults:

- CPU/memory requests nhỏ cho API/check pod.
- CPU/memory requests lớn hơn cho Spark driver/executor và ML job.
- Mọi manifest phải có `resources.requests` và `resources.limits`, kể cả giá trị dev ban đầu.

Smoke test:

```bash
kubectl apply -k deploy/k8s
kubectl get ns ais
kubectl -n ais get configmap,secret,serviceaccount
```

Acceptance criteria:

- `kubectl apply -k deploy/k8s` chạy được sau khi file được tạo.
- ConfigMap chứa endpoint không hardcode trong source.
- Secret mẫu không chứa giá trị thật.

### 4.2 Spark manifests

Files:

- `deploy/k8s/spark/spark-serviceaccount.yaml` - **create**
- `deploy/k8s/spark/spark-rbac.yaml` - **create**
- `deploy/k8s/spark/spark-submit-template.yaml` - **create**
- `deploy/k8s/spark/spark-application-template.yaml` - **create optional**
- `deploy/k8s/spark/README.md` - **create**

Mục đích:

- Cấp quyền cho Spark driver tạo executor pods.
- Chuẩn hóa command `spark-submit --master k8s://...`.
- Ghi rõ cách truyền Iceberg/HDFS/Kafka config vào driver/executor.

Env vars:

```text
SPARK_K8S_MASTER
SPARK_K8S_NAMESPACE
SPARK_DRIVER_SERVICE_ACCOUNT
SPARK_IMAGE
SPARK_EXECUTOR_INSTANCES
SPARK_EXECUTOR_MEMORY
SPARK_EXECUTOR_CORES
SPARK_DRIVER_MEMORY
ICEBERG_CATALOG
ICEBERG_WAREHOUSE
HDFS_NAMENODE
KAFKA_BOOTSTRAP_SERVERS
```

Secrets:

- Registry pull secret nếu image không public.
- Artifact/object storage secret nếu Spark job cần đọc/ghi model hoặc data.

Resources:

- Driver dev default: request `500m` CPU, `1Gi` memory; limit `1` CPU, `2Gi`.
- Executor dev default: request `500m` CPU, `1Gi` memory; limit `1` CPU, `2Gi`.
- TODO3 phải yêu cầu tune theo job size, không hardcode mãi giá trị dev.

Smoke test:

```bash
kubectl -n ais get sa spark
kubectl -n ais auth can-i create pods --as=system:serviceaccount:ais:spark
```

Acceptance criteria:

- Spark driver pod có quyền tạo executor pods.
- Spark README có lệnh submit tối thiểu.
- Không phụ thuộc `spark-master` Compose.

### 4.3 ML manifests

Files:

- `deploy/k8s/ml/pm25-train-job.yaml` - **create**
- `deploy/k8s/ml/pm25-predict-cronjob.yaml` - **create**
- `deploy/k8s/ml/pm25-predict-job.yaml` - **create optional**
- `deploy/k8s/ml/README.md` - **create**

Mục đích:

- Chạy training one-off bằng Kubernetes Job.
- Chạy inference định kỳ bằng Kubernetes CronJob.
- Cho phép manual run với `kubectl create job --from=cronjob/...`.

Env vars:

```text
DATASET_VERSION
FEATURE_SET_NAME
FEATURE_VERSION
MODEL_TYPE
MODEL_ARTIFACT_BASE_URI
MODEL_RUNS_TABLE
MODEL_REGISTRY_TABLE
SERVING_FEATURE_TABLE
PREDICTION_TABLE
ICEBERG_CATALOG
ICEBERG_WAREHOUSE
HDFS_NAMENODE
LOCATION_ID
```

Secrets:

- Artifact storage credentials.
- Catalog credentials nếu dùng REST catalog trong tương lai.

Resources:

- Training job cần request/limit cao hơn inference.
- Inference job có `concurrencyPolicy: Forbid`, `backoffLimit` nhỏ, history limits rõ.

Smoke test:

```bash
kubectl -n ais apply -f deploy/k8s/ml/pm25-predict-job.yaml
kubectl -n ais logs job/pm25-predict
```

Acceptance criteria:

- Job đọc ConfigMap/Secret.
- Inference dry-run có log đầy đủ.
- CronJob không chạy song song nếu lần trước chưa xong.

### 4.4 API manifests

Files:

- `deploy/k8s/api/pm25-api-deployment.yaml` - **create**
- `deploy/k8s/api/pm25-api-service.yaml` - **create**
- `deploy/k8s/api/README.md` - **create**

Mục đích:

- Chạy PM2.5 API long-running.
- Expose nội bộ cluster bằng Service.
- Probe health/readiness.

Env vars:

```text
API_PORT
PREDICTION_TABLE
ICEBERG_CATALOG
ICEBERG_WAREHOUSE
LOCATION_ID
READINESS_TIMEOUT_SECONDS
```

Secrets:

- Catalog/object storage credentials nếu API query qua REST/object store.

Resources:

- Dev default: request `100m` CPU, `256Mi`; limit `500m` CPU, `512Mi`.
- Readiness/liveness probes không được quá đắt.

Smoke test:

```bash
kubectl -n ais port-forward svc/pm25-api 8081:80
curl -i http://localhost:8081/healthz
curl -i http://localhost:8081/readyz
```

Acceptance criteria:

- `/healthz` trả 200 khi process alive.
- `/readyz` trả 200 khi config/catalog/table sẵn sàng, 503 khi dependency lỗi.
- Forecast endpoint chỉ đọc prediction table.

### 4.5 Checks manifests

Files:

- `deploy/k8s/checks/pm25-serving-check-job.yaml` - **create**
- `deploy/k8s/checks/README.md` - **create**

Mục đích:

- Chạy check freshness/schema/model registry/API readiness bằng Kubernetes Job.

Env vars:

```text
SERVING_FEATURE_TABLE
PREDICTION_TABLE
MODEL_REGISTRY_TABLE
FEATURE_FRESHNESS_MAX_MINUTES
PREDICTION_FRESHNESS_MAX_MINUTES
PM25_API_BASE_URL
```

Smoke test:

```bash
kubectl -n ais apply -f deploy/k8s/checks/pm25-serving-check-job.yaml
kubectl -n ais logs job/pm25-serving-check
```

Acceptance criteria:

- Job exit code 0 khi mọi check pass.
- Job exit code khác 0 khi missing production model, stale prediction, schema mismatch, hoặc API readiness fail.

## 5. K8s-to-storage connectivity

Checkpoint này đảm bảo pod Kubernetes kết nối được tới tầng storage/data bên ngoài K8s.

Files cần tạo/sửa:

- `deploy/k8s/configmap.yaml` - **create**
- `deploy/k8s/secret.example.yaml` - **create**
- `deploy/k8s/spark/README.md` - **create**
- `deploy/k8s/ml/README.md` - **create**
- `deploy/k8s/api/README.md` - **create**
- `spark_jobs/hanoi_config.py` - **modify** để không phụ thuộc hostname Compose mặc định nếu env đã cung cấp.
- `spark_jobs/iceberg_to_cassandra.py` - **modify later nếu migrate job này**, vì hiện hardcode `CASSANDRA_HOST = "cassandra"`.
- `spark_jobs/reconcile_iceberg_cassandra.py` - **modify later nếu chạy trên K8s**, vì hiện hardcode `CASSANDRA_HOST = "cassandra"`.

Config bắt buộc:

```text
KAFKA_BOOTSTRAP_SERVERS
HDFS_NAMENODE
HDFS_WEBHDFS_BASE
ICEBERG_CATALOG
ICEBERG_CATALOG_URI
ICEBERG_WAREHOUSE
CASSANDRA_HOST
MODEL_ARTIFACT_BASE_URI
AIRFLOW_K8S_NAMESPACE
AIRFLOW_K8S_SERVICE_ACCOUNT
```

Quy tắc:

- [ ] Không hardcode `kafka`, `namenode`, `spark-master`, `cassandra` trực tiếp trong source code mới.
- [ ] Các giá trị default local có thể còn trong fallback, nhưng production/K8s path phải override qua env/ConfigMap.
- [ ] Nếu dùng Docker Desktop/local K8s, phải document cách pod tới Compose services:
  - `host.docker.internal` nếu Docker Desktop hỗ trợ.
  - Host IP cụ thể nếu dùng Linux/minikube/kind.
  - NodePort hoặc port publish từ Compose.
  - Shared Docker network nếu cluster local cho phép.
- [ ] Không ghi endpoint environment-specific vào image.

Debug pod checklist:

```bash
kubectl -n ais run debug-net --rm -it --image=busybox:1.36 -- sh
```

Trong debug pod, verify:

- DNS/connectivity Kafka:

```sh
nc -vz "$KAFKA_HOST" "$KAFKA_PORT"
```

- WebHDFS:

```sh
wget -qO- "$HDFS_WEBHDFS_BASE/?op=LISTSTATUS"
```

- Iceberg catalog/warehouse:

```sh
# Nếu dùng Hadoop catalog: chạy bằng Spark smoke job để verify warehouse.
# Nếu dùng REST catalog: curl ICEBERG_CATALOG_URI.
```

- Read/write test table nếu có thể:

```bash
kubectl -n ais logs <spark-driver-pod>
```

Acceptance criteria:

- Một Kubernetes pod đọc được config từ ConfigMap/Secret.
- Một Kubernetes pod kết nối được Kafka endpoint cần dùng.
- Một Kubernetes pod truy cập được HDFS/WebHDFS hoặc warehouse endpoint.
- Spark-on-K8s job đọc/ghi được một Iceberg table nhỏ hoặc chạy read check.
- Không có source code mới hardcode hostname theo Docker Compose.

## 6. Container images

Images bắt buộc:

- `ais-spark-runtime`
- `ais-ml-runtime`
- `ais-pm25-api`
- `ais-checks-runtime` optional nếu checks tách riêng khỏi ML/API image.

Quy tắc chung:

- [ ] Không bake secrets vào image.
- [ ] Image tag có convention: `ais-spark-runtime:<git-sha|date|semver>`, `ais-ml-runtime:<git-sha|date|semver>`, `ais-pm25-api:<git-sha|date|semver>`.
- [ ] Config mount/read qua env hoặc ConfigMap.
- [ ] Image local dev và image K8s dùng cùng command contract.

### 6.1 `ais-spark-runtime`

Dockerfile path:

- `spark/Dockerfile` - **modify** hoặc giữ làm base.

Build context:

- `.` hoặc `./spark` tùy quyết định implementation.
- Nếu cần copy `spark_jobs/`, `config/`, `ml/` vào image cho K8s, document rõ thay đổi so với Compose volume mount hiện tại.

Entrypoint/command:

- Spark image phải dùng được cho driver và executor pods.
- Job file được submit qua `local:///opt/spark-jobs/<job>.py` hoặc image path tương đương.

Dependencies cần có:

- Spark runtime.
- Iceberg dependencies.
- Python dependencies trong `spark/Dockerfile` hiện có: PyYAML, xarray, netCDF4, h5netcdf, h5py, pyhdf, rasterio, GDAL, pyproj, shapely, numpy, pandas, pyarrow, scikit-learn, lightgbm, optional xgboost.
- HYSPLIT binary nếu trajectory jobs cần chạy trong Spark pod.
- `spark_jobs/` và `config/` phải truy cập được trong driver/executor.
- ML dependencies chỉ cần trong Spark image nếu Spark jobs thật sự import/dùng chúng.

Mounted config:

- `config/hanoi_pipeline.yaml` qua ConfigMap hoặc copy versioned vào image và override bằng env.

Local smoke test:

```bash
docker build -t ais-spark-runtime:local -f spark/Dockerfile .
docker run --rm ais-spark-runtime:local /opt/spark/bin/spark-submit --version
```

K8s smoke test:

```bash
kubectl -n ais run spark-image-smoke --rm -it --image=ais-spark-runtime:local -- /opt/spark/bin/spark-submit --version
```

Acceptance criteria:

- Image chạy được driver/executor pods.
- HYSPLIT jobs không fail vì thiếu binary nếu chúng nằm trong phase migration.
- Image không phụ thuộc bind mount Compose.

### 6.2 `ais-ml-runtime`

Dockerfile path:

- `ml/Dockerfile` - **create**

Build context:

- Repo root để image truy cập `ml/`, `spark_jobs/hanoi_config.py`, `config/`, và schema shared nếu cần.

Entrypoint/command:

```text
python ml/train_hanoi_pm25.py
python ml/predict_hanoi_pm25.py
python ml/promote_hanoi_pm25_model.py
```

Dependencies:

- Python.
- pandas, numpy, pyarrow.
- lightgbm/xgboost đúng với `ml/train_hanoi_pm25.py`.
- Iceberg/table client hoặc PySpark nếu inference/training cần Spark để đọc/ghi Iceberg.
- Shared config loader.

Mounted config:

- `config/hanoi_pipeline.yaml`.
- ConfigMap/Secret cho endpoints/artifact storage.

Local smoke test:

```bash
docker build -t ais-ml-runtime:local -f ml/Dockerfile .
docker run --rm --env-file .env ais-ml-runtime:local python ml/train_hanoi_pm25.py --help
```

K8s smoke test:

```bash
kubectl -n ais run ml-help --rm -it --image=ais-ml-runtime:local -- python ml/predict_hanoi_pm25.py --help
```

Acceptance criteria:

- Training và inference command tồn tại.
- Image đọc được config.
- Artifact path không nằm trong ephemeral-only path nếu dùng production.

### 6.3 `ais-pm25-api`

Dockerfile path:

- `serving/pm25_api/Dockerfile` - **create**

Build context:

- Repo root hoặc `serving/pm25_api` nếu package shared được copy đúng.

Entrypoint/command:

```text
uvicorn main:app --host 0.0.0.0 --port ${API_PORT:-8080}
```

Dependencies:

- FastAPI/uvicorn hoặc framework được chọn.
- Iceberg/table query dependency.
- Config loader.
- Health/readiness support.

Mounted config:

- Table name, catalog/warehouse, location ID, readiness timeout.

Local smoke test:

```bash
docker build -t ais-pm25-api:local -f serving/pm25_api/Dockerfile .
docker run --rm -p 8081:8080 --env-file .env ais-pm25-api:local
curl -i http://localhost:8081/healthz
```

K8s smoke test:

```bash
kubectl -n ais port-forward svc/pm25-api 8081:80
curl -i http://localhost:8081/healthz
curl -i http://localhost:8081/readyz
```

Acceptance criteria:

- API image không chứa model artifact hoặc secret.
- API chỉ đọc prediction table.

### 6.4 `ais-checks-runtime` optional

Dockerfile path:

- `checks/Dockerfile` - **create optional**, hoặc dùng `ais-ml-runtime`.

Mục đích:

- Chạy `scripts/check_pm25_serving.py` và các checks phụ thuộc Iceberg/API.

Acceptance criteria:

- Check image có exit code rõ.
- Có thể chạy trong K8s Job.

## 7. Spark-on-Kubernetes runtime

Mode chính của TODO3: **Option A - `spark-submit --master k8s://...`**.

Lý do: repo hiện chưa có Spark Operator manifests/CRD. Chọn `spark-submit --master k8s://...` giúp migration ban đầu ít phụ thuộc hơn. Spark Operator có thể là enhancement sau, nhưng runtime Spark trên K8s vẫn bắt buộc.

Files cần tạo/sửa:

- `deploy/k8s/spark/spark-serviceaccount.yaml` - **create**
- `deploy/k8s/spark/spark-rbac.yaml` - **create**
- `deploy/k8s/spark/spark-submit-template.yaml` - **create**
- `deploy/k8s/spark/README.md` - **create**
- `scripts/submit_spark.sh` - **modify** để có mode K8s hoặc giữ Compose fallback và trỏ sang script mới.
- `scripts/submit_spark_k8s.sh` - **create optional** nếu tách khỏi script hiện tại sạch hơn.
- `airflow/dags/ais_dag_utils.py` - **modify** để Airflow submit Spark-on-K8s.

Spark configs bắt buộc:

```text
spark.master=k8s://...
spark.kubernetes.container.image=ais-spark-runtime:<tag>
spark.kubernetes.namespace=ais
spark.kubernetes.authenticate.driver.serviceAccountName=spark
spark.executor.instances=<env>
spark.executor.memory=<env>
spark.executor.cores=<env>
spark.driver.memory=<env>
spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.<catalog>=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.<catalog>.type=hadoop
spark.sql.catalog.<catalog>.warehouse=<ICEBERG_WAREHOUSE>
spark.hadoop.fs.defaultFS=<HDFS_NAMENODE>
```

Generic submit template:

```bash
/opt/spark/bin/spark-submit \
  --master "${SPARK_K8S_MASTER}" \
  --deploy-mode cluster \
  --name "${APP_NAME}" \
  --conf "spark.kubernetes.namespace=${SPARK_K8S_NAMESPACE:-ais}" \
  --conf "spark.kubernetes.container.image=${SPARK_IMAGE}" \
  --conf "spark.kubernetes.authenticate.driver.serviceAccountName=${SPARK_DRIVER_SERVICE_ACCOUNT:-spark}" \
  --conf "spark.executor.instances=${SPARK_EXECUTOR_INSTANCES:-2}" \
  --conf "spark.executor.memory=${SPARK_EXECUTOR_MEMORY:-2g}" \
  --conf "spark.executor.cores=${SPARK_EXECUTOR_CORES:-1}" \
  --conf "spark.driver.memory=${SPARK_DRIVER_MEMORY:-1g}" \
  --conf "spark.hadoop.fs.defaultFS=${HDFS_NAMENODE}" \
  --conf "spark.sql.catalog.${ICEBERG_CATALOG}=org.apache.iceberg.spark.SparkCatalog" \
  --conf "spark.sql.catalog.${ICEBERG_CATALOG}.type=hadoop" \
  --conf "spark.sql.catalog.${ICEBERG_CATALOG}.warehouse=${ICEBERG_WAREHOUSE}" \
  local:///opt/spark-jobs/<job>.py <args>
```

Minimal Spark smoke job:

- Có thể dùng `spark_jobs/ensure_iceberg_tables.py` ở dry/minimal mode nếu thêm được, hoặc tạo `spark_jobs/spark_k8s_smoke.py` - **create optional**.
- Job phải:
  - Start driver pod.
  - Start executor pod.
  - Đọc config từ env/ConfigMap.
  - Chạy simple Spark action.
  - Đọc hoặc ghi một Iceberg table test nhỏ nếu storage sẵn sàng.

Validation commands:

```bash
kubectl -n ais get pods
kubectl -n ais logs <spark-driver-pod>
kubectl -n ais describe pod <spark-driver-pod>
kubectl -n ais get events --sort-by=.lastTimestamp
```

Table validation:

```bash
kubectl -n ais logs <spark-driver-pod> | grep -E "Ensured|input_count|output_count|status=success"
```

Acceptance criteria:

- Spark job chạy trên K8s không cần `spark-master`/`spark-worker` Compose.
- Driver/executor pods thấy được bằng `kubectl get pods`.
- Job truy cập được external storage layer.
- Job logs truy cập được bằng `kubectl logs`.
- Job exit cleanly.
- Nếu Spark Operator chưa dùng, TODO3 document rõ Operator là optional future enhancement.

## 8. Migration of existing Spark jobs to K8s

Tất cả job dưới đây phải có đường chạy K8s. Migration có thể theo từng đợt, nhưng không checkpoint nào được coi là complete nếu chỉ có Compose Spark path.

Template chung:

```text
current: bash scripts/submit_spark.sh <job-type>
target: scripts/submit_spark_k8s.sh <job-type>
runtime: Spark driver/executor pods trong namespace ais
```

| Job category | Existing file(s) | Current command/path | Target K8s submit | Input | Output | Idempotency rule | Phase |
|---|---|---|---|---|---|---|---|
| Ensure Iceberg | `spark_jobs/ensure_iceberg_tables.py` | `bash scripts/submit_spark.sh ensure-iceberg` | `scripts/submit_spark_k8s.sh ensure-iceberg` | Catalog/warehouse config | Namespaces/tables | `CREATE IF NOT EXISTS`, `ALTER ADD COLUMN` safe | migrate first |
| Bronze streaming/batch metadata | `weather_streaming.py`, `openaq_hourly_streaming.py`, `sentinel5p_summary_streaming.py`, `maiac_summary_streaming.py`, `era5_files_streaming.py` | `scripts/submit_spark.sh weather|openaq|sentinel5p|maiac|era5-files` | K8s Spark submit with checkpoint config | Kafka topics | Iceberg bronze | Kafka checkpoint + merge/dedupe where implemented | migrate after smoke |
| Tier 1 silver | `hanoi_openaq_silver.py`, `hanoi_weather_surface_proxy_silver.py`, `era5_surface_hanoi_silver.py`, `sentinel5p_hanoi_silver.py`, `maiac_hanoi_silver.py` | `scripts/submit_spark.sh hanoi-openaq-silver`, etc. | K8s Spark submit | Bronze/raw | Silver tables | MERGE/overwrite partitions as existing | migrate in TODO3 |
| Tier 2 trajectory/HYSPLIT | `era5_pressure_levels_to_arl.py`, `hysplit_trajectory_run.py`, `hysplit_trajectory_parse_silver.py`, `hysplit_trajectory_cluster_silver.py`, `openaq_spatial_gradient_silver.py`, `sentinel5p_grid_silver.py`, `trajectory_path_sampling_silver.py`, `trajectory_hourly_features_silver.py` | `scripts/submit_spark.sh era5-pressure-arl|hysplit-run|...` | K8s Spark submit or Kubernetes Job for non-Spark binary step if needed | ERA5/ARL/OpenAQ/S5P | Trajectory/feature silver | MERGE by run/time keys | migrate in TODO3 if dependencies available; document deferred binary gaps |
| Hanoi PM2.5 master gold | `hanoi_pm25_master_features_gold.py` | `scripts/submit_spark.sh hanoi-master-features-gold` | K8s Spark submit | Silver + trajectory features | `ais.features.hanoi_pm25_master_hourly_gold` | MERGE by `hour`/partition | migrate in TODO3 |
| Training dataset gold | `hanoi_pm25_training_dataset_gold.py` | `scripts/submit_spark.sh hanoi-training-dataset-gold` | K8s Spark submit | Master gold | `ais.features.hanoi_pm25_training_dataset_gold` | MERGE by `hour + dataset_version + feature_set_name` | migrate in TODO3 |
| PM2.5 serving features | `spark_jobs/hanoi_pm25_serving_features_gold.py` | create | K8s Spark submit | Master gold | `ais.features.hanoi_pm25_serving_features_gold` | MERGE by `base_hour + location_id + feature_version` | create in TODO3 |
| Iceberg maintenance | `spark_jobs/iceberg_maintenance.py` | `scripts/submit_spark.sh maintenance-iceberg` | K8s Spark submit | Iceberg tables | optimized tables | Procedure idempotent | migrate or document gap |
| Cassandra serving/reconcile | `iceberg_to_cassandra.py`, `reconcile_iceberg_cassandra.py` | `scripts/submit_spark.sh cassandra-weather|cassandra-openaq|reconcile-serving` | K8s Spark submit after removing hardcoded `cassandra` | Iceberg/Cassandra | Cassandra/check logs | Per-table keys | optional if still needed |

Per-job migration checklist:

- [ ] Current command/path documented.
- [ ] K8s submit command documented.
- [ ] Input table/path documented.
- [ ] Output table/path documented.
- [ ] Required config/env listed.
- [ ] Expected logs listed.
- [ ] Smoke test listed.
- [ ] Idempotency key or rerun behavior documented.

Acceptance criteria:

- Existing Spark jobs have K8s execution plan.
- TODO3 identifies migrated vs deferred jobs.
- No TODO3 checkpoint depends on Spark standalone Compose as target runtime.
- Deferred job must have explicit reason, such as missing HYSPLIT binary in image or missing storage connectivity; không được defer chỉ vì muốn giữ Spark standalone làm target.

## 9. Iceberg table/schema updates

Files cần sửa:

- `spark_jobs/hanoi_config.py` - **modify**
- `spark_jobs/ensure_iceberg_tables.py` - **modify**
- `config/hanoi_pipeline.yaml` - **modify optional** để thêm `serving`, `prediction`, `model_registry` config.

Table names cần thêm vào `spark_jobs/hanoi_config.py`:

```python
TABLES.update({
    "serving_features_gold": f"{ICEBERG_CATALOG}.features.hanoi_pm25_serving_features_gold",
    "prediction_gold": f"{ICEBERG_CATALOG}.predictions.hanoi_pm25_forecast_gold",
    "model_registry_gold": f"{ICEBERG_CATALOG}.models.hanoi_pm25_model_registry_gold",
})
```

Namespace cần thêm:

- `ais.predictions`
- `ais.models` đã có nhưng phải verify registry table.

Giữ lại:

- `ais.models.hanoi_pm25_model_runs_gold` là run history.

Thêm:

- `ais.models.hanoi_pm25_model_registry_gold` là production pointer, không thay thế run history.

### 9.1 `ais.features.hanoi_pm25_serving_features_gold`

Purpose:

- Materialized serving features cho inference, derived từ master gold.

Source table:

- `ais.features.hanoi_pm25_master_hourly_gold`

Target table:

- `ais.features.hanoi_pm25_serving_features_gold`

Owner files:

- `spark_jobs/hanoi_config.py` - table name.
- `spark_jobs/ensure_iceberg_tables.py` - schema.
- `spark_jobs/hanoi_pm25_serving_features_gold.py` - writer job.

Schema yêu cầu:

```sql
base_hour TIMESTAMP,
location_id STRING,
location_name STRING,
feature_version STRING,
feature_set_name STRING,
dataset_version STRING,
schema_hash STRING,
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
pm25_grad_n DOUBLE,
pm25_grad_s DOUBLE,
pm25_grad_e DOUBLE,
pm25_grad_w DOUBLE,
pm25_spatial_std DOUBLE,
pm25_grad_mag DOUBLE,
dominant_cluster INT,
n_traj INT,
traj_source_lat DOUBLE,
traj_source_lon DOUBLE,
traj_path_no2_mean DOUBLE,
traj_path_aer_mean DOUBLE,
traj_path_no2_aer_ratio DOUBLE,
hour_of_day INT,
day_of_week INT,
month INT,
season STRING,
is_weekend BOOLEAN,
hour_sin DOUBLE,
hour_cos DOUBLE,
dow_sin DOUBLE,
dow_cos DOUBLE,
month_sin DOUBLE,
month_cos DOUBLE,
is_rush_hour BOOLEAN,
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
year INT,
month_partition INT,
created_at TIMESTAMP,
spark_processed_at TIMESTAMP
```

Partitioning:

```sql
PARTITIONED BY (year, month_partition)
```

Merge/idempotency key:

```text
base_hour + location_id + feature_version
```

Validation rules:

- Drop target/leakage columns:
  - `pm25_next_6h`
  - `pm25_next_12h`
  - `pm25_next_24h`
- Serving feature schema phải match `ml/train_hanoi_pm25.py::FEATURE_COLUMNS` sau khi tính preprocessing rules, ví dụ `season` có thể được one-hot khi train.
- `base_hour` phải map từ `master_gold.hour`.
- `location_id` default `hanoi`.
- `schema_hash` phải stable theo ordered feature list và preprocessing metadata.

Expected logs:

```text
input_count
output_count
min_base_hour
max_base_hour
feature_version
feature_set_name
schema_hash
null_ratio_by_feature
status
```

Acceptance criteria:

- Table được tạo bằng `ensure_iceberg_tables.py` chạy trên K8s.
- Job ghi được data bằng Spark-on-K8s.
- Không có target columns trong output.
- Rerun cùng range không duplicate.

### 9.2 `ais.predictions.hanoi_pm25_forecast_gold`

Purpose:

- Canonical PM2.5 forecast output cho API.

Source table:

- `ais.features.hanoi_pm25_serving_features_gold`
- `ais.models.hanoi_pm25_model_registry_gold`

Target table:

- `ais.predictions.hanoi_pm25_forecast_gold`

Owner files:

- `spark_jobs/hanoi_config.py`
- `spark_jobs/ensure_iceberg_tables.py`
- `ml/predict_hanoi_pm25.py`
- `serving/pm25_api/`

Schema yêu cầu:

```sql
prediction_id STRING,
base_hour TIMESTAMP,
location_id STRING,
location_name STRING,
pm25_now DOUBLE,
pm25_6h DOUBLE,
risk_6h STRING,
pm25_12h DOUBLE,
risk_12h STRING,
pm25_24h DOUBLE,
risk_24h STRING,
dominant_cluster INT,
source_lat DOUBLE,
source_lon DOUBLE,
path_no2_mean DOUBLE,
path_aer_mean DOUBLE,
pm25_grad_mag DOUBLE,
model_version STRING,
model_version_6h STRING,
model_version_12h STRING,
model_version_24h STRING,
model_status STRING,
feature_version STRING,
feature_schema_hash STRING,
inference_run_id STRING,
created_at TIMESTAMP,
year INT,
month_partition INT
```

Partitioning:

```sql
PARTITIONED BY (year, month_partition)
```

Merge/idempotency key:

```text
base_hour + location_id + model_version + feature_version
```

`prediction_id`:

```text
sha256(base_hour, location_id, model_version, feature_version)
```

Validation rules:

- `pm25_6h`, `pm25_12h`, `pm25_24h` không null khi status success.
- `risk_*` thuộc `low|medium|high|very_high`.
- `model_status = 'production'` cho rows API đọc.
- API chỉ đọc table này cho forecast response.

Expected logs:

```text
input_count
output_count
base_hour
location_id
model_version_6h
model_version_12h
model_version_24h
feature_version
feature_schema_hash
dry_run
status
```

Acceptance criteria:

- Inference dry-run không ghi table.
- Inference `--dry-run 0` ghi hoặc merge đúng row.
- Rerun cùng key không tạo duplicate.
- API forecast endpoint không query source/raw/master/serving feature tables.

### 9.3 `ais.models.hanoi_pm25_model_registry_gold`

Purpose:

- Production model pointer theo `location_id`, `horizon_hour`, `feature_version`, `model_version`.

Source table:

- `ais.models.hanoi_pm25_model_runs_gold`

Target table:

- `ais.models.hanoi_pm25_model_registry_gold`

Owner files:

- `spark_jobs/hanoi_config.py`
- `spark_jobs/ensure_iceberg_tables.py`
- `ml/promote_hanoi_pm25_model.py` optional
- `ml/predict_hanoi_pm25.py`

Schema yêu cầu:

```sql
model_version STRING,
model_run_id STRING,
horizon_hour INT,
location_id STRING,
model_type STRING,
model_path STRING,
artifact_uri STRING,
feature_set_name STRING,
feature_version STRING,
training_dataset_version STRING,
feature_schema_hash STRING,
status STRING,
mae DOUBLE,
rmse DOUBLE,
mape DOUBLE,
promoted_at TIMESTAMP,
promoted_by STRING,
created_at TIMESTAMP,
effective_from TIMESTAMP,
effective_to TIMESTAMP
```

Partitioning:

```sql
PARTITIONED BY (status, horizon_hour)
```

Merge/idempotency key:

```text
model_version + horizon_hour + location_id
```

Validation rules:

- Có đúng một production model cho mỗi `location_id + horizon_hour`, trừ khi dùng effective-time versioning rõ ràng.
- Inference fail fast nếu thiếu production model cho 6h/12h/24h.
- Inference không được tự lấy latest model run nếu chưa được promote.
- `feature_schema_hash` của registry phải match serving feature row.

Expected logs:

```text
promotion_action
model_run_id
model_version
horizon_hour
location_id
old_status
new_status
promoted_by
status
```

Acceptance criteria:

- Query registry trả đủ 3 production models cho horizon 6/12/24.
- Rollback/demotion có command rõ.
- Registry không xóa run history.

## 10. PM2.5 serving feature Spark job on K8s

File cần tạo:

- `spark_jobs/hanoi_pm25_serving_features_gold.py` - **create**

Mục đích:

- Đọc `ais.features.hanoi_pm25_master_hourly_gold`.
- Chọn feature biết tại `base_hour`.
- Bỏ target/leakage columns.
- Ghi `ais.features.hanoi_pm25_serving_features_gold`.
- Chạy bằng Spark-on-K8s, không dùng Spark Compose làm target.

CLI args:

```text
--start-date YYYY-MM-DD
--end-date YYYY-MM-DD
--full-refresh 0|1
--feature-version hanoi_pm25_core_v1
--feature-set-name hanoi_pm25_core_v1
--dataset-version <optional>
--location-id hanoi
--location-name Hanoi
--dry-run 0|1
```

Env vars:

```text
ICEBERG_CATALOG
ICEBERG_WAREHOUSE
HDFS_NAMENODE
SOURCE_TABLE
TARGET_TABLE
FEATURE_VERSION
FEATURE_SET_NAME
DATASET_VERSION
LOCATION_ID
LOCATION_NAME
```

Input/output:

- Input: `ais.features.hanoi_pm25_master_hourly_gold`
- Output: `ais.features.hanoi_pm25_serving_features_gold`

Logic bắt buộc:

- `base_hour = hour`.
- Select only `ml/train_hanoi_pm25.py::FEATURE_COLUMNS` cộng metadata serving.
- Drop:
  - `pm25_next_6h`
  - `pm25_next_12h`
  - `pm25_next_24h`
- Add:
  - `feature_version`
  - `feature_set_name`
  - `schema_hash`
  - `location_id`
  - `location_name`
  - `created_at`
- Validate schema against `FEATURE_COLUMNS`.
- Idempotent by `base_hour + location_id + feature_version`.

Dry-run/check mode:

- `--dry-run 1` chỉ validate schema/count/null ratio, không ghi table.

K8s submit command:

```bash
scripts/submit_spark_k8s.sh hanoi-serving-features-gold \
  --start-date 2026-05-01 \
  --end-date 2026-05-02 \
  --feature-version hanoi_pm25_core_v1 \
  --dry-run 1
```

Airflow integration point:

- Task trong `airflow/dags/ais_pm25_k8s_compute_dag.py` sau upstream Tier 2/master gold và trước inference.

Expected logs:

```text
job=hanoi_pm25_serving_features_gold
input_count
output_count
min_base_hour
max_base_hour
feature_version
schema_hash
dry_run
status
```

Acceptance criteria:

- Job chạy Spark-on-K8s và tạo driver/executor pods.
- Output không có target leakage.
- Schema hash stable và được dùng bởi inference.
- Rerun không duplicate.

## 11. Model registry and promotion

Files cần tạo/sửa:

- `spark_jobs/ensure_iceberg_tables.py` - **modify**
- `spark_jobs/hanoi_config.py` - **modify**
- `ml/promote_hanoi_pm25_model.py` - **create optional nhưng khuyến nghị**
- `ml/train_hanoi_pm25.py` - **modify** để artifact URI/run metadata đủ cho promotion nếu thiếu.

Registry:

- Table: `ais.models.hanoi_pm25_model_registry_gold`
- Source run history: `ais.models.hanoi_pm25_model_runs_gold`

Production pointer dimensions:

```text
location_id
horizon_hour
feature_version
model_version
```

Rules:

- [ ] Có đúng một production model cho mỗi `location_id + horizon_hour`, trừ khi có `effective_from/effective_to`.
- [ ] Có production model cho cả 6h, 12h, 24h trước khi inference production chạy.
- [ ] Inference fail nếu thiếu production model cho bất kỳ horizon nào.
- [ ] Promotion là explicit action, không lấy latest run tự động.

Artifact URI convention:

```text
MODEL_ARTIFACT_BASE_URI/hanoi_pm25/{feature_version}/{model_version}/horizon={horizon_hour}/model.<ext>
MODEL_ARTIFACT_BASE_URI/hanoi_pm25/{feature_version}/{model_version}/horizon={horizon_hour}/feature_importance.csv
```

Promotion command:

```bash
python ml/promote_hanoi_pm25_model.py \
  --model-run-id <run_id> \
  --location-id hanoi \
  --horizon-hour 6 \
  --feature-version hanoi_pm25_core_v1 \
  --status production \
  --promoted-by <user>
```

Rollback/demotion behavior:

- Demote current production to `archived` hoặc set `effective_to`.
- Promote previous model version explicitly.
- Log old/new production pointer.

Acceptance criteria:

- Registry table tồn tại.
- Promotion idempotent.
- Query registry trả đúng production model cho 6/12/24.
- Inference không chạy thành công nếu registry thiếu horizon.

## 12. ML training Job on K8s

Files cần tạo/sửa:

- `ml/train_hanoi_pm25.py` - **modify**
- `ml/Dockerfile` - **create**
- `deploy/k8s/ml/pm25-train-job.yaml` - **create**
- `deploy/k8s/ml/README.md` - **create**

Runtime:

- Kubernetes Job dùng image `ais-ml-runtime`.
- Có thể chạy thủ công hoặc Airflow-triggered.
- Không ghi production registry trực tiếp, trừ khi có flag promote rõ và được document. Default chỉ ghi run metadata.

Command:

```bash
python ml/train_hanoi_pm25.py \
  --dataset-version hanoi_pm25_v1 \
  --feature-set-name hanoi_pm25_core_v1 \
  --model-type lightgbm \
  --output-dir "${MODEL_ARTIFACT_BASE_URI}/hanoi_pm25"
```

Args/env:

```text
DATASET_VERSION
FEATURE_SET_NAME
MODEL_TYPE
MODEL_OUTPUT_DIR
MODEL_ARTIFACT_BASE_URI
ICEBERG_CATALOG
ICEBERG_WAREHOUSE
HDFS_NAMENODE
TRAINING_TABLE
MODEL_RUNS_TABLE
```

Required resources:

- CPU/memory lớn hơn inference; giá trị dev có thể bắt đầu `1 CPU / 2Gi`.
- Nếu dùng XGBoost/LightGBM GPU sau này phải có node selector/toleration riêng; không yêu cầu trong TODO3.

Output artifacts:

- Model file cho horizon 6/12/24.
- Feature importance.
- Row metadata trong `ais.models.hanoi_pm25_model_runs_gold`.

Logs:

```text
train_metrics
validation_metrics
test_metrics
horizon
mae
rmse
mape
model_path
model_run_id
status
```

Manual K8s run:

```bash
kubectl -n ais apply -f deploy/k8s/ml/pm25-train-job.yaml
kubectl -n ais logs job/pm25-train
```

Airflow-triggered:

- `airflow/dags/ais_pm25_k8s_compute_dag.py` có task optional `train_hanoi_pm25`.

Acceptance criteria:

- Training Job chạy trên K8s.
- Đọc training dataset table.
- Ghi artifact vào external artifact storage.
- Ghi run metadata vào model runs table.
- Không tự promote production trừ khi task promotion explicit.

## 13. ML inference Job/CronJob on K8s

Files cần tạo:

- `ml/predict_hanoi_pm25.py` - **create**
- `deploy/k8s/ml/pm25-predict-cronjob.yaml` - **create**
- `deploy/k8s/ml/pm25-predict-job.yaml` - **create optional**

Mục đích:

- Đọc latest hoặc specified `base_hour` từ serving features.
- Đọc production models từ registry.
- Validate schema hash.
- Predict 6h/12h/24h.
- Tính risk levels.
- Ghi prediction table.

CLI args:

```text
--base-hour <ISO8601 optional>
--location hanoi
--model-status production
--feature-version hanoi_pm25_core_v1
--dry-run 0|1
--max-feature-age-minutes 180
```

Env vars:

```text
ICEBERG_CATALOG
ICEBERG_WAREHOUSE
HDFS_NAMENODE
SERVING_FEATURE_TABLE
PREDICTION_TABLE
MODEL_REGISTRY_TABLE
MODEL_ARTIFACT_BASE_URI
LOCATION_ID
FEATURE_VERSION
```

Rules:

- Nếu `--base-hour` rỗng, dùng latest `base_hour` trong serving features.
- Load production model từ `ais.models.hanoi_pm25_model_registry_gold`, không từ latest run.
- Fail fast nếu thiếu production model cho 6/12/24.
- Fail fast nếu `feature_schema_hash` mismatch.
- Idempotent by `base_hour + location_id + model_version + feature_version`.
- `--dry-run 1` không ghi table.

Risk levels:

```text
low: PM2.5 < 35
medium: 35 <= PM2.5 < 75
high: 75 <= PM2.5 < 150
very_high: PM2.5 >= 150
```

Logs bắt buộc:

```text
input_count
output_count
model_version_6h
model_version_12h
model_version_24h
feature_version
base_hour
location_id
dry_run
status
```

CronJob config:

- Schedule dev default: hourly, ví dụ `15 * * * *`, nhưng Airflow vẫn là dependency/backfill control plane.
- `concurrencyPolicy: Forbid`
- `backoffLimit: 1` hoặc `2`
- `successfulJobsHistoryLimit: 3`
- `failedJobsHistoryLimit: 5`
- `restartPolicy: Never`

Manual run command:

```bash
kubectl -n ais create job pm25-predict-manual-$(date +%Y%m%d%H%M%S) --from=cronjob/pm25-predict
kubectl -n ais logs job/<job-name>
```

Airflow integration:

- Airflow có thể trigger Kubernetes Job từ CronJob hoặc chạy KubernetesPodOperator.
- CronJob không thay thế Airflow dependency/rerun/backfill.

Acceptance criteria:

- Inference dry-run succeed khi registry + features hợp lệ.
- `--dry-run 0` ghi prediction.
- Rerun không duplicate.
- Missing model/schema mismatch làm job fail rõ.
- Logs đủ field bắt buộc.

## 14. PM2.5 API Deployment on K8s

Files cần tạo:

- `serving/pm25_api/` - **create**
- `serving/pm25_api/main.py` - **create**
- `serving/pm25_api/requirements.txt` - **create**
- `serving/pm25_api/Dockerfile` - **create**
- `serving/pm25_api/README.md` - **create**
- `deploy/k8s/api/pm25-api-deployment.yaml` - **create**
- `deploy/k8s/api/pm25-api-service.yaml` - **create**

Required endpoints:

- `GET /healthz`
- `GET /readyz`
- `GET /api/v1/hanoi/pm25/forecast/latest`

API rules:

- API reads only `ais.predictions.hanoi_pm25_forecast_gold`.
- API returns latest row for `location_id='hanoi'` and `model_status='production'`.
- API must not run Spark/model prediction in request.
- `/healthz` only checks process alive.
- `/readyz` checks required config and prediction table/catalog connectivity.
- Logs request path, status_code, latency_ms, error_code.

404 response when no prediction:

```json
{
  "error": "prediction_not_found",
  "location": "hanoi"
}
```

Forecast response contract:

```json
{
  "base_hour": "2026-05-19T09:00:00Z",
  "location": "hanoi",
  "pm25_now": 0.0,
  "forecast": {
    "6h": {"pm25": 0.0, "risk": "low"},
    "12h": {"pm25": 0.0, "risk": "medium"},
    "24h": {"pm25": 0.0, "risk": "high"}
  },
  "source_attribution": {
    "dominant_cluster": 0,
    "source_lat": 0.0,
    "source_lon": 0.0,
    "path_no2_mean": 0.0,
    "path_aer_mean": 0.0,
    "pm25_grad_mag": 0.0
  },
  "model": {
    "model_version_6h": "example",
    "model_version_12h": "example",
    "model_version_24h": "example",
    "feature_version": "hanoi_pm25_core_v1"
  },
  "created_at": "2026-05-19T09:05:00Z"
}
```

Config:

```text
API_PORT
PREDICTION_TABLE
ICEBERG_CATALOG
ICEBERG_WAREHOUSE
LOCATION_ID
READY_TIMEOUT_SECONDS
```

Docker build/run:

```bash
docker build -t ais-pm25-api:local -f serving/pm25_api/Dockerfile .
docker run --rm -p 8081:8080 --env-file .env ais-pm25-api:local
```

K8s deployment/service:

```bash
kubectl -n ais apply -f deploy/k8s/api/pm25-api-deployment.yaml
kubectl -n ais apply -f deploy/k8s/api/pm25-api-service.yaml
```

Probes:

- Liveness: `GET /healthz`
- Readiness: `GET /readyz`

Resource requests/limits:

- Requests: `100m` CPU, `256Mi` memory.
- Limits: `500m` CPU, `512Mi` memory.
- Tune after load test.

Local smoke test:

```bash
curl -i http://localhost:8081/healthz
curl -i http://localhost:8081/readyz
curl -i http://localhost:8081/api/v1/hanoi/pm25/forecast/latest
```

K8s smoke test:

```bash
kubectl -n ais port-forward svc/pm25-api 8081:80
curl -i http://localhost:8081/healthz
curl -i http://localhost:8081/readyz
curl -i http://localhost:8081/api/v1/hanoi/pm25/forecast/latest
```

Acceptance criteria:

- API chạy trên K8s Deployment.
- Service route được.
- Health/readiness đúng semantics.
- Forecast endpoint trả JSON khi prediction tồn tại.
- Forecast endpoint trả 404 JSON rõ khi missing prediction.
- Không có request handler chạy compute nặng.

## 15. Airflow orchestration for K8s compute

Airflow vẫn là orchestration/control plane. Kubernetes chỉ là runtime thực thi.

Files cần tạo/sửa:

- `airflow/dags/ais_pm25_k8s_compute_dag.py` - **create**
- `airflow/dags/ais_dag_utils.py` - **modify**
- `airflow/dags/ais_hanoi_silver_gold_dag.py` - **modify optional** để chuyển path K8s hoặc đánh dấu fallback.
- `airflow/dags/ais_trajectory_tier2_dag.py` - **modify optional** để chuyển path K8s hoặc đánh dấu fallback.

DAG nên orchestrate:

- `ensure_iceberg_tables` trên Spark-on-K8s.
- Upstream Tier 2 dependency.
- Build serving features trên Spark-on-K8s.
- Optional training Job trên K8s.
- Promote model bằng task explicit/manual gate.
- Inference Job trên K8s.
- Prediction freshness check.
- API smoke/readiness check nếu phù hợp.

Integration options:

- `KubernetesPodOperator` nếu Airflow image có provider `apache-airflow-providers-cncf-kubernetes`.
- BashOperator gọi `kubectl apply/create job` nếu Airflow container có kubeconfig và kubectl.
- BashOperator chạy `scripts/submit_spark_k8s.sh` cho Spark jobs.
- SparkApplication nếu sau này chọn Spark Operator.

Required documentation:

- Airflow service account/kubeconfig path.
- Namespace `ais`.
- RBAC cần để create/watch pods/jobs.
- Cách truyền DAG conf: `start_date`, `end_date`, `base_hour`, `feature_version`, `dry_run`.

Example DAG flow:

```text
ensure_iceberg_tables_k8s
-> wait_for_upstream_tier2_or_run_tier2_k8s
-> build_pm25_serving_features_k8s
-> check_serving_features
-> run_pm25_inference_k8s
-> check_prediction_freshness
-> check_api_ready
```

Acceptance criteria:

- Airflow trigger được K8s compute workloads, hoặc TODO3 ghi chính xác provider/config còn thiếu.
- Airflow giữ vai trò dependency/rerun/backfill.
- CronJob không thay thế Airflow dependency management.
- DAG có manual backfill theo date range/base_hour.

## 16. Data quality and monitoring checks

Files cần tạo/sửa:

- `scripts/check_pm25_serving.py` - **create**
- `spark_jobs/hanoi_pm25_serving_quality_checks.py` - **create optional**
- `deploy/k8s/checks/pm25-serving-check-job.yaml` - **create**
- `monitoring/app.py` - **modify optional**

Checks bắt buộc:

| Check | Command | Expected output | Failure condition | Log fields | Acceptance criteria |
|---|---|---|---|---|---|
| Serving feature freshness | `python scripts/check_pm25_serving.py --check serving-freshness` | latest base hour + age | age vượt threshold | `latest_base_hour`, `age_minutes`, `threshold_minutes`, `status` | Fail rõ khi feature stale |
| Prediction freshness | `python scripts/check_pm25_serving.py --check prediction-freshness` | latest prediction + age | prediction stale/missing | `latest_base_hour`, `created_at`, `age_minutes`, `status` | Fail rõ khi stale |
| Critical feature null ratio | `python scripts/check_pm25_serving.py --check feature-null-ratio` | null ratio list | critical feature null vượt threshold | `feature`, `null_ratio`, `threshold`, `status` | Có threshold config |
| Missing production model | `python scripts/check_pm25_serving.py --check model-registry` | 6/12/24 model list | missing horizon | `horizon_hour`, `location_id`, `feature_version`, `status` | Fail nếu thiếu 1 horizon |
| Schema hash mismatch | inference hoặc check script | expected/current hash | mismatch | `expected_hash`, `actual_hash`, `feature_version`, `status` | Fail fast |
| API readiness | `curl /readyz` hoặc check script | 200 | non-200 | `url`, `status_code`, `latency_ms`, `error_code` | Readiness phân biệt dependency |
| Failed inference CronJob | `kubectl -n ais get jobs` hoặc API K8s client | last job status | failed job hoặc missing schedule | `job_name`, `schedule_time`, `status` | Check chạy được bằng K8s Job/Airflow |
| Failed Spark job | `kubectl logs/describe` hoặc Airflow task status | driver exit status | non-zero/failed pod | `app_name`, `driver_pod`, `exit_code`, `status` | Failure visible |
| Duplicate prediction rows | table query | duplicate count 0 | duplicate key count > 0 | `duplicate_count`, `key`, `status` | Fail nếu duplicate |

Monitoring app optional updates:

- `monitoring/app.py` có thể thêm cards cho:
  - latest serving feature base_hour.
  - latest prediction base_hour.
  - production model status 6/12/24.
  - API readiness.
  - K8s CronJob last success.

Acceptance criteria:

- Có command/manual check cho feature freshness và prediction freshness.
- Inference fail fast khi thiếu model hoặc schema mismatch.
- API readiness phân biệt process alive và dependency ready.
- Check job có exit code dùng được trong Airflow/K8s.

## 17. Smoke tests and acceptance gates

Các smoke test bắt buộc:

1. `kubectl apply -k deploy/k8s`
   - Pass khi namespace/config/RBAC base apply được.

2. Spark smoke job starts driver/executor pods.
   - Command: `scripts/submit_spark_k8s.sh spark-smoke`
   - Verify: `kubectl -n ais get pods`.

3. Spark smoke job can access Iceberg/HDFS.
   - Verify logs có read/write hoặc table list success.

4. `ensure_iceberg_tables` runs on K8s.
   - Command: `scripts/submit_spark_k8s.sh ensure-iceberg`
   - Verify namespace/table tạo được.

5. Serving feature job runs on K8s.
   - Command: `scripts/submit_spark_k8s.sh hanoi-serving-features-gold --dry-run 1`, rồi `--dry-run 0`.
   - Verify output table có rows.

6. Model registry has production models for 6/12/24.
   - Verify query registry theo `location_id=hanoi`.

7. Inference dry-run succeeds.
   - Command: `kubectl -n ais create job ... --dry-run=1` hoặc manifest manual job.

8. Inference `dry-run=0` writes prediction.
   - Verify prediction table có row.

9. Rerun inference does not duplicate rows.
   - Verify duplicate count by idempotency key = 0.

10. API `/healthz` returns 200.
    - Command: `curl -i http://localhost:8081/healthz`.

11. API `/readyz` returns 200 when dependency is ready.
    - Command: `curl -i http://localhost:8081/readyz`.

12. API forecast endpoint returns JSON when prediction exists.
    - Command: `curl -i http://localhost:8081/api/v1/hanoi/pm25/forecast/latest`.

13. API forecast endpoint returns clear 404 when prediction is missing.
    - Verify body:

```json
{"error":"prediction_not_found","location":"hanoi"}
```

14. Airflow can trigger K8s workloads or document exact gap.
    - Verify DAG/task success or list missing provider/kubeconfig/RBAC.

15. Existing TODO1/TODO2 pipeline behavior is not broken.
    - Dev fallback command vẫn chạy nếu cần.
    - K8s path không đổi table contract cũ ngoài additions.

Acceptance gates:

- Không đánh dấu TODO3 done nếu Spark vẫn chỉ chạy Compose.
- Không đánh dấu API done nếu inference/prediction table chưa có smoke.
- Không đánh dấu inference done nếu registry production model chưa có 6/12/24.
- Không đánh dấu Airflow done nếu chỉ có CronJob mà không có dependency/backfill story.


# TODO3 chỉ hoàn thành khi:

- [ ] Storage layer vẫn là storage-only trong pha này.
- [ ] Spark jobs chạy trên Kubernetes làm target runtime.
- [ ] Spark driver/executor pods hiện diện và log truy cập được.
- [ ] K8s pods kết nối được external Kafka/HDFS/Iceberg endpoints.
- [ ] Không có source code mới hardcode Docker Compose hostnames cho K8s path.
- [ ] `ais.features.hanoi_pm25_serving_features_gold` tồn tại và không có target leakage.
- [ ] Serving feature schema khớp `ml/train_hanoi_pm25.py::FEATURE_COLUMNS` sau preprocessing.
- [ ] `ais.models.hanoi_pm25_model_registry_gold` có production pointer cho horizon 6/12/24.
- [ ] Inference Job/CronJob chạy trên K8s và ghi prediction idempotent.
- [ ] `ais.predictions.hanoi_pm25_forecast_gold` là table duy nhất API đọc cho forecast response.
- [ ] PM2.5 API chạy trên K8s và không chạy Spark/HYSPLIT/model inference trong request.
- [ ] Airflow orchestrates K8s compute workloads hoặc có gap chính xác về provider/kubeconfig/RBAC.
- [ ] Freshness/schema/readiness checks tồn tại và có exit code rõ.
- [ ] Smoke tests trong section 17 được document.
- [ ] TODO1/TODO2 behavior vẫn compatible, với Docker Compose Spark chỉ là dev fallback.
