$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$rootDir = Resolve-Path (Join-Path $scriptDir "..")
Set-Location $rootDir

$lookbackDays = if ($env:LOOKBACK_DAYS) { [int]$env:LOOKBACK_DAYS } else { 7 }
$realtimeLookbackMinutes = if ($env:REALTIME_LOOKBACK_MINUTES) { [int]$env:REALTIME_LOOKBACK_MINUTES } else { 10 }
$realtimePollSeconds = if ($env:REALTIME_POLL_SECONDS) { [int]$env:REALTIME_POLL_SECONDS } else { 600 }
$enableAirflow = if ($env:ENABLE_AIRFLOW) { $env:ENABLE_AIRFLOW.ToLower() -eq "true" } else { $false }
$enableMonitoring = if ($env:ENABLE_MONITORING) { $env:ENABLE_MONITORING.ToLower() -eq "true" } else { $false }

function Show-ContainerDiagnostics {
    param(
        [Parameter(Mandatory = $true)][string]$ContainerName
    )

    Write-Host "[DEBUG] $ContainerName health log:"
    docker inspect --format '{{range .State.Health.Log}}{{println .ExitCode ":" .Output}}{{end}}' $ContainerName 2>$null | Out-Host

    Write-Host "[DEBUG] $ContainerName container logs (tail):"
    docker logs --tail 120 $ContainerName 2>&1 | Out-Host
}

function Wait-ForHealthy {
    param(
        [Parameter(Mandatory = $true)][string]$ContainerName,
        [int]$TimeoutSec = 300
    )

    $elapsed = 0
    while ($true) {
        $status = docker inspect -f "{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}" $ContainerName 2>$null
        if (-not $status) {
            $status = "unknown"
        }

        if ($status -eq "healthy" -or $status -eq "running") {
            Write-Host "[OK] $ContainerName status=$status"
            return
        }

        if ($elapsed -ge $TimeoutSec) {
            Show-ContainerDiagnostics -ContainerName $ContainerName
            throw "Timeout waiting for $ContainerName (last status=$status)"
        }

        Write-Host "[WAIT] $ContainerName status=$status (${elapsed}s/${TimeoutSec}s)"
        Start-Sleep -Seconds 5
        $elapsed += 5
    }
}

function Initialize-Topics {
    $topics = @("openaq-hourly", "weather_history", "sentinel5p-summary", "maiac-summary", "era5-files")

    Write-Host "=== Create AIS Kafka topics ==="
    foreach ($topic in $topics) {
        Write-Host "- Create topic: $topic"
        docker exec kafka kafka-topics --create --bootstrap-server kafka:9092 --replication-factor 1 --partitions 3 --topic $topic --if-not-exists | Out-Host
    }
}

function Submit-SparkJobDetached {
    param(
        [Parameter(Mandatory = $true)][string]$AppName,
        [Parameter(Mandatory = $true)][string]$JobFile,
        [Parameter(Mandatory = $true)][string]$HdfsDataDir,
        [Parameter(Mandatory = $true)][string]$HdfsCheckpointDir
    )

    docker exec namenode hdfs dfs -mkdir -p $HdfsDataDir | Out-Null
    docker exec namenode hdfs dfs -mkdir -p $HdfsCheckpointDir | Out-Null
    docker exec namenode hdfs dfs -chmod -R 777 $HdfsDataDir | Out-Null
    docker exec namenode hdfs dfs -chmod -R 777 $HdfsCheckpointDir | Out-Null

    docker exec -d spark-master /opt/spark/bin/spark-submit `
        --master spark://spark-master:7077 `
        --deploy-mode client `
        --name $AppName `
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-client:3.2.1,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2 `
        --conf "spark.hadoop.fs.defaultFS=hdfs://namenode:9000" `
        --conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" `
        --conf "spark.sql.catalog.ais=org.apache.iceberg.spark.SparkCatalog" `
        --conf "spark.sql.catalog.ais.type=hadoop" `
        --conf "spark.sql.catalog.ais.warehouse=hdfs://namenode:9000/warehouse/iceberg" `
        --conf "spark.sql.adaptive.enabled=true" `
        --conf "spark.driver.memory=1g" `
        --conf "spark.executor.memory=1g" `
        $JobFile | Out-Null

    Write-Host "Submitted in detached mode: $AppName"
}

Write-Host "=== [1/7] Start core infrastructure ==="
docker compose up -d --build zookeeper kafka namenode datanode spark-master spark-worker cassandra | Out-Host

Wait-ForHealthy -ContainerName "kafka" -TimeoutSec 300
Wait-ForHealthy -ContainerName "namenode" -TimeoutSec 300
Wait-ForHealthy -ContainerName "spark-master" -TimeoutSec 300

Write-Host "=== [2/7] Create Kafka topics ==="
Ensure-Topics

Write-Host "=== [3/8] Ensure Iceberg catalog/tables ==="
docker exec spark-master /opt/spark/bin/spark-submit `
    --master spark://spark-master:7077 `
    --deploy-mode client `
    --name "AIS_EnsureIcebergTables" `
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.apache.hadoop:hadoop-client:3.2.1,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2 `
    --conf "spark.hadoop.fs.defaultFS=hdfs://namenode:9000" `
    --conf "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions" `
    --conf "spark.sql.catalog.ais=org.apache.iceberg.spark.SparkCatalog" `
    --conf "spark.sql.catalog.ais.type=hadoop" `
    --conf "spark.sql.catalog.ais.warehouse=hdfs://namenode:9000/warehouse/iceberg" `
    /opt/spark-jobs/ensure_iceberg_tables.py | Out-Host

Write-Host "=== [4/8] Start Spark streaming sinks (detached) ==="
Submit-SparkJobDetached -AppName "WeatherHistory_Streaming" -JobFile "/opt/spark-jobs/weather_streaming.py" -HdfsDataDir "/warehouse/iceberg/weather/weather_history_bronze" -HdfsCheckpointDir "/checkpoints/weather_history"
Submit-SparkJobDetached -AppName "OpenAQHourly_Streaming" -JobFile "/opt/spark-jobs/openaq_hourly_streaming.py" -HdfsDataDir "/warehouse/iceberg/air_quality/openaq_hourly_bronze" -HdfsCheckpointDir "/checkpoints/openaq_hourly"
Submit-SparkJobDetached -AppName "Sentinel5PSummary_Streaming" -JobFile "/opt/spark-jobs/sentinel5p_summary_streaming.py" -HdfsDataDir "/warehouse/iceberg/satellite/sentinel5p_summary_bronze" -HdfsCheckpointDir "/checkpoints/sentinel5p_summary"
Submit-SparkJobDetached -AppName "MAIACSummary_Streaming" -JobFile "/opt/spark-jobs/maiac_summary_streaming.py" -HdfsDataDir "/warehouse/iceberg/satellite/maiac_summary_bronze" -HdfsCheckpointDir "/checkpoints/maiac_summary"

Write-Host "=== [5/8] Historical backfill: Weather (last $lookbackDays days) ==="
docker compose run --rm -e WINDOW_MODE=batch -e BATCH_LOOKBACK_DAYS="$lookbackDays" ingest | Out-Host

Write-Host "=== [6/8] Historical backfill: OpenAQ, Sentinel-5P, MAIAC ==="
docker compose run --rm -e WINDOW_MODE=batch -e BATCH_LOOKBACK_DAYS="$lookbackDays" openaq-ingest | Out-Host
docker compose run --rm -e WINDOW_MODE=batch -e BATCH_LOOKBACK_DAYS="$lookbackDays" sentinel5p-ingest | Out-Host
docker compose run --rm -e WINDOW_MODE=batch -e BATCH_LOOKBACK_DAYS="$lookbackDays" maiac-ingest | Out-Host

Write-Host "=== [7/8] Start realtime loops for Weather + OpenAQ ==="
$prevWeatherWindowMode = $env:WEATHER_WINDOW_MODE
$prevWeatherRealtimeContinuous = $env:WEATHER_REALTIME_CONTINUOUS
$prevWeatherRealtimeLookback = $env:WEATHER_REALTIME_LOOKBACK_MINUTES
$prevWeatherRealtimePoll = $env:WEATHER_REALTIME_POLL_SECONDS
$prevOpenaqWindowMode = $env:OPENAQ_WINDOW_MODE
$prevOpenaqRealtimeContinuous = $env:OPENAQ_REALTIME_CONTINUOUS
$prevOpenaqRealtimeLookback = $env:OPENAQ_REALTIME_LOOKBACK_MINUTES
$prevOpenaqRealtimePoll = $env:OPENAQ_REALTIME_POLL_SECONDS

$env:WEATHER_WINDOW_MODE = "realtime"
$env:WEATHER_REALTIME_CONTINUOUS = "true"
$env:WEATHER_REALTIME_LOOKBACK_MINUTES = "$realtimeLookbackMinutes"
$env:WEATHER_REALTIME_POLL_SECONDS = "$realtimePollSeconds"
$env:OPENAQ_WINDOW_MODE = "realtime"
$env:OPENAQ_REALTIME_CONTINUOUS = "true"
$env:OPENAQ_REALTIME_LOOKBACK_MINUTES = "$realtimeLookbackMinutes"
$env:OPENAQ_REALTIME_POLL_SECONDS = "$realtimePollSeconds"

try {
    docker compose -p atmospheric_intelligence_sys---ais up -d --no-recreate ingest openaq-ingest | Out-Host
}
finally {
    $env:WEATHER_WINDOW_MODE = $prevWeatherWindowMode
    $env:WEATHER_REALTIME_CONTINUOUS = $prevWeatherRealtimeContinuous
    $env:WEATHER_REALTIME_LOOKBACK_MINUTES = $prevWeatherRealtimeLookback
    $env:WEATHER_REALTIME_POLL_SECONDS = $prevWeatherRealtimePoll
    $env:OPENAQ_WINDOW_MODE = $prevOpenaqWindowMode
    $env:OPENAQ_REALTIME_CONTINUOUS = $prevOpenaqRealtimeContinuous
    $env:OPENAQ_REALTIME_LOOKBACK_MINUTES = $prevOpenaqRealtimeLookback
    $env:OPENAQ_REALTIME_POLL_SECONDS = $prevOpenaqRealtimePoll
}

Write-Host "=== [8/8] Optional services (Monitoring/Airflow) ==="
if ($enableMonitoring) {
    try {
        docker compose up -d monitoring-ui | Out-Host
    }
    catch {
        Write-Host "[WARN] docker compose up failed, trying direct container start fallback..."
        docker start monitoring-ui | Out-Host
    }
}
else {
    Write-Host "[INFO] Skip Monitoring UI startup (ENABLE_MONITORING=false)"
}

if ($enableAirflow) {
    Write-Host "[INFO] ENABLE_AIRFLOW=true -> starting Airflow services"
    docker compose up airflow-init | Out-Host
    try {
        docker compose up -d airflow-webserver airflow-scheduler airflow-triggerer | Out-Host
    }
    catch {
        Write-Host "[WARN] docker compose up failed, trying direct container start fallback..."
        docker start airflow-webserver airflow-scheduler airflow-triggerer | Out-Host
    }
}
else {
    Write-Host "[INFO] Skip Airflow startup (ENABLE_AIRFLOW=false)"
}

Write-Host ""
Write-Host "DONE. Pipeline status checks:"
Write-Host "  bash scripts/check_pipeline.sh weather"
Write-Host "  bash scripts/check_pipeline.sh openaq"
Write-Host "  bash scripts/check_pipeline.sh sentinel5p"
Write-Host "  bash scripts/check_pipeline.sh maiac"
Write-Host ""
Write-Host "UIs:"
Write-Host "  NameNode:  http://localhost:9870"
Write-Host "  Spark:     http://localhost:8080"
if ($enableAirflow) {
    Write-Host "  Airflow:   http://localhost:8088"
}
if ($enableMonitoring) {
    Write-Host "  Monitor:   http://localhost:8501"
}
