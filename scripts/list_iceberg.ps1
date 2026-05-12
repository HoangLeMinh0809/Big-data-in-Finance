<#
.SYNOPSIS
  Helper script to run Spark SQL commands against Iceberg from PowerShell.

.DESCRIPTION
  Sets catalog/warehouse defaults, ensures `spark-sql` is present, and runs
  common queries: SHOW NAMESPACES, SHOW TABLES IN namespace, DESCRIBE TABLE,
  COUNT rows and SELECT sample rows. Designed for PowerShell on Windows.

.EXAMPLE
  .\scripts\list_iceberg.ps1 -IcebergCatalog ais -Namespace features

  .\scripts\list_iceberg.ps1 -IcebergCatalog ais -Table features.hanoi_pm25_master_hourly_gold -DescribeAll
#>

param(
    [string]$IcebergCatalog = "ais",
    [string]$IcebergWarehouse = "hdfs://namenode:9000/warehouse/iceberg",
    [string]$Namespace = "features",
    [string]$Table = "",
    [switch]$DescribeAll,
    [switch]$Count,
    [int]$Sample = 5,
    [switch]$UseDocker,
    [string]$DockerService = "spark-master",
    [string]$DockerSparkSqlPath = "/opt/spark/bin/spark-sql",
    [string]$SparkPackages = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2"
)

$script:ComposeCmd = $null
$script:ComposeArgs = @()

if (Get-Command docker -ErrorAction SilentlyContinue) {
    & docker compose version > $null 2>&1
    if ($LASTEXITCODE -eq 0) {
        $script:ComposeCmd = "docker"
        $script:ComposeArgs = @("compose")
    }
}

if (-not $script:ComposeCmd -and (Get-Command docker-compose -ErrorAction SilentlyContinue)) {
    $script:ComposeCmd = "docker-compose"
    $script:ComposeArgs = @()
}

function Invoke-Compose {
    param([string[]]$ComposeCommandArgs)
    if (-not $script:ComposeCmd) {
        Write-Error "Docker Compose not found. Install Docker Desktop or ensure docker-compose is on PATH."
        exit 2
    }
    & $script:ComposeCmd @script:ComposeArgs @ComposeCommandArgs
    return $LASTEXITCODE
}

function Invoke-SparkSql {
    param([string]$Query)

    $confArgs = @(
        "--conf", "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "--conf", "spark.sql.catalog.$env:ICEBERG_CATALOG=org.apache.iceberg.spark.SparkCatalog",
        "--conf", "spark.sql.catalog.$env:ICEBERG_CATALOG.type=hadoop",
        "--conf", "spark.sql.catalog.$env:ICEBERG_CATALOG.warehouse=$env:ICEBERG_WAREHOUSE"
    )

    $sparkSqlArgs = $confArgs + @("-e", $Query)

    $spark = Get-Command spark-sql -ErrorAction SilentlyContinue
    if ($spark -and -not $UseDocker) {
        & spark-sql @sparkSqlArgs
        return $LASTEXITCODE
    }

    if (-not $spark -and -not $UseDocker) {
        Write-Warning "spark-sql not found in PATH; falling back to Docker Compose."
    }

    $confArgs = @(
        "--conf", "spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "--conf", "spark.sql.catalog.$env:ICEBERG_CATALOG=org.apache.iceberg.spark.SparkCatalog",
        "--conf", "spark.sql.catalog.$env:ICEBERG_CATALOG.type=hadoop",
        "--conf", "spark.sql.catalog.$env:ICEBERG_CATALOG.warehouse=$env:ICEBERG_WAREHOUSE"
    )
    $packageArgs = @()
    if ($SparkPackages) {
        $packageArgs = @("--packages", $SparkPackages)
    }
    Write-Host "Running via Docker Compose in service: $DockerService" -ForegroundColor Yellow
    $execArgs = @("exec", $DockerService, $DockerSparkSqlPath) + $packageArgs + $confArgs + @("-e", $Query)
    return Invoke-Compose $execArgs
}

# Set environment variables for child processes
$env:ICEBERG_CATALOG = $IcebergCatalog
$env:ICEBERG_WAREHOUSE = $IcebergWarehouse

Write-Host "Using Iceberg catalog: $env:ICEBERG_CATALOG" -ForegroundColor Cyan
Write-Host "Using Iceberg warehouse: $env:ICEBERG_WAREHOUSE" -ForegroundColor Cyan

if ($DescribeAll) {
    if (-not $Table) { Write-Error "-DescribeAll requires -Table <catalog.namespace.table>"; exit 3 }
    Write-Host "Describing table $Table..." -ForegroundColor Green
    Invoke-SparkSql "DESCRIBE TABLE $Table;"
    if ($Count) {
        Write-Host "Counting rows in $Table..." -ForegroundColor Green
        Invoke-SparkSql "SELECT COUNT(*) FROM $Table;"
    }
    Write-Host "Selecting sample rows from $Table..." -ForegroundColor Green
    Invoke-SparkSql "SELECT * FROM $Table LIMIT $Sample;"
    exit 0
}

Write-Host "Showing namespaces in catalog $env:ICEBERG_CATALOG..." -ForegroundColor Green
Invoke-SparkSql "SHOW NAMESPACES IN $env:ICEBERG_CATALOG;"

if ($Namespace) {
    Write-Host "Showing tables in $env:ICEBERG_CATALOG.$Namespace..." -ForegroundColor Green
    Invoke-SparkSql "SHOW TABLES IN $env:ICEBERG_CATALOG.$Namespace;"
}

if ($Table) {
    Write-Host "Describing table $Table..." -ForegroundColor Green
    Invoke-SparkSql "DESCRIBE TABLE $Table;"
    if ($Count) {
        Write-Host "Counting rows in $Table..." -ForegroundColor Green
        Invoke-SparkSql "SELECT COUNT(*) FROM $Table;"
    }
    Write-Host "Selecting sample rows from $Table..." -ForegroundColor Green
    Invoke-SparkSql "SELECT * FROM $Table LIMIT $Sample;"
}

Write-Host "Done." -ForegroundColor Cyan

# Optional HDFS metadata listing if hdfs cli available
if (Get-Command hdfs -ErrorAction SilentlyContinue) {
    Write-Host "Listing Iceberg warehouse path on HDFS: $env:ICEBERG_WAREHOUSE" -ForegroundColor Yellow
    # strip scheme and host if present to show path
    $path = $env:ICEBERG_WAREHOUSE -replace '^.*?://[^/]+', ''
    if (-not [string]::IsNullOrEmpty($path)) {
        Write-Host "HDFS path: $path" -ForegroundColor Yellow
        & hdfs dfs -ls $path
    }
}
