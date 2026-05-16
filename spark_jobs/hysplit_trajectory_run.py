"""HYSPLIT trajectory runner (Spark job)

Reads ARL metadata from Iceberg (`ais.weather.era5_arl_files_bronze`) and
optionally runs HYSPLIT backward/forward trajectories. Writes run metadata
to `ais.trajectory.hysplit_runs_bronze`.

This implementation is resilient for environments without an actual HYSPLIT
binary: it will record `status='skipped'` with an explanatory message if the
configured HYSPLIT binary is not present. When running in the full cluster
environment where `/opt/hysplit/exec/` exists, set `HYSPLIT_BIN` env var.

Usage examples:
  spark-submit --master spark://spark-master:7077 \
    /opt/spark-jobs/hysplit_trajectory_run.py --start-date 2026-05-09 --end-date 2026-05-16 --direction backward
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import tempfile
from datetime import datetime, timezone
from typing import Dict, List

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


DEFAULT_HYSPLIT_BIN = os.environ.get("HYSPLIT_BIN", "/opt/hysplit/exec/hysplit")
DEFAULT_PM25_THRESHOLD = float(os.environ.get("PM25_TRIGGER_THRESHOLD", "75"))
ERA5_ARL_TABLE = os.environ.get("ERA5_ARL_TABLE", "ais.weather.era5_arl_files_bronze")
OPENAQ_TABLE = os.environ.get("OPENAQ_TABLE", "ais.air_quality.openaq_hanoi_hourly_silver")
HYSPLIT_RUNS_TABLE = os.environ.get("HYSPLIT_RUNS_TABLE", "ais.trajectory.hysplit_runs_bronze")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--start-date", type=str, help="YYYY-MM-DD inclusive start date", required=False)
    p.add_argument("--end-date", type=str, help="YYYY-MM-DD inclusive end date", required=False)
    p.add_argument("--direction", choices=("backward", "forward", "both"), default="both")
    p.add_argument("--full-refresh", action="store_true")
    return p.parse_args()


def make_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("HYSPLIT_Trajectory_Run")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.ais", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.ais.type", "hadoop")
        .config("spark.sql.catalog.ais.warehouse", os.environ.get("ICEBERG_WAREHOUSE", "hdfs://namenode:9000/warehouse/iceberg"))
        .config("spark.hadoop.fs.defaultFS", os.environ.get("HDFS_DEFAULT", "hdfs://namenode:9000"))
        .getOrCreate()
    )


def should_run_backward_for_time(spark: SparkSession, init_time_iso: str) -> bool:
    """Decide whether to run backward trajectories by checking PM2.5 at init_time.

    This queries the OpenAQ hourly table for the same hour. Returns True if
    the pm25 value >= threshold.
    """
    try:
        ts = datetime.fromisoformat(init_time_iso)
    except Exception:
        return True

    # Normalize to hour
    ts_hour = ts.replace(minute=0, second=0, microsecond=0)
    df = (
        spark.read.format("iceberg").load(OPENAQ_TABLE)
        .filter(col("hour") == ts_hour)
        .select("pm25")
    )
    val = df.limit(1).collect()
    if not val:
        return True
    try:
        pm25 = float(val[0][0])
    except Exception:
        return True
    return pm25 >= DEFAULT_PM25_THRESHOLD


def run_hysplit_local(hysplit_bin: str, arl_path: str, direction: str, run_id: str, init_lat: float | None = None, init_lon: float | None = None, init_alt_m: float | None = None, duration_hours: int = 72) -> Dict:
    """Attempt to run HYSPLIT locally. Returns metadata dict describing outcome."""
    meta: Dict = {
        "run_id": run_id,
        "direction": direction,
        "init_time": datetime.now(timezone.utc).isoformat(),
        "duration_hours": duration_hours,
        "init_lat": init_lat,
        "init_lon": init_lon,
        "init_alt_m": init_alt_m,
        "arl_path": arl_path,
        "output_path": None,
        "status": "skipped",
        "error_message": None,
        "spark_processed_at": datetime.now(timezone.utc).isoformat(),
    }

    if not os.path.exists(hysplit_bin):
        meta["error_message"] = f"HYSPLIT binary not found: {hysplit_bin}"
        return meta

    # Build a placeholder command. Real installations should set HYSPLIT_BIN to the actual executable and args.
    cmd = [hysplit_bin, "--arl", arl_path, "--direction", direction, "--duration", str(duration_hours)]
    if init_lat is not None and init_lon is not None:
        cmd += ["--lat", str(init_lat), "--lon", str(init_lon)]
    if init_alt_m is not None:
        cmd += ["--alt", str(init_alt_m)]

    try:
        proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False, text=True)
        if proc.returncode != 0:
            meta["status"] = "failed"
            meta["error_message"] = proc.stderr.strip()[:4000]
        else:
            meta["status"] = "success"
            # In a real deployment, parse stdout to locate the produced file(s) and
            # set `output_path` to the HDFS location where those files were copied.
            meta["output_path"] = proc.stdout.strip().splitlines()[-1] if proc.stdout else None
    except Exception as exc:
        meta["status"] = "failed"
        meta["error_message"] = str(exc)

    return meta


def main() -> None:
    args = parse_args()
    spark = make_spark()
    spark.sparkContext.setLogLevel("WARN")

    # Read ARL metadata
    arl_df = spark.read.format("iceberg").load(ERA5_ARL_TABLE)

    if args.start_date:
        arl_df = arl_df.filter(col("year") >= int(args.start_date[:4]))
    if args.end_date:
        arl_df = arl_df.filter(col("year") <= int(args.end_date[:4]))

    arl_rows = arl_df.collect()
    out_records: List[Dict] = []
    run_count = 0
    skipped_count = 0

    for r in arl_rows:
        # Extract expected fields with safe fallback
        arl_path = getattr(r, "arl_path", None) or getattr(r, "arl", None) or getattr(r, "source_nc", None)
        if not arl_path:
            continue

        # Build run_id from year/month/path
        run_id = f"hysplit-{getattr(r, 'year', 'na')}-{getattr(r, 'month', 'na')}-{os.path.basename(arl_path)}"

        for direction in ([args.direction] if args.direction in ("backward", "forward") else ("backward", "forward")):
            # If backward, consult OpenAQ to decide whether to run
            if direction == "backward":
                init_time_iso = getattr(r, "created_at", datetime.now(timezone.utc).isoformat())
                if not should_run_backward_for_time(spark, init_time_iso):
                    skipped_count += 1
                    out_records.append({
                        "run_id": run_id,
                        "direction": direction,
                        "init_time": init_time_iso,
                        "duration_hours": 72,
                        "init_lat": None,
                        "init_lon": None,
                        "init_alt_m": None,
                        "arl_path": arl_path,
                        "output_path": None,
                        "status": "skipped",
                        "error_message": "PM2.5 below threshold; skipping backward run",
                        "spark_processed_at": datetime.now(timezone.utc).isoformat(),
                    })
                    continue

            # Attempt run (may be a no-op if binary is missing)
            meta = run_hysplit_local(DEFAULT_HYSPLIT_BIN, arl_path, direction, run_id)
            out_records.append(meta)
            run_count += 1

    # Write metadata results back to Iceberg table
    if out_records:
        out_df = spark.createDataFrame(out_records)
        # Ensure the destination namespace exists and write append
        out_df.write.format("iceberg").mode("append").saveAsTable(HYSPLIT_RUNS_TABLE)

    print(f"HYSPLIT trajectory run summary: attempted={run_count}, skipped={skipped_count}, written={len(out_records)}")


if __name__ == "__main__":
    main()
