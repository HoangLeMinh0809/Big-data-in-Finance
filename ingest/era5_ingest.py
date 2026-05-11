from __future__ import annotations

import argparse
import hashlib
import logging
import os
import posixpath
import tempfile
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import cdsapi
import requests

from kafka_utils import create_kafka_producer, send_event

try:
    import yaml
except ModuleNotFoundError as exc:  # pragma: no cover
    raise ModuleNotFoundError(
        "Missing dependency 'pyyaml'. Install from ingest/requirements.txt"
    ) from exc


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("era5_ingest")


@dataclass(frozen=True)
class Era5Region:
    west: float
    east: float
    south: float
    north: float


def _utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def _month_start(d: date) -> date:
    return d.replace(day=1)


def _next_month(d: date) -> date:
    if d.month == 12:
        return d.replace(year=d.year + 1, month=1, day=1)
    return d.replace(month=d.month + 1, day=1)


def _iter_months(start: date, end: date) -> list[tuple[int, int]]:
    cur = _month_start(start)
    last = _month_start(end)
    months: list[tuple[int, int]] = []
    while cur <= last:
        months.append((cur.year, cur.month))
        cur = _next_month(cur)
    return months


def _load_yaml(path: str) -> dict[str, Any]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Missing config file: {p}")
    with p.open("r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    if not isinstance(cfg, dict):
        raise ValueError(f"Invalid YAML root in {p}")
    return cfg


def _require_mapping(obj: dict[str, Any], key: str) -> dict[str, Any]:
    value = obj.get(key)
    if not isinstance(value, dict):
        raise KeyError(f"Missing/invalid config section '{key}'")
    return value


def _require_list(obj: dict[str, Any], key: str) -> list[Any]:
    value = obj.get(key)
    if not isinstance(value, list):
        raise KeyError(f"Missing/invalid config list '{key}'")
    return value


def _region_from_cfg(cfg: dict[str, Any]) -> Era5Region:
    era5 = _require_mapping(cfg, "era5")
    region = _require_mapping(era5, "region")
    return Era5Region(
        west=float(region["west"]),
        east=float(region["east"]),
        south=float(region["south"]),
        north=float(region["north"]),
    )


def _surface_vars_from_cfg(cfg: dict[str, Any]) -> list[str]:
    era5 = _require_mapping(cfg, "era5")
    variables = _require_list(era5, "surface_variables")
    return [str(v) for v in variables]


def _raw_base_path_from_cfg(cfg: dict[str, Any]) -> str:
    era5 = _require_mapping(cfg, "era5")
    raw_base_path = era5.get("raw_base_path")
    if not raw_base_path:
        raise KeyError("Missing era5.raw_base_path in config")
    return str(raw_base_path).rstrip("/")


def _sha256_of_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _split_hdfs_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith("hdfs://"):
        raise ValueError(f"Expected hdfs:// URI, got: {uri}")
    rest = uri[len("hdfs://") :]
    host_port, path = rest.split("/", 1)
    return host_port, "/" + path


def _webhdfs_base() -> str:
    return os.getenv("WEBHDFS_BASE", "http://namenode:9870/webhdfs/v1").rstrip("/")


def _webhdfs_path_url(hdfs_uri: str) -> str:
    _, abs_path = _split_hdfs_uri(hdfs_uri)
    return f"{_webhdfs_base()}{abs_path}"


def _hdfs_path_exists(hdfs_uri: str) -> bool:
    url = _webhdfs_path_url(hdfs_uri)
    response = requests.get(
        url,
        params={"op": "GETFILESTATUS", "user.name": os.getenv("HDFS_USER", "root")},
        timeout=30,
    )
    if response.status_code == 200:
        return True
    if response.status_code == 404:
        return False
    response.raise_for_status()
    return False


def _hdfs_mkdirs(hdfs_uri: str) -> None:
    _, abs_path = _split_hdfs_uri(hdfs_uri)
    parent = posixpath.dirname(abs_path)
    url = f"{_webhdfs_base()}{parent}"
    response = requests.put(
        url,
        params={"op": "MKDIRS", "user.name": os.getenv("HDFS_USER", "root")},
        timeout=60,
    )
    response.raise_for_status()


def _hdfs_put(local_path: Path, hdfs_uri: str) -> None:
    _hdfs_mkdirs(hdfs_uri)
    url = _webhdfs_path_url(hdfs_uri)
    params = {
        "op": "CREATE",
        "overwrite": "true",
        "user.name": os.getenv("HDFS_USER", "root"),
    }
    with local_path.open("rb") as f:
        response = requests.put(url, params=params, data=f, allow_redirects=True)
    response.raise_for_status()


def _build_surface_request(
    *,
    variables: list[str],
    region: Era5Region,
    year: int,
    month: int,
    start_date: date,
    end_date: date,
) -> dict[str, Any]:
    # Only request days within the window.
    month_first = date(year, month, 1)
    month_last = _next_month(month_first) - timedelta(days=1)

    req_start = max(start_date, month_first)
    req_end = min(end_date, month_last)

    days: list[str] = []
    cur = req_start
    while cur <= req_end:
        days.append(f"{cur.day:02d}")
        cur = cur + timedelta(days=1)

    return {
        "product_type": "reanalysis",
        "variable": variables,
        "year": [str(year)],
        "month": [f"{month:02d}"],
        "day": days,
        "time": [f"{h:02d}:00" for h in range(24)],
        "area": [region.north, region.west, region.south, region.east],
        "format": "netcdf",
    }


def _event_payload(
    *,
    dataset_type: str,
    year: int,
    month: int,
    start_utc: datetime,
    end_utc: datetime,
    region: Era5Region,
    file_path: str,
    file_size: int,
    checksum: str,
) -> dict[str, Any]:
    ingest_time = datetime.now(timezone.utc)
    return {
        "event_id": f"era5_{dataset_type}_{year}_{month:02d}",
        "dataset_type": dataset_type,
        "year": year,
        "month": month,
        "start_time": _utc(start_utc).isoformat(),
        "end_time": _utc(end_utc).isoformat(),
        "bbox": [region.west, region.south, region.east, region.north],
        "file_path": file_path,
        "file_size": int(file_size),
        "checksum": checksum,
        "source": "era5_cds",
        "ingest_time": ingest_time.isoformat(),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Download ERA5 files and publish metadata to Kafka")
    parser.add_argument("--start-date", default=os.getenv("ERA5_START_DATE", ""), help="YYYY-MM-DD")
    parser.add_argument("--end-date", default=os.getenv("ERA5_END_DATE", ""), help="YYYY-MM-DD")
    parser.add_argument(
        "--dataset-type",
        default=os.getenv("ERA5_DATASET_TYPE", "surface"),
        choices=["surface"],
        help="ERA5 dataset type",
    )
    parser.add_argument(
        "--output-base-path",
        default=os.getenv("ERA5_OUTPUT_BASE_PATH", "") or None,
        help="Override base HDFS path (default from config era5.raw_base_path)",
    )
    parser.add_argument(
        "--config",
        default=os.getenv("HANOI_PIPELINE_CONFIG", "config/hanoi_pipeline.yaml"),
        help="Path to config/hanoi_pipeline.yaml",
    )
    parser.add_argument(
        "--kafka-bootstrap",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
    )
    parser.add_argument(
        "--topic",
        default=os.getenv("KAFKA_TOPIC", "era5-files"),
    )
    parser.add_argument(
        "--skip-existing",
        default=os.getenv("ERA5_SKIP_EXISTING", "true"),
        help="Skip download if HDFS file already exists (true/false)",
    )

    args = parser.parse_args()

    if not args.start_date or not args.end_date:
        raise SystemExit(
            "Missing --start-date/--end-date (or ERA5_START_DATE/ERA5_END_DATE env vars)"
        )

    cfg = _load_yaml(args.config)
    region = _region_from_cfg(cfg)

    if args.dataset_type != "surface":
        raise ValueError("Only --dataset-type surface is implemented for TODO_1")

    variables = _surface_vars_from_cfg(cfg)
    raw_base_path = (args.output_base_path or _raw_base_path_from_cfg(cfg)).rstrip("/")

    start_d = _parse_date(args.start_date)
    end_d = _parse_date(args.end_date)

    if end_d < start_d:
        raise ValueError("end-date must be >= start-date")

    skip_existing = str(args.skip_existing).strip().lower() in {"1", "true", "yes", "y", "on"}

    producer = create_kafka_producer(args.kafka_bootstrap, logger)

    months = _iter_months(start_d, end_d)
    logger.info(f"ERA5 ingest months: {months}")

    # Initialize CDS client with credentials from environment
    cds_url = os.environ.get("CDS_URL", "https://cds.climate.copernicus.eu/api/v2")
    cds_key = os.environ.get("CDS_KEY")
    
    if not cds_key:
        raise ValueError("CDS_KEY environment variable is required for ERA5 ingest")
    
    logger.info(f"Connecting to CDS: {cds_url}")
    client = cdsapi.Client(url=cds_url, key=cds_key)

    for year, month in months:
        hdfs_path = (
            f"{raw_base_path}/{args.dataset_type}/year={year}/month={month:02d}/"
            f"era5_{args.dataset_type}_{year}{month:02d}.nc"
        )

        if skip_existing and _hdfs_path_exists(hdfs_path):
            logger.info(f"Skip existing HDFS file: {hdfs_path}")
            continue

        logger.info(f"Downloading ERA5 {args.dataset_type} {year}-{month:02d} -> {hdfs_path}")

        request = _build_surface_request(
            variables=variables,
            region=region,
            year=year,
            month=month,
            start_date=start_d,
            end_date=end_d,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            local_path = Path(tmpdir) / f"era5_{args.dataset_type}_{year}{month:02d}.nc"

            # Retry download.
            max_retries = int(os.getenv("ERA5_DOWNLOAD_MAX_RETRIES", "3"))
            for attempt in range(1, max_retries + 1):
                try:
                    client.retrieve("reanalysis-era5-single-levels", request, str(local_path))
                    break
                except Exception as exc:
                    if attempt >= max_retries:
                        raise
                    delay = 10 * attempt
                    logger.warning(f"Download failed attempt {attempt}/{max_retries}: {exc}; sleep {delay}s")
                    import time

                    time.sleep(delay)

            size = local_path.stat().st_size
            checksum = _sha256_of_file(local_path)

            _hdfs_put(local_path, hdfs_path)

        # Compute time range for the event.
        month_first = date(year, month, 1)
        month_last = _next_month(month_first) - timedelta(days=1)
        req_start = max(start_d, month_first)
        req_end = min(end_d, month_last)

        start_time = datetime(req_start.year, req_start.month, req_start.day, 0, 0, tzinfo=timezone.utc)
        end_time = datetime(req_end.year, req_end.month, req_end.day, 23, 0, tzinfo=timezone.utc)

        event = _event_payload(
            dataset_type=args.dataset_type,
            year=year,
            month=month,
            start_utc=start_time,
            end_utc=end_time,
            region=region,
            file_path=hdfs_path,
            file_size=size,
            checksum=checksum,
        )

        ok = send_event(producer, args.topic, event, logger, key_field="event_id", wait_for_ack=True)
        if ok:
            logger.info(f"Published Kafka event: {event['event_id']}")
        else:
            logger.error(f"Failed to publish Kafka event: {event['event_id']}")

    producer.flush()
    producer.close()
    logger.info("ERA5 ingest complete")


if __name__ == "__main__":
    main()
