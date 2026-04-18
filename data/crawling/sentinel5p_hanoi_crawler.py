"""Sentinel-5P crawler for Vietnam area (3-day window by default).

This script queries Copernicus Data Space Ecosystem (CDSE) OData catalogue
for Sentinel-5P Level-2 gas products in a Vietnam bounding box.

Key defaults requested:
- Bounding box: Vietnam
- Crawl window: last 3 days
- Request timeout: 120 seconds

Optional download can be enabled via --download with CDSE_ACCESS_TOKEN.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests


CDSE_ODATA_URL = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"
CDSE_DOWNLOAD_URLS = (
    "https://zipper.dataspace.copernicus.eu/odata/v1/Products",
    "https://catalogue.dataspace.copernicus.eu/odata/v1/Products",
)
DEFAULT_TIMEOUT_SECONDS = 120
DEFAULT_MAX_PER_TYPE = 200

# Vietnam bbox: min_lon, min_lat, max_lon, max_lat
DEFAULT_VIETNAM_BBOX = (102.0, 8.0, 110.0, 24.0)

# Common Sentinel-5P gas product identifiers present in product names.
DEFAULT_GAS_PRODUCT_TYPES = (
    "L2__NO2___",
    "L2__SO2___",
    "L2__CO____",
    "L2__O3____",
    "L2__CH4___",
    "L2__HCHO__",
    "L2__AER_AI",
)


@dataclass(frozen=True)
class CrawlConfig:
    bbox: tuple[float, float, float, float]
    start_utc: datetime
    end_utc: datetime
    timeout_seconds: int
    max_per_type: int
    output_json: Path
    output_csv: Path
    download: bool
    download_dir: Path
    access_token: str
    max_downloads: int
    download_report: Path


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_iso_datetime(value: str) -> datetime:
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def to_odata_datetime(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def bbox_to_wkt_polygon(bbox: tuple[float, float, float, float]) -> str:
    min_lon, min_lat, max_lon, max_lat = bbox
    return (
        "POLYGON(("
        f"{min_lon} {min_lat},"
        f"{max_lon} {min_lat},"
        f"{max_lon} {max_lat},"
        f"{min_lon} {max_lat},"
        f"{min_lon} {min_lat}"
        "))"
    )


def build_filter(
    bbox: tuple[float, float, float, float],
    start_utc: datetime,
    end_utc: datetime,
    product_type: str,
) -> str:
    polygon = bbox_to_wkt_polygon(bbox)
    start_text = to_odata_datetime(start_utc)
    end_text = to_odata_datetime(end_utc)

    return (
        "Collection/Name eq 'SENTINEL-5P'"
        f" and ContentDate/Start ge {start_text}"
        f" and ContentDate/Start le {end_text}"
        f" and OData.CSC.Intersects(area=geography'SRID=4326;{polygon}')"
        f" and contains(Name,'{product_type}')"
    )


def fetch_products_for_type(
    session: requests.Session,
    cfg: CrawlConfig,
    product_type: str,
) -> list[dict[str, Any]]:
    all_items: list[dict[str, Any]] = []

    params: dict[str, Any] = {
        "$filter": build_filter(cfg.bbox, cfg.start_utc, cfg.end_utc, product_type),
        "$top": cfg.max_per_type,
        "$orderby": "ContentDate/Start desc",
        "$select": "Id,Name,ContentDate,OriginDate,PublicationDate,S3Path,GeoFootprint,Online",
    }

    next_url: str | None = CDSE_ODATA_URL
    next_params: dict[str, Any] | None = params

    while next_url:
        response = session.get(next_url, params=next_params, timeout=cfg.timeout_seconds)
        response.raise_for_status()
        payload = response.json()

        values = payload.get("value", [])
        for item in values:
            item["product_type"] = product_type
        all_items.extend(values)

        next_url = payload.get("@odata.nextLink")
        next_params = None

        if len(all_items) >= cfg.max_per_type:
            return all_items[: cfg.max_per_type]

    return all_items


def normalize_item(item: dict[str, Any]) -> dict[str, Any]:
    content_date = item.get("ContentDate", {}) or {}
    return {
        "id": item.get("Id"),
        "name": item.get("Name"),
        "product_type": item.get("product_type"),
        "start_time_utc": content_date.get("Start"),
        "end_time_utc": content_date.get("End"),
        "origin_date": item.get("OriginDate"),
        "publication_date": item.get("PublicationDate"),
        "online": item.get("Online"),
        "s3_path": item.get("S3Path"),
        "geofootprint": item.get("GeoFootprint"),
    }


def save_json(path: Path, records: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)


def save_csv(path: Path, records: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "id",
        "name",
        "product_type",
        "start_time_utc",
        "end_time_utc",
        "origin_date",
        "publication_date",
        "online",
        "s3_path",
    ]
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in records:
            writer.writerow({k: row.get(k) for k in fieldnames})


def save_download_report(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["id", "name", "product_type", "status", "file_path", "error"]
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k) for k in fieldnames})


def download_products(
    session: requests.Session,
    cfg: CrawlConfig,
    records: list[dict[str, Any]],
) -> None:
    token = cfg.access_token or os.getenv("CDSE_ACCESS_TOKEN", "").strip()
    if not token:
        print("[WARN] Missing CDSE_ACCESS_TOKEN, skip download mode.")
        return

    cfg.download_dir.mkdir(parents=True, exist_ok=True)
    headers = {"Authorization": f"Bearer {token}"}
    report_rows: list[dict[str, Any]] = []

    downloaded = 0
    skipped = 0
    failed = 0

    for idx, row in enumerate(records, start=1):
        if cfg.max_downloads > 0 and downloaded >= cfg.max_downloads:
            break

        product_id = row.get("id")
        name = row.get("name") or f"product_{idx}"
        product_type = row.get("product_type")
        if not product_id:
            skipped += 1
            report_rows.append(
                {
                    "id": "",
                    "name": name,
                    "product_type": product_type,
                    "status": "skipped_missing_id",
                    "file_path": "",
                    "error": "Missing product id",
                }
            )
            continue

        if not row.get("online", True):
            skipped += 1
            report_rows.append(
                {
                    "id": product_id,
                    "name": name,
                    "product_type": product_type,
                    "status": "skipped_offline",
                    "file_path": "",
                    "error": "Product is offline in catalogue",
                }
            )
            continue

        safe_name = "".join(ch if ch.isalnum() or ch in ("-", "_", ".") else "_" for ch in name)
        output_path = cfg.download_dir / f"{safe_name}.nc"
        if output_path.exists():
            skipped += 1
            report_rows.append(
                {
                    "id": product_id,
                    "name": name,
                    "product_type": product_type,
                    "status": "skipped_exists",
                    "file_path": str(output_path),
                    "error": "",
                }
            )
            continue

        download_urls = [f"{base}({product_id})/$value" for base in CDSE_DOWNLOAD_URLS]
        last_error = ""
        for attempt in range(1, 4):
            try:
                downloaded_this_item = False
                auth_failed = False

                for url in download_urls:
                    with session.get(url, headers=headers, timeout=cfg.timeout_seconds, stream=True) as resp:
                        if resp.status_code in (401, 403):
                            auth_failed = True
                            continue
                        resp.raise_for_status()
                        with output_path.open("wb") as f:
                            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                                if chunk:
                                    f.write(chunk)
                        downloaded_this_item = True
                        break

                if auth_failed and not downloaded_this_item:
                    print("[WARN] Unauthorized download response on all CDSE download endpoints.")
                    save_download_report(cfg.download_report, report_rows)
                    return

                if not downloaded_this_item:
                    raise requests.RequestException("No CDSE download endpoint returned file content")

                downloaded += 1
                report_rows.append(
                    {
                        "id": product_id,
                        "name": name,
                        "product_type": product_type,
                        "status": "downloaded",
                        "file_path": str(output_path),
                        "error": "",
                    }
                )
                print(f"[INFO] Downloaded {downloaded}: {output_path.name}")
                break
            except requests.RequestException as exc:
                last_error = str(exc)
                if output_path.exists():
                    output_path.unlink(missing_ok=True)
                if attempt < 3:
                    time.sleep(2 * attempt)
                else:
                    failed += 1
                    report_rows.append(
                        {
                            "id": product_id,
                            "name": name,
                            "product_type": product_type,
                            "status": "failed",
                            "file_path": str(output_path),
                            "error": last_error,
                        }
                    )
                    print(f"[WARN] Failed download: {name} | {last_error}")

    save_download_report(cfg.download_report, report_rows)
    print(f"[INFO] Download report: {cfg.download_report}")
    print(f"[INFO] Download summary => downloaded={downloaded}, skipped={skipped}, failed={failed}")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Crawl Sentinel-5P gas metadata for Vietnam from CDSE OData.",
    )
    parser.add_argument("--min-lon", type=float, default=DEFAULT_VIETNAM_BBOX[0])
    parser.add_argument("--min-lat", type=float, default=DEFAULT_VIETNAM_BBOX[1])
    parser.add_argument("--max-lon", type=float, default=DEFAULT_VIETNAM_BBOX[2])
    parser.add_argument("--max-lat", type=float, default=DEFAULT_VIETNAM_BBOX[3])
    parser.add_argument(
        "--start",
        type=str,
        default="",
        help="ISO datetime UTC. Default: now-3days.",
    )
    parser.add_argument(
        "--end",
        type=str,
        default="",
        help="ISO datetime UTC. Default: now.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT_SECONDS,
        help="HTTP request timeout in seconds. Default: 120.",
    )
    parser.add_argument(
        "--max-per-type",
        type=int,
        default=DEFAULT_MAX_PER_TYPE,
        help="Maximum records per gas product type.",
    )
    parser.add_argument(
        "--output-json",
        type=Path,
        default=Path("data/crawling/outputs/sentinel5p_vietnam_last_3d.json"),
    )
    parser.add_argument(
        "--output-csv",
        type=Path,
        default=Path("data/crawling/outputs/sentinel5p_vietnam_last_3d.csv"),
    )
    parser.add_argument(
        "--download",
        action="store_true",
        help="Download product files (.nc) using CDSE_ACCESS_TOKEN.",
    )
    parser.add_argument(
        "--download-dir",
        type=Path,
        default=Path("data/crawling/sentinel5p_downloads"),
    )
    parser.add_argument(
        "--access-token",
        type=str,
        default="",
        help="CDSE OAuth token. If empty, use CDSE_ACCESS_TOKEN env var.",
    )
    parser.add_argument(
        "--max-downloads",
        type=int,
        default=0,
        help="Maximum number of .nc files to download (0 means all).",
    )
    parser.add_argument(
        "--download-report",
        type=Path,
        default=Path("data/crawling/outputs/sentinel5p_download_report.csv"),
    )
    return parser


def make_config(args: argparse.Namespace) -> CrawlConfig:
    now = utc_now()
    end_utc = parse_iso_datetime(args.end) if args.end else now
    start_utc = parse_iso_datetime(args.start) if args.start else (end_utc - timedelta(days=3))

    return CrawlConfig(
        bbox=(args.min_lon, args.min_lat, args.max_lon, args.max_lat),
        start_utc=start_utc,
        end_utc=end_utc,
        timeout_seconds=args.timeout,
        max_per_type=args.max_per_type,
        output_json=args.output_json,
        output_csv=args.output_csv,
        download=args.download,
        download_dir=args.download_dir,
        access_token=args.access_token.strip(),
        max_downloads=max(0, args.max_downloads),
        download_report=args.download_report,
    )


def main() -> None:
    args = build_arg_parser().parse_args()
    cfg = make_config(args)

    if cfg.start_utc >= cfg.end_utc:
        raise ValueError("start time must be earlier than end time")

    print("[INFO] Sentinel-5P Vietnam crawler")
    print(
        "[INFO] Window UTC: "
        f"{cfg.start_utc.isoformat()} -> {cfg.end_utc.isoformat()}"
    )
    print(f"[INFO] BBox: {cfg.bbox}")
    print(f"[INFO] Request timeout: {cfg.timeout_seconds}s")

    session = requests.Session()
    all_records: list[dict[str, Any]] = []

    for product_type in DEFAULT_GAS_PRODUCT_TYPES:
        print(f"[INFO] Query product type: {product_type}")
        try:
            raw_items = fetch_products_for_type(session, cfg, product_type)
            normalized = [normalize_item(item) for item in raw_items]
            all_records.extend(normalized)
            print(f"[INFO]   -> {len(normalized)} records")
        except requests.RequestException as exc:
            print(f"[WARN] Request failed for {product_type}: {exc}")

    all_records.sort(
        key=lambda x: (x.get("product_type") or "", x.get("start_time_utc") or ""),
        reverse=True,
    )

    save_json(cfg.output_json, all_records)
    save_csv(cfg.output_csv, all_records)

    print(f"[INFO] Saved JSON: {cfg.output_json}")
    print(f"[INFO] Saved CSV:  {cfg.output_csv}")
    print(f"[INFO] Total records: {len(all_records)}")

    if cfg.download:
        print("[INFO] Download mode enabled")
        print(f"[INFO] Download dir: {cfg.download_dir}")
        download_products(session, cfg, all_records)


if __name__ == "__main__":
    main()
