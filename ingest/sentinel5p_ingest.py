"""
Sentinel-5P Product Metadata Ingest
====================================
Fetches Sentinel-5P product metadata from Copernicus Data Space using their ODATA API
and publishes summary events to Kafka without downloading full files.

Products supported: NO2, CO, O3, SO2, CH4, AER
Window modes: batch (historical) or realtime (continuous/polling)
"""

import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from tempfile import TemporaryDirectory

import requests

from kafka_utils import create_kafka_producer as create_shared_kafka_producer, send_event
from window_utils import (
    build_default_window_config,
    parse_bool,
    resolve_window,
    to_utc_iso,
    utc_now,
    write_window_state,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("sentinel5p_ingest")

# =============================================================================
# Copernicus Data Space API endpoints
# =============================================================================
AUTH_URL = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
ODATA_URL = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"

# =============================================================================
# Product definitions for Sentinel-5P
# =============================================================================
PRODUCTS_DEF = {
    "NO2": {
        "collection": "SENTINEL-5P",
        "type_filter": "L2__NO2___",
        "variable": "nitrogendioxide_tropospheric_column",
        "group": "PRODUCT",
        "unit": "mol/m²",
    },
    "CO": {
        "collection": "SENTINEL-5P",
        "type_filter": "L2__CO____",
        "variable": "carbonmonoxide_total_column",
        "group": "PRODUCT",
        "unit": "mol/m²",
    },
    "O3": {
        "collection": "SENTINEL-5P",
        "type_filter": "L2__O3____",
        "variable": "ozone_total_vertical_column",
        "group": "PRODUCT",
        "unit": "mol/m²",
    },
    "SO2": {
        "collection": "SENTINEL-5P",
        "type_filter": "L2__SO2___",
        "variable": "sulfurdioxide_total_vertical_column",
        "group": "PRODUCT",
        "unit": "mol/m²",
    },
    "CH4": {
        "collection": "SENTINEL-5P",
        "type_filter": "L2__CH4___",
        "variable": "methane_mixing_ratio_bias_corrected",
        "group": "PRODUCT",
        "unit": "ppb",
    },
    "AER": {
        "collection": "SENTINEL-5P",
        "type_filter": "L2__AER_AI",
        "variable": "aerosol_index_354_388",
        "group": "PRODUCT",
        "unit": "unitless",
    },
}

# =============================================================================
# Configuration from environment
# =============================================================================
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sentinel5p-summary")

CDSE_USERNAME = os.getenv("CDSE_USERNAME", "").strip()
CDSE_PASSWORD = os.getenv("CDSE_PASSWORD", "").strip()

# Parse bounding box: min_lon,min_lat,max_lon,max_lat
BBOX_RAW = os.getenv("BBOX", "100,8,110,24")
BBOX = [float(x.strip()) for x in BBOX_RAW.split(",")]

MAX_RESULTS = int(os.getenv("MAX_RESULTS", "1"))
PRODUCTS = [
    p.strip()
    for p in os.getenv("PRODUCTS", "NO2,CO,O3,SO2,CH4,AER").split(",")
    if p.strip()
]

REQUEST_TIMEOUT_SEC = int(os.getenv("REQUEST_TIMEOUT_SEC", "30"))
REQUEST_DELAY_SEC = float(os.getenv("REQUEST_DELAY_SEC", "0.2"))
SEND_DELAY_MS = int(os.getenv("SEND_DELAY_MS", "0"))
DOWNLOAD_RAW = parse_bool(os.getenv("S5P_DOWNLOAD_RAW", os.getenv("DOWNLOAD_RAW", "false")), default=False)
RAW_HDFS_BASE_PATH = os.getenv("S5P_RAW_HDFS_BASE_PATH", "/raw/sentinel5p").strip().rstrip("/")
HDFS_WEBHDFS_BASE = os.getenv("HDFS_WEBHDFS_BASE", "http://namenode:9870/webhdfs/v1").strip().rstrip("/")
DOWNLOAD_TIMEOUT_SEC = int(os.getenv("DOWNLOAD_TIMEOUT_SEC", "600"))
DOWNLOAD_CHUNK_SIZE = int(os.getenv("DOWNLOAD_CHUNK_SIZE", str(1024 * 1024)))
MAX_DOWNLOAD_BYTES = int(os.getenv("S5P_MAX_DOWNLOAD_BYTES", "0"))

WINDOW_CONFIG = build_default_window_config(
    mode=os.getenv("WINDOW_MODE", "batch"),
    batch_lookback_days=int(os.getenv("BATCH_LOOKBACK_DAYS", "7")),
    realtime_lookback_minutes=int(os.getenv("REALTIME_LOOKBACK_MINUTES", "10")),
    poll_seconds=int(os.getenv("REALTIME_POLL_SECONDS", "600")),
    continuous=parse_bool(os.getenv("REALTIME_CONTINUOUS", "false"), default=False),
    start_override=os.getenv("WINDOW_START_UTC", ""),
    end_override=os.getenv("WINDOW_END_UTC", ""),
    state_file=os.getenv("WINDOW_STATE_FILE", "/tmp/sentinel5p_window_state.json"),
)


# =============================================================================
# Utility functions
# =============================================================================
def get_access_token(username: str, password: str) -> str:
    """Authenticate with Copernicus Data Space and get access token."""
    resp = requests.post(
        AUTH_URL,
        data={
            "username": username,
            "password": password,
            "client_id": "cdse-public",
            "grant_type": "password",
        },
        timeout=REQUEST_TIMEOUT_SEC,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def to_odata_datetime(dt: datetime) -> str:
    """Format datetime for ODATA filter (Copernicus API expects this format)."""
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")


def _safe_int(value) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _parse_content_date_day(content_start: str | None) -> tuple[int, int, int]:
    if content_start:
        try:
            parsed = datetime.fromisoformat(content_start.replace("Z", "+00:00"))
            parsed = parsed.astimezone(timezone.utc)
            return parsed.year, parsed.month, parsed.day
        except ValueError:
            pass

    now = datetime.now(timezone.utc)
    return now.year, now.month, now.day


def build_download_url(product_id: str) -> str:
    return f"https://download.dataspace.copernicus.eu/odata/v1/Products({product_id})/$value"


def _webhdfs_path(path: str) -> str:
    return f"{HDFS_WEBHDFS_BASE}/{path.strip('/')}"


def webhdfs_exists(path: str) -> bool:
    response = requests.get(
        _webhdfs_path(path),
        params={"op": "GETFILESTATUS"},
        timeout=REQUEST_TIMEOUT_SEC,
    )
    if response.status_code == 200:
        return True
    if response.status_code == 404:
        return False
    response.raise_for_status()
    return False


def webhdfs_mkdirs(path: str) -> None:
    response = requests.put(
        _webhdfs_path(path),
        params={"op": "MKDIRS"},
        timeout=REQUEST_TIMEOUT_SEC,
    )
    response.raise_for_status()


def upload_file_to_hdfs(local_path: Path, hdfs_path: str) -> str:
    parent = str(Path(hdfs_path).parent).replace("\\", "/")
    webhdfs_mkdirs(parent)

    create_response = requests.put(
        _webhdfs_path(hdfs_path),
        params={"op": "CREATE", "overwrite": "true"},
        allow_redirects=False,
        timeout=REQUEST_TIMEOUT_SEC,
    )
    if create_response.status_code not in {201, 307}:
        create_response.raise_for_status()

    upload_url = create_response.headers.get("Location")
    if not upload_url:
        return f"hdfs://namenode:9000/{hdfs_path.strip('/')}"

    with local_path.open("rb") as handle:
        response = requests.put(upload_url, data=handle, timeout=DOWNLOAD_TIMEOUT_SEC)
    response.raise_for_status()
    return f"hdfs://namenode:9000/{hdfs_path.strip('/')}"


def download_product_to_file(download_url: str, token: str, local_path: Path, expected_size: int | None) -> None:
    if MAX_DOWNLOAD_BYTES > 0 and expected_size and expected_size > MAX_DOWNLOAD_BYTES:
        raise RuntimeError(
            f"Product is larger than S5P_MAX_DOWNLOAD_BYTES: {expected_size} > {MAX_DOWNLOAD_BYTES}"
        )

    with requests.get(
        download_url,
        headers={"Authorization": f"Bearer {token}"},
        stream=True,
        timeout=DOWNLOAD_TIMEOUT_SEC,
    ) as response:
        response.raise_for_status()
        with local_path.open("wb") as handle:
            for chunk in response.iter_content(chunk_size=DOWNLOAD_CHUNK_SIZE):
                if chunk:
                    handle.write(chunk)


def maybe_download_raw_product(product_key: str, item: dict, token: str, content_start: str | None) -> dict:
    product_id = item.get("Id", "")
    product_name = item.get("Name", "")
    content_length = _safe_int(item.get("ContentLength"))
    download_url = build_download_url(product_id) if product_id else ""

    result = {
        "file_name": product_name,
        "download_url": download_url,
        "content_length": content_length,
        "s3_path": item.get("S3Path"),
        "raw_file_path": None,
        "raw_downloaded": False,
        "raw_download_error": None,
    }

    if not DOWNLOAD_RAW:
        return result
    if not product_id or not product_name:
        result["raw_download_error"] = "missing_product_id_or_name"
        return result

    year, month, day = _parse_content_date_day(content_start)
    hdfs_path = (
        f"{RAW_HDFS_BASE_PATH}/product={product_key}/year={year:04d}/month={month:02d}/day={day:02d}/"
        f"{product_name}"
    )
    result["raw_file_path"] = f"hdfs://namenode:9000/{hdfs_path.strip('/')}"

    try:
        if webhdfs_exists(hdfs_path):
            result["raw_downloaded"] = True
            logger.info("Raw product already exists in HDFS: %s", result["raw_file_path"])
            return result

        with TemporaryDirectory(prefix="s5p_download_") as tmp_dir:
            local_path = Path(tmp_dir) / product_name
            logger.info("Downloading raw %s to temporary file (%s bytes)", product_name, content_length)
            download_product_to_file(download_url, token, local_path, content_length)
            result["raw_file_path"] = upload_file_to_hdfs(local_path, hdfs_path)
            result["raw_downloaded"] = True
            logger.info("Uploaded raw product to %s", result["raw_file_path"])
    except Exception as exc:
        result["raw_downloaded"] = False
        result["raw_download_error"] = str(exc)[:500]
        logger.error("Raw product download/upload failed for %s: %s", product_name, exc)

    return result


def search_products(
    product_key: str, token: str, start_utc: datetime, end_utc: datetime
) -> list[dict]:
    """
    Query Copernicus Data Space ODATA API for product granules in the given window.
    Returns list of product entries (metadata only, not downloaded).
    """
    p = PRODUCTS_DEF[product_key]

    # WKT polygon for bounding box (Copernicus requires this format)
    wkt = (
        "POLYGON(("
        f"{BBOX[0]} {BBOX[1]},"
        f"{BBOX[2]} {BBOX[1]},"
        f"{BBOX[2]} {BBOX[3]},"
        f"{BBOX[0]} {BBOX[3]},"
        f"{BBOX[0]} {BBOX[1]}"
        "))"
    )

    params = {
        "$filter": (
            f"Collection/Name eq '{p['collection']}' and "
            f"contains(Name,'{p['type_filter'].strip()}') and "
            f"OData.CSC.Intersects(area=geography'SRID=4326;{wkt}') and "
            f"ContentDate/Start ge {to_odata_datetime(start_utc)} and "
            f"ContentDate/Start le {to_odata_datetime(end_utc)}"
        ),
        "$orderby": "ContentDate/Start desc",
        "$top": MAX_RESULTS,
    }

    headers = {"Authorization": f"Bearer {token}"}

    try:
        resp = requests.get(
            ODATA_URL, params=params, headers=headers, timeout=REQUEST_TIMEOUT_SEC
        )
        resp.raise_for_status()
        return resp.json().get("value", [])
    except Exception as e:
        logger.error(f"Error querying ODATA for {product_key}: {e}")
        return []


def run_once(producer) -> int:
    """
    Execute one ingest cycle: resolve window, search for each product,
    and publish events to Kafka. Returns count of events sent.
    """
    window = resolve_window(WINDOW_CONFIG)
    ingest_time = utc_now().isoformat()

    logger.info(
        f"Window UTC: {window.start_utc.isoformat()} -> {window.end_utc.isoformat()} "
        f"(mode={WINDOW_CONFIG.mode})"
    )

    if not CDSE_USERNAME or not CDSE_PASSWORD:
        logger.error("Missing CDSE_USERNAME / CDSE_PASSWORD env vars")
        return 0

    try:
        token = get_access_token(CDSE_USERNAME, CDSE_PASSWORD)
    except Exception as e:
        logger.error(f"Failed to get access token: {e}")
        return 0

    sent = 0
    window_start = to_utc_iso(window.start_utc)
    window_end = to_utc_iso(window.end_utc)
    window_now = to_utc_iso(window.now_utc)

    for idx, product_key in enumerate(PRODUCTS, 1):
        if product_key not in PRODUCTS_DEF:
            logger.warning(f"Unknown product key: {product_key}, skip")
            continue

        logger.info(f"[{idx}/{len(PRODUCTS)}] Searching {product_key}...")
        items = search_products(product_key, token, window.start_utc, window.end_utc)

        if not items:
            logger.info(f"  No products found for {product_key}")
            continue

        # Take only the first (most recent) product
        item = items[0]
        product_name = item.get("Name", "unknown")
        product_id = item.get("Id", "unknown")
        content_start = (item.get("ContentDate") or {}).get("Start")
        content_end = (item.get("ContentDate") or {}).get("End")
        raw_payload = maybe_download_raw_product(product_key, item, token, content_start)

        event_id = f"s5p_{product_key}_{product_id}_{window_start}_{window_end}".replace(
            " ", ""
        )

        event = {
            "product": product_key,
            "collection": PRODUCTS_DEF[product_key]["collection"],
            "content_start": content_start,
            "content_end": content_end,
            "bbox": BBOX,
            "product_name": product_name,
            "product_id": product_id,
            "file_name": raw_payload["file_name"],
            "download_url": raw_payload["download_url"],
            "content_length": raw_payload["content_length"],
            "s3_path": raw_payload["s3_path"],
            "raw_file_path": raw_payload["raw_file_path"],
            "raw_downloaded": raw_payload["raw_downloaded"],
            "raw_download_error": raw_payload["raw_download_error"],
            "unit": PRODUCTS_DEF[product_key]["unit"],
            "ingest_time": ingest_time,
            "window_mode": WINDOW_CONFIG.mode,
            "window_start_utc": window_start,
            "window_end_utc": window_end,
            "window_now_utc": window_now,
            "event_id": event_id,
            "source": "cdse",
        }

        if send_event(
            producer=producer,
            topic=KAFKA_TOPIC,
            event=event,
            logger=logger,
            key_field="event_id",
            wait_for_ack=False,
        ):
            sent += 1
            logger.info(f"  ✓ Sent {product_key}: {product_name}")
        else:
            logger.warning(f"  ✗ Failed to send {product_key}")

        time.sleep(REQUEST_DELAY_SEC)

        # Refresh token every 2 products to avoid expiration
        if idx % 2 == 0:
            try:
                token = get_access_token(CDSE_USERNAME, CDSE_PASSWORD)
            except Exception as e:
                logger.warning(f"Failed to refresh token: {e}")

        if SEND_DELAY_MS > 0:
            time.sleep(SEND_DELAY_MS / 1000.0)

    # Write state for resumption on error
    write_window_state(
        WINDOW_CONFIG.state_file,
        source="sentinel5p",
        config=WINDOW_CONFIG,
        window=window,
        extra={"topic": KAFKA_TOPIC, "products": PRODUCTS, "sent": sent},
    )

    return sent


def main():
    """Main entry point: loop over window(s) and ingest."""
    logger.info("=" * 70)
    logger.info("Sentinel-5P Product Metadata Ingest")
    logger.info("=" * 70)
    logger.info(f"Products:       {','.join(PRODUCTS)}")
    logger.info(f"Window mode:    {WINDOW_CONFIG.mode}")
    logger.info(f"BBOX:           {BBOX}")
    logger.info(f"Kafka topic:    {KAFKA_TOPIC}")
    logger.info(f"State file:     {WINDOW_CONFIG.state_file}")
    logger.info(f"Download raw:   {DOWNLOAD_RAW}")
    logger.info(f"Raw HDFS path:  {RAW_HDFS_BASE_PATH}")

    if WINDOW_CONFIG.mode == "realtime":
        logger.info(f"Realtime poll:  {WINDOW_CONFIG.poll_seconds}s")
        logger.info(f"Continuous:     {WINDOW_CONFIG.continuous}")

    producer = create_shared_kafka_producer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        logger=logger,
        max_retries=10,
        retry_delay=5,
    )

    loop_forever = WINDOW_CONFIG.mode == "realtime" and WINDOW_CONFIG.continuous
    total_sent = 0

    while True:
        try:
            sent = run_once(producer)
            total_sent += sent
            logger.info(f"Sent {sent} events in this cycle (total={total_sent})")
        except Exception as e:
            logger.error(f"Error in run_once: {e}", exc_info=True)

        if not loop_forever:
            break

        logger.info(
            f"Sleeping {WINDOW_CONFIG.poll_seconds}s before next realtime pull..."
        )
        time.sleep(WINDOW_CONFIG.poll_seconds)

    producer.flush()
    producer.close()
    logger.info(f"Done. Total sent: {total_sent} events.")


if __name__ == "__main__":
    main()
