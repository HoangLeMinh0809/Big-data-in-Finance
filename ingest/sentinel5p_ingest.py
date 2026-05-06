"""
Sentinel-5P Product Metadata Ingest
====================================
Fetches Sentinel-5P product metadata from Copernicus Data Space using their ODATA API
and publishes summary events to Kafka without downloading full files.

Products supported: NO2, CO, O3, SO2, CH4, AER
Window modes: batch (historical) or realtime (continuous/polling)
"""

import logging
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

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
LOCAL_METADATA_PATH = Path(
    os.getenv(
        "SENTINEL5P_LOCAL_METADATA_PATH",
        "data/crawling/outputs/sentinel5p_vietnam_last_3d.json",
    )
)

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


def parse_utc_datetime(value: str | None) -> datetime | None:
    if not value:
        return None

    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


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


def load_local_catalogue() -> list[dict]:
    if not LOCAL_METADATA_PATH.exists():
        return []

    try:
        with LOCAL_METADATA_PATH.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except Exception as exc:
        logger.warning(f"Failed to load local Sentinel-5P metadata cache: {exc}")
        return []

    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        value = payload.get("value")
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
    return []


def select_local_product(product_key: str, start_utc: datetime, end_utc: datetime) -> dict | None:
    type_filter = PRODUCTS_DEF[product_key]["type_filter"]
    records = [
        record
        for record in load_local_catalogue()
        if record.get("product_type") == type_filter
    ]

    if not records:
        return None

    in_window = []
    for record in records:
        record_start = parse_utc_datetime(record.get("start_time_utc"))
        if record_start and start_utc <= record_start <= end_utc:
            in_window.append(record)

    candidates = in_window or records
    candidates.sort(
        key=lambda record: parse_utc_datetime(record.get("start_time_utc"))
        or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )
    return candidates[0]


def build_event_payload(
    *,
    product_key: str,
    item: dict,
    source: str,
    ingest_time: str,
    window_start: str,
    window_end: str,
    window_now: str,
) -> dict:
    product_meta = PRODUCTS_DEF[product_key]
    content_start = (item.get("ContentDate") or {}).get("Start") or item.get("start_time_utc")
    content_end = (item.get("ContentDate") or {}).get("End") or item.get("end_time_utc")
    product_name = item.get("Name") or item.get("name") or "unknown"
    product_id = item.get("Id") or item.get("id") or product_name

    event_id = f"s5p_{product_key}_{product_id}_{window_start}_{window_end}".replace(
        " ", ""
    )

    return {
        "product": product_key,
        "collection": product_meta["collection"],
        "content_start": content_start,
        "content_end": content_end,
        "bbox": BBOX,
        "product_name": product_name,
        "product_id": product_id,
        "unit": product_meta["unit"],
        "ingest_time": ingest_time,
        "window_mode": WINDOW_CONFIG.mode,
        "window_start_utc": window_start,
        "window_end_utc": window_end,
        "window_now_utc": window_now,
        "event_id": event_id,
        "source": source,
    }


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

    token: str | None = None
    if CDSE_USERNAME and CDSE_PASSWORD:
        try:
            token = get_access_token(CDSE_USERNAME, CDSE_PASSWORD)
        except Exception as e:
            logger.warning(f"Failed to get access token, using local fallback if available: {e}")
    else:
        logger.warning("Missing CDSE_USERNAME / CDSE_PASSWORD env vars, using local fallback if available")

    sent = 0
    window_start = to_utc_iso(window.start_utc)
    window_end = to_utc_iso(window.end_utc)
    window_now = to_utc_iso(window.now_utc)

    for idx, product_key in enumerate(PRODUCTS, 1):
        if product_key not in PRODUCTS_DEF:
            logger.warning(f"Unknown product key: {product_key}, skip")
            continue

        logger.info(f"[{idx}/{len(PRODUCTS)}] Searching {product_key}...")
        items = search_products(product_key, token, window.start_utc, window.end_utc) if token else []
        source = "cdse"

        if not items:
            local_item = select_local_product(product_key, window.start_utc, window.end_utc)
            if local_item is not None:
                items = [local_item]
                source = "local-cache"
                logger.info(
                    f"  Using local Sentinel-5P metadata cache: {LOCAL_METADATA_PATH}"
                )

        if not items:
            logger.info(f"  No products found for {product_key}")
            continue

        # Take only the first (most recent) product
        item = items[0]
        event = build_event_payload(
            product_key=product_key,
            item=item,
            source=source,
            ingest_time=ingest_time,
            window_start=window_start,
            window_end=window_end,
            window_now=window_now,
        )

        if send_event(
            producer=producer,
            topic=KAFKA_TOPIC,
            event=event,
            logger=logger,
            key_field="event_id",
            wait_for_ack=False,
        ):
            sent += 1
            logger.info(f"  ✓ Sent {product_key}: {event['product_name']} ({source})")
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
