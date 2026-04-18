import logging
import os
import re
import time
from datetime import datetime, timezone

import requests

from kafka_utils import create_kafka_producer, send_event
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
logger = logging.getLogger("maiac_ingest")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "maiac-summary")

CMR_GRANULES_URL = os.getenv("CMR_GRANULES_URL", "https://cmr.earthdata.nasa.gov/search/granules.json")
MAIAC_SHORT_NAME = os.getenv("MAIAC_SHORT_NAME", "MCD19A2")
MAIAC_VERSION = os.getenv("MAIAC_VERSION", "061")
MAIAC_BBOX_RAW = os.getenv("MAIAC_BBOX", "102,8,110,24")
MAIAC_PROVIDER = os.getenv("MAIAC_PROVIDER", "").strip()
PAGE_SIZE = int(os.getenv("PAGE_SIZE", "500"))
MAX_PAGES = int(os.getenv("MAX_PAGES", "50"))
MAX_GRANULES = int(os.getenv("MAX_GRANULES", "0"))
REQUEST_TIMEOUT_SEC = int(os.getenv("REQUEST_TIMEOUT_SEC", "30"))
REQUEST_DELAY_SEC = float(os.getenv("REQUEST_DELAY_SEC", "0.2"))
SEND_DELAY_MS = int(os.getenv("SEND_DELAY_MS", "0"))

WINDOW_CONFIG = build_default_window_config(
    mode=os.getenv("WINDOW_MODE", "batch"),
    batch_lookback_days=int(os.getenv("BATCH_LOOKBACK_DAYS", "30")),
    realtime_lookback_minutes=int(os.getenv("REALTIME_LOOKBACK_MINUTES", "10")),
    poll_seconds=int(os.getenv("REALTIME_POLL_SECONDS", "600")),
    continuous=parse_bool(os.getenv("REALTIME_CONTINUOUS", "false"), default=False),
    start_override=os.getenv("WINDOW_START_UTC", ""),
    end_override=os.getenv("WINDOW_END_UTC", ""),
    state_file=os.getenv("WINDOW_STATE_FILE", "/tmp/maiac_window_state.json"),
)


def parse_bbox(raw_bbox: str) -> list[float]:
    parts = [part.strip() for part in raw_bbox.split(",") if part.strip()]
    if len(parts) != 4:
        raise ValueError("MAIAC_BBOX must have 4 comma-separated values: min_lon,min_lat,max_lon,max_lat")
    return [float(value) for value in parts]


MAIAC_BBOX = parse_bbox(MAIAC_BBOX_RAW)


def _to_cmr_iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _extract_tile(granule_name: str) -> str:
    match = re.search(r"\.(h\d{2}v\d{2})\.", granule_name)
    return match.group(1) if match else ""


def _extract_acquisition_date(granule_name: str) -> str:
    match = re.search(r"\.A(\d{7})\.", granule_name)
    if not match:
        return ""

    token = match.group(1)
    year = int(token[:4])
    day_of_year = int(token[4:])
    dt = datetime.strptime(f"{year}{day_of_year:03d}", "%Y%j")
    return dt.date().isoformat()


def _pick_download_url(entry: dict) -> str:
    links = entry.get("links") or []
    for link in links:
        href = str(link.get("href") or "").strip()
        if not href:
            continue
        if link.get("inherited") is True:
            continue
        return href
    return ""


def get_with_retry(url: str, params: dict, max_retries: int = 4) -> dict | None:
    for attempt in range(max_retries):
        try:
            response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT_SEC)
            if response.status_code == 429:
                wait_sec = 5 * (attempt + 1)
                logger.warning(f"CMR rate limited. Retry after {wait_sec}s")
                time.sleep(wait_sec)
                continue
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as exc:
            wait_sec = 3 * (attempt + 1)
            logger.warning(f"CMR request failed (attempt {attempt + 1}/{max_retries}): {exc}")
            time.sleep(wait_sec)
    return None


def iter_cmr_entries(start_utc: datetime, end_utc: datetime):
    temporal = f"{_to_cmr_iso(start_utc)},{_to_cmr_iso(end_utc)}"
    emitted = 0

    for page_num in range(1, MAX_PAGES + 1):
        params = {
            "short_name": MAIAC_SHORT_NAME,
            "version": MAIAC_VERSION,
            "temporal": temporal,
            "bounding_box": MAIAC_BBOX_RAW,
            "page_size": PAGE_SIZE,
            "page_num": page_num,
        }
        if MAIAC_PROVIDER:
            params["provider"] = MAIAC_PROVIDER

        payload = get_with_retry(CMR_GRANULES_URL, params)
        if payload is None:
            logger.warning(f"Skip page {page_num} because CMR response is empty")
            continue

        entries = (payload.get("feed") or {}).get("entry") or []
        logger.info(f"CMR page {page_num}: {len(entries)} granules")

        if not entries:
            break

        for entry in entries:
            yield entry
            emitted += 1
            if MAX_GRANULES > 0 and emitted >= MAX_GRANULES:
                return

        if len(entries) < PAGE_SIZE:
            break

        if REQUEST_DELAY_SEC > 0:
            time.sleep(REQUEST_DELAY_SEC)


def build_event(
    entry: dict,
    ingest_time: str,
    window_start_utc: str,
    window_end_utc: str,
    window_now_utc: str,
) -> dict:
    granule_id = str(entry.get("id") or "")
    granule_name = str(entry.get("title") or entry.get("producer_granule_id") or granule_id)

    event_id = f"maiac_{granule_id}" if granule_id else f"maiac_{granule_name}"

    return {
        "event_id": event_id,
        "granule_id": granule_id,
        "granule_name": granule_name,
        "producer_granule_id": str(entry.get("producer_granule_id") or ""),
        "short_name": MAIAC_SHORT_NAME,
        "version": MAIAC_VERSION,
        "tile": _extract_tile(granule_name),
        "acquisition_date": _extract_acquisition_date(granule_name),
        "time_start": str(entry.get("time_start") or ""),
        "time_end": str(entry.get("time_end") or ""),
        "updated": str(entry.get("updated") or ""),
        "download_url": _pick_download_url(entry),
        "bbox": MAIAC_BBOX,
        "source": "nasa_cmr_maiac",
        "ingest_time": ingest_time,
        "window_mode": WINDOW_CONFIG.mode,
        "window_start_utc": window_start_utc,
        "window_end_utc": window_end_utc,
        "window_now_utc": window_now_utc,
    }


def run_once(producer) -> int:
    window = resolve_window(WINDOW_CONFIG)
    ingest_time = utc_now().isoformat()

    write_window_state(
        WINDOW_CONFIG.state_file,
        source="maiac",
        config=WINDOW_CONFIG,
        window=window,
        extra={
            "topic": KAFKA_TOPIC,
            "short_name": MAIAC_SHORT_NAME,
            "version": MAIAC_VERSION,
            "bbox": MAIAC_BBOX,
        },
    )

    logger.info(
        "Window UTC: "
        f"{window.start_utc.isoformat()} -> {window.end_utc.isoformat()} "
        f"(mode={WINDOW_CONFIG.mode})"
    )

    window_start = to_utc_iso(window.start_utc)
    window_end = to_utc_iso(window.end_utc)
    window_now = to_utc_iso(window.now_utc)

    sent = 0
    for entry in iter_cmr_entries(window.start_utc, window.end_utc):
        event = build_event(
            entry,
            ingest_time=ingest_time,
            window_start_utc=window_start,
            window_end_utc=window_end,
            window_now_utc=window_now,
        )

        if send_event(
            producer=producer,
            topic=KAFKA_TOPIC,
            event=event,
            logger=logger,
            key_field="event_id",
        ):
            sent += 1

        if SEND_DELAY_MS > 0:
            time.sleep(SEND_DELAY_MS / 1000.0)

    return sent


def main():
    logger.info("MODIS MAIAC ingest")
    logger.info(f"  Topic: {KAFKA_TOPIC}")
    logger.info(f"  Product: {MAIAC_SHORT_NAME}.{MAIAC_VERSION}")
    logger.info(f"  BBox: {MAIAC_BBOX}")
    logger.info(f"  Window mode: {WINDOW_CONFIG.mode}")
    logger.info(f"  Batch lookback days: {WINDOW_CONFIG.batch_lookback_days}")
    logger.info(f"  Realtime lookback minutes: {WINDOW_CONFIG.realtime_lookback_minutes}")
    logger.info(
        "  Realtime continuous: "
        f"{WINDOW_CONFIG.continuous if WINDOW_CONFIG.mode == 'realtime' else False}"
    )
    logger.info(f"  Window state file: {WINDOW_CONFIG.state_file}")

    producer = create_kafka_producer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        logger=logger,
    )

    loop_forever = WINDOW_CONFIG.mode == "realtime" and WINDOW_CONFIG.continuous
    total_sent = 0

    while True:
        sent = run_once(producer)
        total_sent += sent
        logger.info(f"Run done. Sent {sent} messages (total={total_sent}).")

        if not loop_forever:
            break

        logger.info(f"Sleep {WINDOW_CONFIG.poll_seconds}s before next realtime pull...")
        time.sleep(WINDOW_CONFIG.poll_seconds)

    producer.flush()
    producer.close()
    logger.info(f"Done. Sent {total_sent} messages.")


if __name__ == "__main__":
    main()
