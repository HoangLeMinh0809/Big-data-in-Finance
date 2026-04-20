import logging
import os
import time
from datetime import datetime

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
logger = logging.getLogger("openaq_ingest")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "openaq-hourly")
OPENAQ_API_KEY = os.getenv("OPENAQ_API_KEY", "").strip()
OPENAQ_BASE_URL = os.getenv("OPENAQ_BASE_URL", "https://api.openaq.org/v3")
MAX_LOCATIONS = int(os.getenv("MAX_LOCATIONS", "100"))
REQUEST_DELAY_SEC = float(os.getenv("REQUEST_DELAY_SEC", "0.35"))
SEND_DELAY_MS = int(os.getenv("SEND_DELAY_MS", "0"))

WINDOW_CONFIG = build_default_window_config(
    mode=os.getenv("WINDOW_MODE", "batch"),
    batch_lookback_days=int(os.getenv("BATCH_LOOKBACK_DAYS", "7")),
    realtime_lookback_minutes=int(os.getenv("REALTIME_LOOKBACK_MINUTES", "10")),
    poll_seconds=int(os.getenv("REALTIME_POLL_SECONDS", "600")),
    continuous=parse_bool(os.getenv("REALTIME_CONTINUOUS", "false"), default=False),
    start_override=os.getenv("WINDOW_START_UTC", ""),
    end_override=os.getenv("WINDOW_END_UTC", ""),
    state_file=os.getenv("WINDOW_STATE_FILE", "/tmp/openaq_window_state.json"),
)

TARGET_PARAMETERS = {"pm25", "pm10", "no2", "o3", "co", "so2"}
PAGE_LIMIT = int(os.getenv("PAGE_LIMIT", "1000"))

HEADERS = {
    "Accept": "application/json",
}
if OPENAQ_API_KEY:
    HEADERS["X-API-Key"] = OPENAQ_API_KEY


def _safe_int(value):
    if value in (None, ""):
        return None
    try:
        return int(value)
    except Exception:
        return None


def _safe_float(value):
    if value in (None, ""):
        return None
    try:
        return float(value)
    except Exception:
        return None


def get_with_retry(url: str, params: dict, max_retries: int = 4) -> dict | None:
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=HEADERS, params=params, timeout=30)
            if response.status_code == 429:
                time.sleep(15 * (attempt + 1))
                continue
            if response.status_code == 404:
                return None
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException:
            time.sleep(5 * (attempt + 1))
    return None


def fetch_all_pages(url: str, base_params: dict) -> list[dict]:
    results = []
    page = 1
    while True:
        params = {**base_params, "page": page, "limit": PAGE_LIMIT}
        data = get_with_retry(url, params)
        if not data:
            break
        batch = data.get("results", [])
        if not batch:
            break
        results.extend(batch)
        found_raw = data.get("meta", {}).get("found", 0)
        try:
            found = int(found_raw)
        except (TypeError, ValueError):
            found = 0
        if found > 0 and len(results) >= found:
            break
        if found <= 0 and len(batch) < PAGE_LIMIT:
            break
        page += 1
        time.sleep(REQUEST_DELAY_SEC)
    return results


def get_vietnam_locations() -> list[dict]:
    locations = fetch_all_pages(f"{OPENAQ_BASE_URL}/locations", {"countries_id": 220})
    if not locations:
        locations = fetch_all_pages(f"{OPENAQ_BASE_URL}/locations", {"country": "VN"})
    return locations


def get_sensors(location_id: int) -> list[dict]:
    data = get_with_retry(f"{OPENAQ_BASE_URL}/locations/{location_id}/sensors", {})
    return data.get("results", []) if data else []


def get_hourly_data(sensor_id: int, start_utc: datetime, end_utc: datetime) -> list[dict]:
    return fetch_all_pages(
        f"{OPENAQ_BASE_URL}/sensors/{sensor_id}/hours",
        {
            "datetime_from": start_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "datetime_to": end_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "period_name": "hour",
        },
    )


def build_event(
    location: dict,
    sensor: dict,
    measurement: dict,
    ingest_time: str,
    window_start_utc: str,
    window_end_utc: str,
    window_now_utc: str,
) -> dict:
    location_id = _safe_int(location.get("id"))
    sensor_id = _safe_int(sensor.get("id"))
    parameter = str(sensor.get("parameter", {}).get("name") or "").strip().lower()
    display_parameter = parameter.upper().replace("PM25", "PM2.5")
    datetime_utc = str(measurement.get("period", {}).get("datetimeFrom", {}).get("utc") or "").strip()
    expected_count = _safe_int(measurement.get("coverage", {}).get("expectedCount")) or 1
    observed_count = _safe_int(measurement.get("coverage", {}).get("observedCount")) or 0

    event_id = f"openaq_{location_id}_{sensor_id}_{parameter}_{datetime_utc}"

    return {
        "location_id": location_id,
        "location_name": str(location.get("name") or ""),
        "city": str(location.get("locality") or location.get("city") or ""),
        "latitude": _safe_float((location.get("coordinates") or {}).get("latitude")),
        "longitude": _safe_float((location.get("coordinates") or {}).get("longitude")),
        "provider": str((location.get("provider") or {}).get("name") or ""),
        "sensor_id": sensor_id,
        "parameter": display_parameter,
        "unit": str(sensor.get("parameter", {}).get("units") or ""),
        "datetime_utc": datetime_utc,
        "datetime_local": str(measurement.get("period", {}).get("datetimeFrom", {}).get("local") or ""),
        "value": _safe_float(measurement.get("value")),
        "min": _safe_float(measurement.get("summary", {}).get("min")),
        "max": _safe_float(measurement.get("summary", {}).get("max")),
        "sd": _safe_float(measurement.get("summary", {}).get("sd")),
        "expected_count": expected_count,
        "observed_count": observed_count,
        "coverage_pct": round((observed_count / max(expected_count, 1)) * 100.0, 1),
        "source": "openaq_api",
        "ingest_time": ingest_time,
        "window_mode": WINDOW_CONFIG.mode,
        "window_start_utc": window_start_utc,
        "window_end_utc": window_end_utc,
        "window_now_utc": window_now_utc,
        "event_id": event_id,
    }


def iter_events(
    ingest_time: str,
    start_utc: datetime,
    end_utc: datetime,
    now_utc: datetime,
):
    locations = get_vietnam_locations()
    if not locations:
        raise RuntimeError("No OpenAQ Vietnam locations found")

    if MAX_LOCATIONS > 0:
        locations = locations[:MAX_LOCATIONS]

    logger.info(f"OpenAQ API mode: {len(locations)} locations")

    window_start_text = to_utc_iso(start_utc)
    window_end_text = to_utc_iso(end_utc)
    window_now_text = to_utc_iso(now_utc)

    for location in locations:
        location_id = location.get("id")
        if not location_id:
            continue

        sensors = get_sensors(location_id)
        time.sleep(REQUEST_DELAY_SEC)

        for sensor in sensors:
            parameter_name = str(sensor.get("parameter", {}).get("name") or "").strip().lower()
            if parameter_name not in TARGET_PARAMETERS:
                continue

            measurements = get_hourly_data(sensor.get("id"), start_utc, end_utc)
            time.sleep(REQUEST_DELAY_SEC)

            for measurement in measurements:
                yield build_event(
                    location,
                    sensor,
                    measurement,
                    ingest_time,
                    window_start_text,
                    window_end_text,
                    window_now_text,
                )


def run_once(producer) -> int:
    window = resolve_window(WINDOW_CONFIG)
    ingest_time = utc_now().isoformat()

    write_window_state(
        WINDOW_CONFIG.state_file,
        source="openaq",
        config=WINDOW_CONFIG,
        window=window,
        extra={"topic": KAFKA_TOPIC, "max_locations": MAX_LOCATIONS},
    )

    logger.info(
        "Window UTC: "
        f"{window.start_utc.isoformat()} -> {window.end_utc.isoformat()} "
        f"(mode={WINDOW_CONFIG.mode})"
    )

    sent = 0
    for event in iter_events(
        ingest_time=ingest_time,
        start_utc=window.start_utc,
        end_utc=window.end_utc,
        now_utc=window.now_utc,
    ):
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
    logger.info("OpenAQ ingest")
    logger.info(f"  Topic:      {KAFKA_TOPIC}")
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
