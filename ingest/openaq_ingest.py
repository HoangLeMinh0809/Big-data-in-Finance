import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from kafka import KafkaProducer
from kafka.errors import NoBrokerAvailable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("openaq_ingest")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "openaq-hourly")
INPUT_FILE = os.getenv("INPUT_FILE", "/data/crawling/openaq_vietnam_hourly.csv")
SEND_DELAY_MS = int(os.getenv("SEND_DELAY_MS", "0"))


def _safe_int(v):
    if pd.isna(v) or v == "":
        return None
    try:
        return int(v)
    except Exception:
        return None


def _safe_float(v):
    if pd.isna(v) or v == "":
        return None
    try:
        return float(v)
    except Exception:
        return None


def create_kafka_producer(max_retries: int = 10, retry_delay: int = 5) -> KafkaProducer:
    for attempt in range(1, max_retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=3,
                max_block_ms=30000,
            )
            logger.info(f"Kafka connected (attempt {attempt})")
            return producer
        except NoBrokerAvailable:
            logger.warning(f"Kafka not ready (attempt {attempt}/{max_retries}), wait {retry_delay}s")
            time.sleep(retry_delay)

    raise RuntimeError("Cannot connect to Kafka")


def build_event(row: pd.Series, ingest_time: str) -> dict:
    # event_id ổn định để có thể dedupe downstream nếu cần
    # Dùng sensor_id + parameter + datetime_utc + location_id
    location_id = _safe_int(row.get("location_id"))
    sensor_id = _safe_int(row.get("sensor_id"))
    parameter = str(row.get("parameter") or "").strip()
    datetime_utc = str(row.get("datetime_utc") or "").strip()

    event_id = f"openaq_{location_id}_{sensor_id}_{parameter}_{datetime_utc}"

    return {
        "location_id": location_id,
        "location_name": str(row.get("location_name") or ""),
        "city": str(row.get("city") or ""),
        "latitude": _safe_float(row.get("latitude")),
        "longitude": _safe_float(row.get("longitude")),
        "provider": str(row.get("provider") or ""),
        "sensor_id": sensor_id,
        "parameter": parameter,
        "unit": str(row.get("unit") or ""),
        "datetime_utc": datetime_utc,
        "datetime_local": str(row.get("datetime_local") or ""),
        "value": _safe_float(row.get("value")),
        "min": _safe_float(row.get("min")),
        "max": _safe_float(row.get("max")),
        "sd": _safe_float(row.get("sd")),
        "expected_count": _safe_int(row.get("expected_count")),
        "observed_count": _safe_int(row.get("observed_count")),
        "coverage_pct": _safe_float(row.get("coverage_pct")),
        "source": "openaq",
        "ingest_time": ingest_time,
        "event_id": event_id,
    }


def main():
    input_path = Path(INPUT_FILE)
    if not input_path.exists():
        raise FileNotFoundError(f"INPUT_FILE not found: {INPUT_FILE}")

    logger.info("OpenAQ ingest")
    logger.info(f"  INPUT_FILE: {INPUT_FILE}")
    logger.info(f"  Topic:      {KAFKA_TOPIC}")

    df = pd.read_csv(input_path)
    if df.empty:
        logger.warning("Input file is empty. Nothing to send.")
        return

    ingest_time = datetime.now(timezone.utc).isoformat()
    producer = create_kafka_producer()

    sent = 0
    for _, row in df.iterrows():
        event = build_event(row, ingest_time)
        key = event.get("event_id")
        producer.send(KAFKA_TOPIC, key=key, value=event)
        sent += 1
        if SEND_DELAY_MS > 0:
            time.sleep(SEND_DELAY_MS / 1000.0)

    producer.flush()
    logger.info(f"Done. Sent {sent} messages.")


if __name__ == "__main__":
    main()
