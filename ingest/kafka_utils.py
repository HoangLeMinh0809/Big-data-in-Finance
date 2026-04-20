import json
import time

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable


def create_kafka_producer(
    bootstrap_servers: str,
    logger,
    max_retries: int = 10,
    retry_delay: int = 5,
) -> KafkaProducer:
    """Create a Kafka producer with simple retry logic while broker is starting."""
    for attempt in range(1, max_retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                retries=3,
                max_block_ms=30000,
            )
            logger.info(f"Kafka connected (attempt {attempt})")
            return producer
        except NoBrokersAvailable:
            logger.warning(f"Kafka not ready (attempt {attempt}/{max_retries}), wait {retry_delay}s")
            time.sleep(retry_delay)

    raise RuntimeError("Cannot connect to Kafka")


def send_event(
    producer: KafkaProducer,
    topic: str,
    event: dict,
    logger,
    key_field: str = "event_id",
    wait_for_ack: bool = False,
    ack_timeout_sec: int = 10,
) -> bool:
    """Send one event and return True when the producer accepted it."""
    try:
        key = event.get(key_field) if key_field else None
        future = producer.send(topic, key=key, value=event)
        if wait_for_ack:
            future.get(timeout=ack_timeout_sec)
        return True
    except Exception as exc:
        event_id = event.get(key_field) if key_field else None
        logger.error(f"Failed to send message: {exc} | event_id={event_id}")
        return False


def send_events(
    producer: KafkaProducer,
    topic: str,
    events: list[dict],
    logger,
    key_field: str = "event_id",
    send_delay_ms: int = 0,
    wait_for_ack: bool = False,
    ack_timeout_sec: int = 10,
) -> int:
    """Send a list of events and return the number of successful sends."""
    success_count = 0

    for event in events:
        ok = send_event(
            producer=producer,
            topic=topic,
            event=event,
            logger=logger,
            key_field=key_field,
            wait_for_ack=wait_for_ack,
            ack_timeout_sec=ack_timeout_sec,
        )
        if ok:
            success_count += 1

        if send_delay_ms > 0:
            time.sleep(send_delay_ms / 1000.0)

    return success_count