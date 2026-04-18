import os
import json
import glob
import time
import logging
from datetime import datetime, timezone
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from dotenv import load_dotenv
import requests

load_dotenv()

# =============================================================================
# Cấu hình logging
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("weather_ingest")

# =============================================================================
# Đọc biến môi trường
# =============================================================================
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "weather-history")
DATA_DIR = os.getenv("DATA_DIR", "./data/weather")
MAX_FILES = int(os.getenv("MAX_FILES", "0"))          # 0 = tất cả
SEND_DELAY_MS = int(os.getenv("SEND_DELAY_MS", "10"))

# SOURCE_MODE: auto | local | api
SOURCE_MODE = os.getenv("SOURCE_MODE", "auto").strip().lower()

# Weather API config
WEATHER_API_BASE_URL = os.getenv("WEATHER_API_BASE_URL", "https://api.weatherapi.com/v1")
WEATHER_API_KEY = os.getenv("WEATHER_API_KEY", "").strip()
WEATHER_QUERY_LIST = os.getenv("WEATHER_QUERY_LIST", "")
WEATHER_START_DATE = os.getenv("WEATHER_START_DATE", "")
WEATHER_END_DATE = os.getenv("WEATHER_END_DATE", "")
WEATHER_TIMEOUT_SEC = int(os.getenv("WEATHER_TIMEOUT_SEC", "30"))
WEATHER_API_DELAY_MS = int(os.getenv("WEATHER_API_DELAY_MS", "200"))


# =============================================================================
# Utility functions
# =============================================================================
def extract_province_from_path(filepath: str) -> str:
    """
    Lấy tên tỉnh/thành từ folder cha.
    Ví dụ:
      ./data/weather/Ha_Noi/2025-01-02.json -> Ha Noi
    """
    parent = Path(filepath).parent.name
    return parent.replace("_", " ")


def extract_query_date_from_filename(filepath: str) -> str:
    """
    Lấy ngày từ tên file.
    Ví dụ:
      2025-01-02.json -> 2025-01-02
    """
    return Path(filepath).stem


def parse_query_list(raw: str) -> list[str]:
    return [item.strip() for item in raw.split(",") if item.strip()]


def build_date_range(start_date: str, end_date: str) -> list[str]:
    start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
    end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
    if end_dt < start_dt:
        raise ValueError("WEATHER_END_DATE phải lớn hơn hoặc bằng WEATHER_START_DATE")

    days = (end_dt - start_dt).days + 1
    return [
        start_dt.fromordinal(start_dt.toordinal() + offset).isoformat()
        for offset in range(days)
    ]


def safe_get(d: dict, *keys, default=None):
    cur = d
    for key in keys:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(key)
        if cur is None:
            return default
    return cur


def fetch_weather_history(query: str, date_str: str) -> dict:
    endpoint = f"{WEATHER_API_BASE_URL.rstrip('/')}/history.json"
    response = requests.get(
        endpoint,
        params={
            "key": WEATHER_API_KEY,
            "q": query,
            "dt": date_str,
        },
        timeout=WEATHER_TIMEOUT_SEC,
    )
    response.raise_for_status()
    return response.json()


def load_api_payloads() -> list[tuple[str, str, dict, str]]:
    if not WEATHER_API_KEY:
        raise ValueError("Thiếu WEATHER_API_KEY cho SOURCE_MODE=api")

    queries = parse_query_list(WEATHER_QUERY_LIST)
    if not queries:
        raise ValueError("Thiếu WEATHER_QUERY_LIST cho SOURCE_MODE=api")
    if not WEATHER_START_DATE or not WEATHER_END_DATE:
        raise ValueError("Thiếu WEATHER_START_DATE hoặc WEATHER_END_DATE cho SOURCE_MODE=api")

    dates = build_date_range(WEATHER_START_DATE, WEATHER_END_DATE)
    targets = [(query, date_str) for query in queries for date_str in dates]

    if MAX_FILES > 0:
        targets = targets[:MAX_FILES]

    logger.info(f"API mode: {len(queries)} locations, {len(dates)} days, {len(targets)} requests")

    payloads = []
    for query, date_str in targets:
        try:
            logger.info(f"--- Gọi API: query={query}, date={date_str} ---")
            data = fetch_weather_history(query, date_str)
            source_ref = f"api://weatherapi/history?q={query}&dt={date_str}"
            payloads.append((query, date_str, data, source_ref))

            if WEATHER_API_DELAY_MS > 0:
                time.sleep(WEATHER_API_DELAY_MS / 1000.0)
        except Exception as e:
            logger.error(f"  Lỗi gọi API query={query}, date={date_str}: {e}")

    return payloads


def load_local_payloads() -> list[tuple[str, str, dict, str]]:
    json_files = sorted(glob.glob(os.path.join(DATA_DIR, "*", "*.json")))
    if not json_files:
        logger.error(f"Không tìm thấy file .json nào trong {DATA_DIR}")
        return []

    logger.info(f"Tìm thấy {len(json_files)} file json")

    if MAX_FILES > 0:
        json_files = json_files[:MAX_FILES]
        logger.info(f"Giới hạn xử lý {MAX_FILES} file đầu tiên")

    payloads = []
    for filepath in json_files:
        filename = os.path.basename(filepath)
        province = extract_province_from_path(filepath)
        query_date = extract_query_date_from_filename(filepath)

        logger.info(f"--- Đang xử lý file local: {filename} (province={province}, date={query_date}) ---")

        try:
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
            payloads.append((province, query_date, data, filepath))
        except Exception as e:
            logger.error(f"  Lỗi đọc file {filename}: {e}")

    return payloads


def resolve_source_mode() -> str:
    if SOURCE_MODE in {"local", "api"}:
        return SOURCE_MODE

    # auto mode: ưu tiên API nếu có đủ cấu hình, ngược lại dùng local file.
    if WEATHER_API_KEY and WEATHER_QUERY_LIST and WEATHER_START_DATE and WEATHER_END_DATE:
        return "api"
    return "local"


# =============================================================================
# Normalizer chính cho weather history
# =============================================================================
def normalize_weather_history(data: dict, province: str, source_file: str) -> list[dict]:
    """
    Normalize 1 file JSON WeatherAPI history thành list event.
    Mỗi giờ = 1 Kafka message.
    """
    events = []
    ingest_time = datetime.now(timezone.utc).isoformat()

    location = data.get("location", {})
    forecastday_list = safe_get(data, "forecast", "forecastday", default=[])

    if not forecastday_list:
        logger.warning(f"Không có forecastday trong file: {source_file}")
        return events

    for forecastday in forecastday_list:
        query_date = forecastday.get("date")
        hours = forecastday.get("hour", [])

        if not hours:
            logger.warning(f"Không có hour data trong file: {source_file}")
            continue

        for hour in hours:
            try:
                event_time = hour.get("time")
                time_epoch = hour.get("time_epoch")

                event = {
                    "event_id": f"{province}_{event_time}",
                    "province": province,
                    "country": location.get("country"),
                    "region": location.get("region"),
                    "location_name": location.get("name"),
                    "lat": location.get("lat"),
                    "lon": location.get("lon"),
                    "tz_id": location.get("tz_id"),

                    "query_date": query_date,
                    "time": event_time,
                    "time_epoch": time_epoch,
                    "is_day": hour.get("is_day"),

                    "temp_c": hour.get("temp_c"),
                    "temp_f": hour.get("temp_f"),
                    "feelslike_c": hour.get("feelslike_c"),
                    "feelslike_f": hour.get("feelslike_f"),
                    "windchill_c": hour.get("windchill_c"),
                    "windchill_f": hour.get("windchill_f"),
                    "heatindex_c": hour.get("heatindex_c"),
                    "heatindex_f": hour.get("heatindex_f"),
                    "dewpoint_c": hour.get("dewpoint_c"),
                    "dewpoint_f": hour.get("dewpoint_f"),

                    "condition_text": safe_get(hour, "condition", "text"),
                    "condition_code": safe_get(hour, "condition", "code"),
                    "condition_icon": safe_get(hour, "condition", "icon"),

                    "wind_mph": hour.get("wind_mph"),
                    "wind_kph": hour.get("wind_kph"),
                    "wind_degree": hour.get("wind_degree"),
                    "wind_dir": hour.get("wind_dir"),
                    "gust_mph": hour.get("gust_mph"),
                    "gust_kph": hour.get("gust_kph"),

                    "pressure_mb": hour.get("pressure_mb"),
                    "pressure_in": hour.get("pressure_in"),

                    "precip_mm": hour.get("precip_mm"),
                    "precip_in": hour.get("precip_in"),
                    "snow_cm": hour.get("snow_cm"),

                    "humidity": hour.get("humidity"),
                    "cloud": hour.get("cloud"),
                    "vis_km": hour.get("vis_km"),
                    "vis_miles": hour.get("vis_miles"),
                    "uv": hour.get("uv"),

                    "will_it_rain": hour.get("will_it_rain"),
                    "chance_of_rain": hour.get("chance_of_rain"),
                    "will_it_snow": hour.get("will_it_snow"),
                    "chance_of_snow": hour.get("chance_of_snow"),

                    "source": "weatherapi_history_json",
                    "source_file": source_file,
                    "ingest_time": ingest_time,
                }
                events.append(event)

            except Exception as e:
                logger.error(f"Lỗi xử lý hour record: {e} | time={hour.get('time')}")
                continue

    return events


# =============================================================================
# Kafka Producer — với retry logic
# =============================================================================
def create_kafka_producer(max_retries: int = 10, retry_delay: int = 5) -> KafkaProducer:
    """
    Tạo Kafka producer với retry khi broker chưa sẵn sàng.
    """
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
            logger.info(f"Kết nối Kafka thành công (attempt {attempt})")
            return producer
        except NoBrokersAvailable:
            logger.warning(
                f"Kafka chưa sẵn sàng (attempt {attempt}/{max_retries}), "
                f"đợi {retry_delay}s..."
            )
            time.sleep(retry_delay)

    raise RuntimeError(f"Không thể kết nối Kafka sau {max_retries} lần thử")


def send_events_to_kafka(producer: KafkaProducer, topic: str, events: list[dict]) -> int:
    """
    Gửi list events vào Kafka topic. Trả về số message gửi thành công.
    """
    success_count = 0

    for event in events:
        try:
            key = event.get("event_id", "")
            future = producer.send(topic, key=key, value=event)
            future.get(timeout=10)
            success_count += 1

            if SEND_DELAY_MS > 0:
                time.sleep(SEND_DELAY_MS / 1000.0)

        except Exception as e:
            logger.error(f"Lỗi gửi message: {e} | event_id={event.get('event_id')}")

    return success_count


# =============================================================================
# Main — Luồng chạy chính
# =============================================================================
def main():
    logger.info("=" * 60)
    logger.info("WEATHER HISTORY — INGEST SERVICE")
    logger.info("=" * 60)
    logger.info(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"Topic: {KAFKA_TOPIC}")
    logger.info(f"Max files: {MAX_FILES if MAX_FILES > 0 else 'ALL'}")
    mode = resolve_source_mode()
    logger.info(f"Source mode: {mode}")

    payloads = load_api_payloads() if mode == "api" else load_local_payloads()
    if not payloads:
        logger.error("Không có dữ liệu đầu vào để ingest")
        return

    producer = create_kafka_producer()

    total_sent = 0
    total_files = 0

    for province, query_date, data, source_ref in payloads:
        logger.info(f"--- Normalize: province={province}, date={query_date} ---")

        try:
            events = normalize_weather_history(
                data=data,
                province=province,
                source_file=source_ref,
            )
            logger.info(f"  Normalize xong: {len(events)} events")

            sent = send_events_to_kafka(producer, KAFKA_TOPIC, events)
            total_sent += sent
            total_files += 1
            logger.info(f"  Gửi thành công: {sent}/{len(events)} messages")

        except Exception as e:
            logger.error(f"  Lỗi xử lý bản ghi province={province}, date={query_date}: {e}")
            continue

    producer.flush()
    producer.close()

    logger.info("=" * 60)
    logger.info(f"HOÀN TẤT: {total_files} files, {total_sent} messages")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()