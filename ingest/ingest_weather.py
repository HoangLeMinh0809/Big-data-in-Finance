import os
import json
import glob
import time
import logging
from datetime import datetime
from pathlib import Path

import requests

try:
    from dotenv import load_dotenv
except ModuleNotFoundError:
    # Allow running without python-dotenv in minimal images.
    def load_dotenv(*_args, **_kwargs):
        return False

from kafka_utils import create_kafka_producer as create_shared_kafka_producer, send_events
from window_utils import (
    build_default_window_config,
    day_strings_from_window,
    parse_bool,
    resolve_window,
    to_utc_iso,
    utc_now,
    write_window_state,
)

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
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "weather_history")
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

WINDOW_CONFIG = build_default_window_config(
    mode=os.getenv("WINDOW_MODE", "batch"),
    batch_lookback_days=int(os.getenv("BATCH_LOOKBACK_DAYS", "7")),
    realtime_lookback_minutes=int(os.getenv("REALTIME_LOOKBACK_MINUTES", "10")),
    poll_seconds=int(os.getenv("REALTIME_POLL_SECONDS", "600")),
    continuous=parse_bool(os.getenv("REALTIME_CONTINUOUS", "false"), default=False),
    start_override=os.getenv("WINDOW_START_UTC", ""),
    end_override=os.getenv("WINDOW_END_UTC", ""),
    state_file=os.getenv("WINDOW_STATE_FILE", "/tmp/weather_window_state.json"),
)

DEFAULT_WEATHER_QUERIES = [
    "Ha Noi",
    "Hai Phong",
    "Quang Ninh",
    "Lao Cai",
    "Thanh Hoa",
    "Nghe An",
    "Ha Tinh",
    "Hue",
    "Da Nang",
    "Quang Nam",
    "Quang Ngai",
    "Binh Dinh",
    "Phu Yen",
    "Khanh Hoa",
    "Gia Lai",
    "Dak Lak",
    "Lam Dong",
    "Ho Chi Minh City",
    "Binh Duong",
    "Dong Nai",
    "Can Tho",
    "An Giang",
    "Kien Giang",
    "Ca Mau",
]


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
    parsed = [item.strip() for item in raw.split(",") if item.strip()]
    return parsed if parsed else DEFAULT_WEATHER_QUERIES


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


def resolve_weather_dates(window) -> list[str]:
    # Backward-compatible override for legacy env names.
    if WEATHER_START_DATE and WEATHER_END_DATE:
        return build_date_range(WEATHER_START_DATE, WEATHER_END_DATE)
    return day_strings_from_window(window)


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


def load_api_payloads(window) -> list[tuple[str, str, dict, str]]:
    if not WEATHER_API_KEY:
        raise ValueError("Thiếu WEATHER_API_KEY cho SOURCE_MODE=api")

    queries = parse_query_list(WEATHER_QUERY_LIST)

    dates = resolve_weather_dates(window)
    targets = [(query, date_str) for query in queries for date_str in dates]

    if MAX_FILES > 0:
        targets = targets[:MAX_FILES]

    logger.info(
        f"API mode: {len(queries)} locations, {len(dates)} days, {len(targets)} requests"
    )

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


def load_local_payloads(window) -> list[tuple[str, str, dict, str]]:
    json_files = sorted(glob.glob(os.path.join(DATA_DIR, "*", "*.json")))
    if not json_files:
        logger.error(f"Không tìm thấy file .json nào trong {DATA_DIR}")
        return []

    logger.info(f"Tìm thấy {len(json_files)} file json")

    if MAX_FILES > 0:
        json_files = json_files[:MAX_FILES]
        logger.info(f"Giới hạn xử lý {MAX_FILES} file đầu tiên")

    start_date = window.start_utc.date().isoformat()
    end_date = window.end_utc.date().isoformat()

    payloads = []
    for filepath in json_files:
        filename = os.path.basename(filepath)
        province = extract_province_from_path(filepath)
        query_date = extract_query_date_from_filename(filepath)

        if query_date < start_date or query_date > end_date:
            continue

        logger.info(f"--- Đang xử lý file local: {filename} (province={province}, date={query_date}) ---")

        try:
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
            payloads.append((province, query_date, data, filepath))
        except Exception as e:
            logger.error(f"  Lỗi đọc file {filename}: {e}")

    logger.info(f"Local mode after date filter: {len(payloads)} payloads")
    return payloads


def resolve_source_mode() -> str:
    if SOURCE_MODE in {"local", "api"}:
        return SOURCE_MODE

    # Auto mode: ưu tiên API nếu có key, ngược lại dùng local file.
    if WEATHER_API_KEY:
        return "api"
    return "local"


# =============================================================================
# Normalizer chính cho weather history
# =============================================================================
def normalize_weather_history(
    data: dict,
    province: str,
    source_file: str,
    ingest_time: str,
    window_start_utc: str,
    window_end_utc: str,
    window_now_utc: str,
) -> list[dict]:
    """
    Normalize 1 file JSON WeatherAPI history thành list event.
    Mỗi giờ = 1 Kafka message.
    """
    events = []
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
                    "window_mode": WINDOW_CONFIG.mode,
                    "window_start_utc": window_start_utc,
                    "window_end_utc": window_end_utc,
                    "window_now_utc": window_now_utc,
                }
                events.append(event)

            except Exception as e:
                logger.error(f"Lỗi xử lý hour record: {e} | time={hour.get('time')}")
                continue

    return events


def send_events_to_kafka(producer, topic: str, events: list[dict]) -> int:
    """
    Gửi list events vào Kafka topic. Trả về số message gửi thành công.
    """
    return send_events(
        producer=producer,
        topic=topic,
        events=events,
        logger=logger,
        key_field="event_id",
        send_delay_ms=SEND_DELAY_MS,
        wait_for_ack=True,
    )


def run_once(producer, mode: str) -> tuple[int, int]:
    window = resolve_window(WINDOW_CONFIG)
    ingest_time = utc_now().isoformat()

    write_window_state(
        WINDOW_CONFIG.state_file,
        source="weather",
        config=WINDOW_CONFIG,
        window=window,
        extra={"topic": KAFKA_TOPIC, "source_mode": mode},
    )

    logger.info(
        "Window UTC: "
        f"{window.start_utc.isoformat()} -> {window.end_utc.isoformat()} "
        f"(mode={WINDOW_CONFIG.mode})"
    )

    payloads = load_api_payloads(window) if mode == "api" else load_local_payloads(window)
    if not payloads:
        logger.warning("Không có dữ liệu đầu vào cho window hiện tại")
        return 0, 0

    window_start = to_utc_iso(window.start_utc)
    window_end = to_utc_iso(window.end_utc)
    window_now = to_utc_iso(window.now_utc)

    total_sent = 0
    total_payloads = 0

    for province, query_date, data, source_ref in payloads:
        logger.info(f"--- Normalize: province={province}, date={query_date} ---")

        try:
            events = normalize_weather_history(
                data=data,
                province=province,
                source_file=source_ref,
                ingest_time=ingest_time,
                window_start_utc=window_start,
                window_end_utc=window_end,
                window_now_utc=window_now,
            )
            logger.info(f"  Normalize xong: {len(events)} events")

            sent = send_events_to_kafka(producer, KAFKA_TOPIC, events)
            total_sent += sent
            total_payloads += 1
            logger.info(f"  Gửi thành công: {sent}/{len(events)} messages")

        except Exception as e:
            logger.error(f"  Lỗi xử lý bản ghi province={province}, date={query_date}: {e}")
            continue

    return total_payloads, total_sent


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
    logger.info(f"Window mode: {WINDOW_CONFIG.mode}")
    logger.info(f"Batch lookback days: {WINDOW_CONFIG.batch_lookback_days}")
    logger.info(f"Realtime lookback minutes: {WINDOW_CONFIG.realtime_lookback_minutes}")
    logger.info(
        "Realtime continuous: "
        f"{WINDOW_CONFIG.continuous if WINDOW_CONFIG.mode == 'realtime' else False}"
    )
    logger.info(f"Window state file: {WINDOW_CONFIG.state_file}")
    mode = resolve_source_mode()
    logger.info(f"Source mode: {mode}")

    producer = create_shared_kafka_producer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        logger=logger,
    )

    loop_forever = WINDOW_CONFIG.mode == "realtime" and WINDOW_CONFIG.continuous
    total_sent = 0
    total_files = 0

    while True:
        files, sent = run_once(producer, mode)
        total_files += files
        total_sent += sent
        logger.info(
            f"Run done. payloads={files}, sent={sent} messages "
            f"(total_payloads={total_files}, total_sent={total_sent})"
        )

        if not loop_forever:
            break

        logger.info(f"Sleep {WINDOW_CONFIG.poll_seconds}s before next realtime pull...")
        time.sleep(WINDOW_CONFIG.poll_seconds)

    producer.flush()
    producer.close()

    logger.info("=" * 60)
    logger.info(f"HOÀN TẤT: {total_files} files, {total_sent} messages")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()