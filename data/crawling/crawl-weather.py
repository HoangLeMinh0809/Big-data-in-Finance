import os
import json
import time
import logging
from datetime import datetime, timedelta

import requests
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

API_KEY = os.getenv("WEATHER_API_KEY")
DATA_DIR = os.getenv("DATA_DIR", "../weather")

WEATHER_API_URL = "http://api.weatherapi.com/v1/history.json"

LOCATIONS= [
    {"province": "Ha Noi", "lat": 21.0285, "lon": 105.8542},
    {"province": "Hai Phong", "lat": 20.8449, "lon": 106.6881},
    {"province": "Quang Ninh", "lat": 21.0064, "lon": 107.2925},
    {"province": "Lao Cai", "lat": 22.4809, "lon": 103.9755},
    {"province": "Thanh Hoa", "lat": 19.8067, "lon": 105.7852},
    {"province": "Nghe An", "lat": 18.6796, "lon": 105.6813},
    {"province": "Ha Tinh", "lat": 18.3550, "lon": 105.8877},
    {"province": "Hue", "lat": 16.4637, "lon": 107.5909},
    {"province": "Da Nang", "lat": 16.0544, "lon": 108.2022},
    {"province": "Quang Nam", "lat": 15.5394, "lon": 108.0191},
    {"province": "Quang Ngai", "lat": 15.1214, "lon": 108.8044},
    {"province": "Binh Dinh", "lat": 13.7563, "lon": 109.2297},
    {"province": "Phu Yen", "lat": 13.0882, "lon": 109.0929},
    {"province": "Khanh Hoa", "lat": 12.2388, "lon": 109.1967},
    {"province": "Gia Lai", "lat": 13.9833, "lon": 108.0000},
    {"province": "Dak Lak", "lat": 12.6667, "lon": 108.0500},
    {"province": "Lam Dong", "lat": 11.5753, "lon": 108.1429},
    {"province": "Ho Chi Minh City", "lat": 10.8231, "lon": 106.6297},
    {"province": "Binh Duong", "lat": 11.3254, "lon": 106.4770},
    {"province": "Dong Nai", "lat": 10.9500, "lon": 106.8167},
    {"province": "Can Tho", "lat": 10.0452, "lon": 105.7469},
    {"province": "An Giang", "lat": 10.5216, "lon": 105.1259},
    {"province": "Kien Giang", "lat": 10.0125, "lon": 105.0809},
    {"province": "Ca Mau", "lat": 9.1769, "lon": 105.1524},
]


def daterange(start, end):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def call_api(lat, lon, date):
    params = {
        "key": API_KEY,
        "q": f"{lat},{lon}",
        "dt": date
    }
    response = requests.get(WEATHER_API_URL, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def save_json(data, province, date):
    folder = os.path.join(DATA_DIR, province.replace(" ", "_"))
    os.makedirs(folder, exist_ok=True)

    file_path = os.path.join(folder, f"{date}.json")

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    return file_path


def crawl(start_date, end_date):
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    for loc in LOCATIONS:
        province = loc["province"]
        lat = loc["lat"]
        lon = loc["lon"]

        logging.info(f"Start province={province}")

        for d in daterange(start, end):
            date_str = d.strftime("%Y-%m-%d")

            try:
                data = call_api(lat, lon, date_str)
                path = save_json(data, province, date_str)

                logging.info(f"Saved {province} {date_str} → {path}")

                time.sleep(0.5)

            except Exception as e:
                logging.error(f"Error {province} {date_str}: {e}")
                continue


if __name__ == "__main__":
    crawl("2025-01-01", "2025-01-10")