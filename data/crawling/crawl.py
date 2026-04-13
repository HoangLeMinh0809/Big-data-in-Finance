import os
import time
import requests
import pandas as pd
from datetime import datetime, timezone, timedelta
from tqdm import tqdm

API_KEY     = os.getenv("OPENAQ_API_KEY", "eda537e3a24d2d2b1951fd126d8816e91bc74e74343d694af24e19265151dc15")
BASE_URL    = "https://api.openaq.org/v3"
OUTPUT_FILE = "openaq_vietnam_hourly.csv"

HOURS_BACK = int(os.getenv("HOURS_BACK", "24"))
DATE_TO   = datetime.now(timezone.utc)
DATE_FROM = DATE_TO - timedelta(hours=HOURS_BACK)
TARGET_PARAMETERS = {"pm25", "pm10", "no2", "o3", "co", "so2"}
# Đặt số nhỏ hơn (vd: 5) để chạy thử nhanh; None = tất cả trạm
MAX_LOCATIONS = 100
REQUEST_DELAY = 0.35
PAGE_LIMIT = 1000  
HEADERS = {
    "X-API-Key": API_KEY,
    "Accept":    "application/json",
}

def get_with_retry(url: str, params: dict, max_retries: int = 4) -> dict | None:
    """GET request với exponential-backoff retry."""
    for attempt in range(max_retries):
        try:
            r = requests.get(url, headers=HEADERS, params=params, timeout=30)
            if r.status_code == 429:
                wait = 15 * (attempt + 1)
                tqdm.write(f"  Rate limited — chờ {wait}s...")
                time.sleep(wait)
                continue
            if r.status_code == 404:
                return None
            r.raise_for_status()
            return r.json()
        except requests.exceptions.RequestException as e:
            time.sleep(5 * (attempt + 1))
    return None

def fetch_all_pages(url: str, base_params: dict) -> list:
    """Lặp qua tất cả các trang và trả về danh sách kết quả."""
    results, page = [], 1
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
        if found <= 0:
            if len(batch) < PAGE_LIMIT:
                break
        else:
            if len(results) >= found:
                break
        page += 1
        time.sleep(REQUEST_DELAY)
    return results

def get_vietnam_locations() -> list[dict]:
    """Bước 1: Lấy tất cả trạm đo tại Việt Nam."""
    locs = fetch_all_pages(f"{BASE_URL}/locations", {"countries_id": 220})
    if not locs:                                    # fallback
        locs = fetch_all_pages(f"{BASE_URL}/locations", {"country": "VN"})
    print(f" Tìm thấy {len(locs)} trạm\n")
    return locs

def get_sensors(location_id: int) -> list[dict]:
    """Bước 2: Lấy sensors (từng chỉ số) của một trạm."""
    data = get_with_retry(f"{BASE_URL}/locations/{location_id}/sensors", {})
    return data.get("results", []) if data else []


def get_hourly_data(sensor_id: int) -> list[dict]:
    """Bước 3: Lấy dữ liệu theo giờ trong N giờ gần nhất cho một sensor."""
    return fetch_all_pages(
        f"{BASE_URL}/sensors/{sensor_id}/hours",
        {
            "datetime_from": DATE_FROM.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "datetime_to":   DATE_TO.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "period_name":   "hour",
        },
    )

def main():
    print(f"  Tu : {DATE_FROM.strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"  Den: {DATE_TO.strftime('%Y-%m-%d %H:%M UTC')}")
    print("  Chi so: PM2.5, PM10, NO2, O3, CO, SO2")

    locations = get_vietnam_locations()
    if not locations:
        print("Khong tim thay tram nao. Kiem tra API key va ket noi mang.")
        return

    if MAX_LOCATIONS:
        locations = locations[:MAX_LOCATIONS]
        print(f"   ⚙️  Che do thu: chi lay {MAX_LOCATIONS} tram dau tien\n")

    print("Bước 2-3/3 — Cào sensors và dữ liệu theo giờ...\n")
    all_rows = []
    skipped_params = 0

    for loc in tqdm(locations, desc="Tram do", unit="tram"):
        loc_id   = loc["id"]
        loc_name = loc.get("name", f"location_{loc_id}")
        city     = loc.get("locality") or loc.get("city", "")
        coords   = loc.get("coordinates", {})
        provider = loc.get("provider", {}).get("name", "")

        sensors = get_sensors(loc_id)
        time.sleep(REQUEST_DELAY)

        for sensor in sensors:
            param      = sensor.get("parameter", {})
            param_name = param.get("name", "").lower()

            # ► Bỏ qua chỉ số không cần
            if param_name not in TARGET_PARAMETERS:
                skipped_params += 1
                continue

            sensor_id = sensor["id"]
            unit      = param.get("units", "")

            measurements = get_hourly_data(sensor_id)
            time.sleep(REQUEST_DELAY)

            for m in measurements:
                period   = m.get("period", {})
                dt_from  = period.get("datetimeFrom", {})
                summary  = m.get("summary", {})
                coverage = m.get("coverage", {})
                exp      = coverage.get("expectedCount") or 1
                obs      = coverage.get("observedCount") or 0

                # Hiển thị tên chỉ số đẹp hơn
                display_param = param_name.upper().replace("PM25", "PM2.5")

                all_rows.append({
                    # Vị trí
                    "location_id":    loc_id,
                    "location_name":  loc_name,
                    "city":           city,
                    "latitude":       coords.get("latitude"),
                    "longitude":      coords.get("longitude"),
                    "provider":       provider,
                    # Chỉ số
                    "sensor_id":      sensor_id,
                    "parameter":      display_param,
                    "unit":           unit,
                    # Thời gian
                    "datetime_utc":   dt_from.get("utc", ""),
                    "datetime_local": dt_from.get("local", ""),
                    # Giá trị đo
                    "value":          m.get("value"),
                    "min":            summary.get("min"),
                    "max":            summary.get("max"),
                    "sd":             summary.get("sd"),
                    # Độ phủ dữ liệu
                    "expected_count": coverage.get("expectedCount"),
                    "observed_count": obs,
                    "coverage_pct":   round(obs / exp * 100, 1),
                })

    # ── Lưu file ─────────────────────────────────────────────
    if not all_rows:
        print("\n Khong co du lieu. Hay kiem tra API key va ket noi.")
        return

    df = pd.DataFrame(all_rows)
    df["datetime_utc"]   = pd.to_datetime(df["datetime_utc"],   utc=True, errors="coerce")
    df["datetime_local"] = pd.to_datetime(df["datetime_local"],            errors="coerce")
    df.sort_values(["city", "location_name", "parameter", "datetime_utc"], inplace=True)
    df.reset_index(drop=True, inplace=True)
    df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8-sig")

    print(f"  HOÀN TẤT — {len(df):,} bản ghi → {OUTPUT_FILE}")
    print(f"  Số trạm có dữ liệu : {df['location_id'].nunique()}")
    print(f"  Số tỉnh/thành phố  : {df['city'].nunique()}")
    print(f"  Khoảng thời gian   :")
    print(f"    {df['datetime_utc'].min()} → {df['datetime_utc'].max()}")

    print(f"\n  Bản ghi theo chỉ số:")
    for param, count in df.groupby("parameter").size().sort_values(ascending=False).items():
        print(f"    · {param:<8}  {count:>8,} bản ghi")

    print(f"\n  Bản ghi theo tỉnh/thành (top 10):")
    for city, count in df.groupby("city").size().sort_values(ascending=False).head(10).items():
        label = city if city else "(chưa có tên)"
        print(f"    · {label:<30} {count:>6,}")

    if skipped_params:
        print(f"\n   Bỏ qua {skipped_params} sensor không thuộc 6 chỉ số đã chọn")

    print(f"\n  Cột trong file CSV ({len(df.columns)} cột):")
    for col in df.columns:
        print(f"    · {col}")
    print()
if __name__ == "__main__":
    main()