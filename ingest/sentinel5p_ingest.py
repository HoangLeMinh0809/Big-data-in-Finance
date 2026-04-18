
import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import requests
import netCDF4 as nc
from kafka import KafkaProducer
from kafka.errors import NoBrokerAvailable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("sentinel5p_ingest")

AUTH_URL = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
ODATA_URL = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"
DOWNLOAD_URL = "https://download.dataspace.copernicus.eu/odata/v1/Products"

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

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sentinel5p-summary")

CDSE_USERNAME = os.getenv("CDSE_USERNAME", "")
CDSE_PASSWORD = os.getenv("CDSE_PASSWORD", "")

DOWNLOAD_DIR = Path(os.getenv("DOWNLOAD_DIR", "/data/sentinel5p_data"))
DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)

bbox_raw = os.getenv("BBOX", "100,8,110,24")
BBOX = [float(x.strip()) for x in bbox_raw.split(",")]

DATE_END = os.getenv("DATE_END") or datetime.now(timezone.utc).strftime("%Y-%m-%d")
DATE_START = os.getenv("DATE_START") or (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d")

MAX_RESULTS = int(os.getenv("MAX_RESULTS", "1"))
PRODUCTS = [p.strip() for p in os.getenv("PRODUCTS", "NO2,CO,O3,SO2,CH4,AER").split(",") if p.strip()]


def get_access_token(username: str, password: str) -> str:
    resp = requests.post(
        AUTH_URL,
        data={
            "client_id": "cdse-public",
            "username": username,
            "password": password,
            "grant_type": "password",
        },
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def search_products(product_key: str, token: str) -> list[dict]:
    p = PRODUCTS_DEF[product_key]

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
            f"ContentDate/Start ge {DATE_START}T00:00:00.000Z and "
            f"ContentDate/Start le {DATE_END}T23:59:59.000Z"
        ),
        "$orderby": "ContentDate/Start desc",
        "$top": MAX_RESULTS,
    }

    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(ODATA_URL, params=params, headers=headers, timeout=60)
    resp.raise_for_status()
    return resp.json().get("value", [])


def download_product(product: dict, token: str) -> Path:
    filename = product["Name"] + ".nc"
    dest = DOWNLOAD_DIR / filename
    if dest.exists():
        return dest

    url = f"{DOWNLOAD_URL}({product['Id']})/$value"
    headers = {"Authorization": f"Bearer {token}"}

    with requests.get(url, headers=headers, stream=True, timeout=60) as r:
        r.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=1 << 20):
                if chunk:
                    f.write(chunk)

    return dest


def compute_stats(nc_path: Path, product_key: str) -> dict:
    p = PRODUCTS_DEF[product_key]
    ds = nc.Dataset(nc_path)

    group = ds.groups.get(p["group"])
    if group is None:
        # Fallback: search all groups
        for g in ds.groups.values():
            if p["variable"] in g.variables:
                group = g
                break

    if group is None or p["variable"] not in group.variables:
        ds.close()
        raise KeyError(f"Variable '{p['variable']}' not found in {nc_path.name}")

    var = group.variables[p["variable"]][0].data.astype(float)

    # mask fill values
    fill = getattr(group.variables[p["variable"]], "_FillValue", None)
    if fill is not None:
        var[var == fill] = np.nan
    var[var < -1e30] = np.nan

    if "qa_value" in group.variables:
        qa = group.variables["qa_value"][0].data
        var[qa < 0.5] = np.nan

    valid = np.isfinite(var)
    valid_pct = float(valid.sum() / var.size * 100.0) if var.size else 0.0

    stats = {
        "min": float(np.nanmin(var)) if valid.any() else None,
        "max": float(np.nanmax(var)) if valid.any() else None,
        "mean": float(np.nanmean(var)) if valid.any() else None,
        "valid_pct": valid_pct,
    }

    ds.close()
    return stats


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


def main():
    if not CDSE_USERNAME or not CDSE_PASSWORD:
        raise RuntimeError("Missing CDSE_USERNAME / CDSE_PASSWORD env vars")

    logger.info("Sentinel-5P ingest (summary)")
    logger.info(f"  Products:    {','.join(PRODUCTS)}")
    logger.info(f"  Date range:  {DATE_START} -> {DATE_END}")
    logger.info(f"  BBOX:        {BBOX}")
    logger.info(f"  Download dir:{DOWNLOAD_DIR}")
    logger.info(f"  Kafka topic: {KAFKA_TOPIC}")

    token = get_access_token(CDSE_USERNAME, CDSE_PASSWORD)
    producer = create_kafka_producer()
    ingest_time = datetime.now(timezone.utc).isoformat()

    sent = 0
    for idx, product_key in enumerate(PRODUCTS, 1):
        if product_key not in PRODUCTS_DEF:
            logger.warning(f"Unknown product key: {product_key}, skip")
            continue

        items = search_products(product_key, token)
        if not items:
            logger.warning(f"No product found for {product_key}")
            continue

        # take newest
        item = items[0]
        nc_path = download_product(item, token)
        stats = compute_stats(nc_path, product_key)

        event_id = f"s5p_{product_key}_{item.get('Id')}_{DATE_START}_{DATE_END}".replace(" ", "")
        event = {
            "product": product_key,
            "collection": PRODUCTS_DEF[product_key]["collection"],
            "content_start": (item.get("ContentDate") or {}).get("Start"),
            "content_end": (item.get("ContentDate") or {}).get("End"),
            "bbox": BBOX,
            "file_name": nc_path.name,
            "stats": stats,
            "unit": PRODUCTS_DEF[product_key]["unit"],
            "ingest_time": ingest_time,
            "event_id": event_id,
            "source": "cdse",
        }

        producer.send(KAFKA_TOPIC, key=event_id, value=event)
        sent += 1
        logger.info(f"[{idx}/{len(PRODUCTS)}] sent {product_key}: {stats}")

        # refresh token every 2 products
        if idx % 2 == 0:
            token = get_access_token(CDSE_USERNAME, CDSE_PASSWORD)

    producer.flush()
    logger.info(f"Done. Sent {sent} messages.")


if __name__ == "__main__":
    main()
