import os
import sys
import json
import time
import requests
import numpy as np
import netCDF4 as nc
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from matplotlib.cm import ScalarMappable
from datetime import datetime, timedelta
from pathlib import Path
from tqdm import tqdm
import warnings
warnings.filterwarnings("ignore")

try:
    import cartopy.crs as ccrs
    import cartopy.feature as cfeature
    HAS_CARTOPY = True
except ImportError:
    HAS_CARTOPY = False

CONFIG = {
    "username": "doraemonlink1@gmail.com",
    "password": "",
    "download_dir": "./sentinel5p_data",
    "bbox": [100.0, 8.0, 110.0, 24.0],
    "date_start": (datetime.utcnow() - timedelta(days=7)).strftime("%Y-%m-%d"),
    "date_end":   datetime.utcnow().strftime("%Y-%m-%d"),
    "max_results": 1,
}

# PRODUCT DEFINITIONS
PRODUCTS = {
    "NO2": {
        "collection": "SENTINEL-5P",
        "type_filter": "L2__NO2___",
        "variable": "nitrogendioxide_tropospheric_column",
        "group": "PRODUCT",
        "label": "NO₂ Tropospheric Column",
        "unit": "mol/m²",
        "cmap": "YlOrRd",
        "vmin": 0,
        "vmax": 0.0002,
    },
    "CO": {
        "collection": "SENTINEL-5P",
        "type_filter": "L2__CO____",
        "variable": "carbonmonoxide_total_column",
        "group": "PRODUCT",
        "label": "CO Total Column",
        "unit": "mol/m²",
        "cmap": "hot_r",
        "vmin": 0,
        "vmax": 0.05,
    },
    "O3": {
        "collection": "SENTINEL-5P",
        "type_filter": "L2__O3____",
        "variable": "ozone_total_vertical_column",
        "group": "PRODUCT",
        "label": "O₃ Total Column",
        "unit": "mol/m²",
        "cmap": "PuBu",
        "vmin": 0.1,
        "vmax": 0.15,
    },
    "SO2": {
        "collection": "SENTINEL-5P",
        "type_filter": "L2__SO2___",
        "variable": "sulfurdioxide_total_vertical_column",
        "group": "PRODUCT",
        "label": "SO₂ Total Column",
        "unit": "mol/m²",
        "cmap": "plasma",
        "vmin": -0.001,
        "vmax": 0.005,
    },
    "CH4": {
        "collection": "SENTINEL-5P",
        "type_filter": "L2__CH4___",
        "variable": "methane_mixing_ratio_bias_corrected",
        "group": "PRODUCT",
        "label": "CH₄ Mixing Ratio",
        "unit": "ppb",
        "cmap": "RdYlGn_r",
        "vmin": 1800,
        "vmax": 1900,
    },
    "AER": {
        "collection": "SENTINEL-5P",
        "type_filter": "L2__AER_AI",
        "variable": "aerosol_index_354_388",
        "group": "PRODUCT",
        "label": "Aerosol Index (354/388 nm)",
        "unit": "unitless",
        "cmap": "afmhot_r",
        "vmin": -1,
        "vmax": 3,
    },
}

# AUTHENTICATION
AUTH_URL = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
ODATA_URL = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"
DOWNLOAD_URL = "https://download.dataspace.copernicus.eu/odata/v1/Products"


def get_access_token(username: str, password: str) -> str:
    """Fetch a short-lived access token from CDSE."""
    resp = requests.post(AUTH_URL, data={
        "client_id": "cdse-public",
        "username": username,
        "password": password,
        "grant_type": "password",
    })
    resp.raise_for_status()
    return resp.json()["access_token"]


# SEARCH
def search_products(product_key: str, cfg: dict, token: str) -> list:
    """Search CDSE OData for S5P products matching bbox and date range."""
    p = PRODUCTS[product_key]
    bbox = cfg["bbox"]  # [lon_min, lat_min, lon_max, lat_max]
    wkt = (
        f"POLYGON(("
        f"{bbox[0]} {bbox[1]},"
        f"{bbox[2]} {bbox[1]},"
        f"{bbox[2]} {bbox[3]},"
        f"{bbox[0]} {bbox[3]},"
        f"{bbox[0]} {bbox[1]}"
        f"))"
    )

    params = {
        "$filter": (
            f"Collection/Name eq '{p['collection']}' and "
            f"contains(Name,'{p['type_filter'].strip()}') and "
            f"OData.CSC.Intersects(area=geography'SRID=4326;{wkt}') and "
            f"ContentDate/Start ge {cfg['date_start']}T00:00:00.000Z and "
            f"ContentDate/Start le {cfg['date_end']}T23:59:59.000Z"
        ),
        "$orderby": "ContentDate/Start desc",
        "$top": cfg["max_results"],
    }

    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(ODATA_URL, params=params, headers=headers)
    resp.raise_for_status()
    items = resp.json().get("value", [])
    print(f"  [{product_key}] Found {len(items)} product(s).")
    return items


# DOWNLOAD
def download_product(product: dict, cfg: dict, token: str) -> Path:
    """Download a single S5P product file, skip if already on disk."""
    out_dir = Path(cfg["download_dir"])
    out_dir.mkdir(parents=True, exist_ok=True)
    filename = product["Name"] + ".nc"
    dest = out_dir / filename

    if dest.exists():
        print(f"  Skipping (already downloaded): {filename}")
        return dest

    url = f"{DOWNLOAD_URL}({product['Id']})/$value"
    headers = {"Authorization": f"Bearer {token}"}

    with requests.get(url, headers=headers, stream=True) as r:
        r.raise_for_status()
        total = int(r.headers.get("content-length", 0))
        with open(dest, "wb") as f, tqdm(
            total=total, unit="B", unit_scale=True, desc=filename[:50]
        ) as bar:
            for chunk in r.iter_content(chunk_size=1 << 20):
                f.write(chunk)
                bar.update(len(chunk))

    return dest


# READ NetCDF
def read_variable(filepath: Path, product_key: str):
    """Extract lat, lon, and the target variable from an S5P NetCDF file."""
    p = PRODUCTS[product_key]
    ds = nc.Dataset(filepath)

    group = ds.groups.get(p["group"])
    if group is None:
        # Fallback: search all groups
        for g in ds.groups.values():
            if p["variable"] in g.variables:
                group = g
                break

    if group is None or p["variable"] not in group.variables:
        ds.close()
        raise KeyError(
            f"Variable '{p['variable']}' not found in {filepath.name}. "
            f"Available: {list(ds.groups.keys())}"
        )

    lat = group.variables["latitude"][0].data
    lon = group.variables["longitude"][0].data
    data = group.variables[p["variable"]][0].data.astype(float)

    # Apply fill/QA masking
    fill = group.variables[p["variable"]]._FillValue if hasattr(
        group.variables[p["variable"]], "_FillValue") else -9999.0
    data[data == fill] = np.nan
    data[data < -1e30] = np.nan

    # QA filtering (if available)
    if "qa_value" in group.variables:
        qa = group.variables["qa_value"][0].data
        data[qa < 0.5] = np.nan

    ds.close()
    return lat, lon, data


# VISUALIZE
def plot_product(lat, lon, data, product_key: str, filename: str, cfg: dict):
    """Render a map for a single S5P product."""
    p = PRODUCTS[product_key]
    bbox = cfg["bbox"]

    fig = plt.figure(figsize=(12, 7))

    if HAS_CARTOPY:
        ax = fig.add_subplot(1, 1, 1, projection=ccrs.PlateCarree())
        ax.set_extent(bbox, crs=ccrs.PlateCarree())
        ax.add_feature(cfeature.COASTLINE, linewidth=0.8)
        ax.add_feature(cfeature.BORDERS, linewidth=0.5, linestyle=":")
        ax.add_feature(cfeature.LAND, facecolor="#f5f5f0", zorder=0)
        ax.add_feature(cfeature.OCEAN, facecolor="#d0e8f5", zorder=0)
        ax.gridlines(draw_labels=True, linewidth=0.4, color="gray", alpha=0.6)
        scatter = ax.scatter(
            lon.ravel(), lat.ravel(),
            c=data.ravel(),
            cmap=p["cmap"],
            vmin=p["vmin"], vmax=p["vmax"],
            s=0.3, alpha=0.85,
            transform=ccrs.PlateCarree(),
        )
    else:
        ax = fig.add_subplot(1, 1, 1)
        ax.set_xlim(bbox[0], bbox[2])
        ax.set_ylim(bbox[1], bbox[3])
        ax.set_xlabel("Longitude")
        ax.set_ylabel("Latitude")
        scatter = ax.scatter(
            lon.ravel(), lat.ravel(),
            c=data.ravel(),
            cmap=p["cmap"],
            vmin=p["vmin"], vmax=p["vmax"],
            s=0.3, alpha=0.85,
        )
        ax.grid(True, linewidth=0.4, alpha=0.5)

    cbar = plt.colorbar(scatter, ax=ax, pad=0.02, shrink=0.85)
    cbar.set_label(f"{p['label']} ({p['unit']})", fontsize=11)

    ax.set_title(
        f"Sentinel-5P  |  {p['label']}\n{filename[:60]}",
        fontsize=12, fontweight="bold", pad=10,
    )

    out_dir = Path(cfg["download_dir"])
    out_path = out_dir / f"{product_key}_map.png"
    plt.tight_layout()
    plt.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved map → {out_path}")
    return out_path


# ALL-PRODUCTS SUMMARY GRID
def plot_summary_grid(results: dict, cfg: dict):
    """Plot all retrieved products in a single figure grid."""
    valid = {k: v for k, v in results.items() if v is not None}
    if not valid:
        print("No data to plot in summary grid.")
        return

    n = len(valid)
    cols = 3
    rows = (n + cols - 1) // cols
    fig = plt.figure(figsize=(7 * cols, 5.5 * rows))
    fig.suptitle("Sentinel-5P Atmospheric Products Summary", fontsize=16, fontweight="bold", y=1.01)

    for idx, (key, (lat, lon, data)) in enumerate(valid.items()):
        p = PRODUCTS[key]
        bbox = cfg["bbox"]

        if HAS_CARTOPY:
            ax = fig.add_subplot(rows, cols, idx + 1, projection=ccrs.PlateCarree())
            ax.set_extent(bbox, crs=ccrs.PlateCarree())
            ax.add_feature(cfeature.COASTLINE, linewidth=0.6)
            ax.add_feature(cfeature.BORDERS, linewidth=0.4, linestyle=":")
            ax.add_feature(cfeature.LAND, facecolor="#f5f5f0", zorder=0)
            ax.add_feature(cfeature.OCEAN, facecolor="#d0e8f5", zorder=0)
            sc = ax.scatter(
                lon.ravel(), lat.ravel(),
                c=data.ravel(),
                cmap=p["cmap"], vmin=p["vmin"], vmax=p["vmax"],
                s=0.2, alpha=0.85, transform=ccrs.PlateCarree(),
            )
        else:
            ax = fig.add_subplot(rows, cols, idx + 1)
            ax.set_xlim(bbox[0], bbox[2])
            ax.set_ylim(bbox[1], bbox[3])
            sc = ax.scatter(
                lon.ravel(), lat.ravel(),
                c=data.ravel(),
                cmap=p["cmap"], vmin=p["vmin"], vmax=p["vmax"],
                s=0.2, alpha=0.85,
            )

        plt.colorbar(sc, ax=ax, shrink=0.7, pad=0.02).set_label(p["unit"], fontsize=8)
        ax.set_title(p["label"], fontsize=10, fontweight="bold")

    plt.tight_layout()
    out_path = Path(cfg["download_dir"]) / "summary_grid.png"
    plt.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"\n  Summary grid saved → {out_path}")


# MAIN
def main(products_to_run=None, cfg=None):
    if cfg is None:
        cfg = CONFIG

    if cfg["username"] == "YOUR_CDSE_EMAIL":
        sys.exit(1)

    if products_to_run is None:
        products_to_run = list(PRODUCTS.keys())  # All products

    print("  Sentinel-5P Atmospheric Data Viewer")
    print(f"  Products  : {', '.join(products_to_run)}")
    print(f"  Date range: {cfg['date_start']} → {cfg['date_end']}")
    print(f"  Bbox      : {cfg['bbox']}")

    # Authenticate
    print("Authenticating with CDSE")
    token = get_access_token(cfg["username"], cfg["password"])
    print("  Access token obtained.\n")

    results = {}

    for key in products_to_run:
        print(f"  Processing: {key} — {PRODUCTS[key]['label']}")
        try:
            # Search
            products = search_products(key, cfg, token)
            if not products:
                print(f"  [SKIP] No products found for {key} in given date/bbox range.")
                results[key] = None
                continue

            # Download
            filepath = download_product(products[0], cfg, token)

            # Read
            lat, lon, data = read_variable(filepath, key)
            valid_pct = np.sum(~np.isnan(data)) / data.size * 100
            print(f"  Valid pixels: {valid_pct:.1f}%  |  "
                  f"Range: [{np.nanmin(data):.4g}, {np.nanmax(data):.4g}] {PRODUCTS[key]['unit']}")

            # Plot individual map
            plot_product(lat, lon, data, key, filepath.name, cfg)
            results[key] = (lat, lon, data)

        except Exception as e:
            print(f"  [ERROR] {key}: {e}")
            results[key] = None

        # Refresh token every 2 products (token valid ~10 min)
        if list(products_to_run).index(key) % 2 == 1:
            token = get_access_token(cfg["username"], cfg["password"])

    # Summary grid
    print("  Generating summary grid...")
    plot_summary_grid(results, cfg)
    print(f"\n  Done! All outputs saved to: {Path(cfg['download_dir']).resolve()}")

if __name__ == "__main__":
    # ── Customize here before running ──────────────────
    CONFIG["username"] = "YOUR_CDSE_EMAIL"
    CONFIG["password"] = "YOUR_CDSE_PASSWORD"
    # Optionally narrow the date range or bbox:
    # CONFIG["date_start"] = "2024-11-01"
    # CONFIG["date_end"]   = "2024-11-07"
    # CONFIG["bbox"] = [100.0, 8.0, 110.0, 24.0]  # [lon_min, lat_min, lon_max, lat_max]
    # Run all 6 products (or pass a subset, e.g. ["NO2", "CO"])
    main(products_to_run=["NO2", "CO", "O3", "SO2", "CH4", "AER"])