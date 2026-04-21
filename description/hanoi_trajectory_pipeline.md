# Hanoi Air Quality — Trajectory Tracking Pipeline
---

## Config chung

```python
HANOI_BBOX = {
    "lon_min": 105.3, "lon_max": 106.0,
    "lat_min": 20.5,  "lat_max": 21.4,
}

HANOI_CENTER = {"lat": 21.028, "lon": 105.854}

ERA5_REGION = {
    "north": 25, "west": 100, "south": 15, "east": 115  # bao toàn Southeast Asia
}
```

---

## 0) Setup Spark session
```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("hanoi-trajectory-pipeline") \
    .config("spark.driver.memory", "16g") \
    .config("spark.executor.memory", "32g") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()

sc = spark.sparkContext
```
---

## TIER 1 — Ground + Satellite

---

### 1) OpenAQ — CSV → Hanoi hourly (Spark)

```python
import pandas as pd
import numpy as np
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

# --- Đọc raw CSV ---
df_raw = spark.read.csv("raw/openaq_vietnam_hourly.csv", header=True, inferSchema=True)

# --- Filter Hà Nội: bbox + fallback city string ---
df_hn = df_raw.filter(
    F.lower(F.col("city")).rlike("hanoi|ha noi|hà nội")
    | (
        F.col("latitude").between(HANOI_BBOX["lat_min"], HANOI_BBOX["lat_max"]) &
        F.col("longitude").between(HANOI_BBOX["lon_min"], HANOI_BBOX["lon_max"])
    )
)

# --- Parse time ---
df_hn = df_hn.withColumn(
    "datetime_utc",
    F.to_timestamp("datetime_utc").cast("timestamp")
).filter(F.col("datetime_utc").isNotNull())

# --- Valid range cleaning ---
# Giá trị ngoài range vật lý = instrument error hoặc data entry error
VALID_RANGES = {
    "pm25": (0, 500), "pm10": (0, 600), "no2": (0, 200),
    "co": (0, 50),    "so2": (0, 500),  "o3":  (0, 500),
}

def clean_value(param, value):
    if param not in VALID_RANGES:
        return value
    lo, hi = VALID_RANGES[param]
    return F.when(
        (F.col(value) >= lo) & (F.col(value) <= hi), F.col(value)
    ).otherwise(None)

df_hn = df_hn.withColumn(
    "value_clean",
    F.when(
        F.lower(F.col("parameter")).isin(list(VALID_RANGES.keys())),
        F.col("value").cast(DoubleType())
    ).otherwise(None)
)

# --- Filter coverage_pct >= 50% (loại giờ thiếu quá nhiều datapoints) ---
df_hn = df_hn.filter(
    F.col("coverage_pct").cast(DoubleType()).isNull() |
    (F.col("coverage_pct").cast(DoubleType()) >= 50)
)

# --- Pivot long → wide theo station × hour ---
# Giữ station-level (KHÔNG aggregate về 1 row/hour) để dùng spatial gradient sau
PARAMS = ["pm25", "no2", "co", "so2", "o3"]

df_filtered = df_hn.filter(F.lower(F.col("parameter")).isin(PARAMS)) \
    .withColumn("parameter", F.lower(F.col("parameter"))) \
    .withColumn("hour", F.date_trunc("hour", F.col("datetime_utc")))

df_station_hourly = df_filtered.groupBy(
    "location_id", "location_name", "latitude", "longitude", "hour"
).pivot("parameter", PARAMS).agg(F.mean("value_clean"))

df_station_hourly.write.partitionBy("hour").parquet(
    "clean/openaq_station_hourly.parquet"
)

# --- Aggregate Hanoi-level (median robust) để dùng làm target ---
# Đây là bảng dùng làm nhãn training (pm25_hanoi_median)
df_hanoi_hourly = df_station_hourly.groupBy("hour").agg(
    F.expr("percentile_approx(pm25, 0.5)").alias("pm25"),
    F.expr("percentile_approx(no2,  0.5)").alias("no2"),
    F.expr("percentile_approx(co,   0.5)").alias("co"),
    F.expr("percentile_approx(so2,  0.5)").alias("so2"),
    F.expr("percentile_approx(o3,   0.5)").alias("o3"),
    F.countDistinct("location_id").alias("station_count"),
)
df_hanoi_hourly = df_hanoi_hourly.withColumn("date", F.to_date("hour"))

df_hanoi_hourly.write.parquet("clean/openaq_hanoi_hourly.parquet")
print("Saved: openaq_station_hourly + openaq_hanoi_hourly")
```

**Output:**
- `clean/openaq_station_hourly.parquet` — station-level, dùng cho spatial gradient
- `clean/openaq_hanoi_hourly.parquet` — Hanoi-level median, dùng làm target

---

### 2) WeatherAPI — surface meteo proxy (giữ lại có chọn lọc)

```python
import glob, json
from pathlib import Path
import numpy as np
import pandas as pd

def parse_weather_file(filepath: str) -> pd.DataFrame:
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)
    location = data.get("location", {})
    rows = []
    for day in data.get("forecast", {}).get("forecastday", []):
        for h in day.get("hour", []):
            rows.append({
                "time": h.get("time"),
                "vis_km":         h.get("vis_km"),
                "uv":             h.get("uv"),
                "condition_code": h.get("condition", {}).get("code"),
                "is_day":         h.get("is_day"),
                "chance_of_rain": h.get("chance_of_rain"),
                "will_it_rain":   h.get("will_it_rain"),
            })
    return pd.DataFrame(rows)

all_files = (
    glob.glob("raw/weather/Hanoi/*.json") +
    glob.glob("raw/weather/Ha Noi/*.json")
)
dfs = [parse_weather_file(f) for f in all_files]
df_weather = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

# Parse time → UTC
df_weather["datetime_local"] = pd.to_datetime(df_weather["time"], errors="coerce")
df_weather = df_weather[df_weather["datetime_local"].notna()].copy()
df_weather["hour"] = (
    df_weather["datetime_local"]
    .dt.tz_localize("Asia/Ho_Chi_Minh")
    .dt.tz_convert("UTC")
    .dt.floor("h")
)
df_weather = df_weather.drop_duplicates(subset=["hour"])

# Chỉ giữ các cột WeatherAPI-specific (không duplicate với ERA5)
KEEP_COLS = ["hour", "vis_km", "uv", "condition_code", "is_day",
             "chance_of_rain", "will_it_rain"]
df_weather[KEEP_COLS].sort_values("hour").to_parquet(
    "clean/weather_surface_proxy.parquet", index=False
)
print("Saved: clean/weather_surface_proxy.parquet")
```

**Lý do giữ lại:** `vis_km` (tầm nhìn) là proxy tốt cho aerosol loading. `condition_code` encode fog/haze/mist — các event làm PM2.5 tăng đột biến. Những biến này ERA5 không có.

---

### 3) Sentinel-5P — NetCDF → Hanoi daily

**Mục đích:** Lấy column concentration (NO2, CO, SO2, AER index) trong bbox Hà Nội từ satellite S5P/TROPOMI. Ở Tier 2, các giá trị này sẽ được dùng theo cách khác — không chỉ join theo date mà còn extract dọc theo **đường trajectory**, biến satellite thành "witness" của nguồn ô nhiễm mà air mass đã đi qua.

```python
from pathlib import Path
import netCDF4 as nc
import numpy as np
import pandas as pd

PRODUCT_VARS = {
    "NO2": "nitrogendioxide_tropospheric_column",
    "CO":  "carbonmonoxide_total_column",
    "O3":  "ozone_total_vertical_column",
    "SO2": "sulfurdioxide_total_vertical_column",
    "AER": "aerosol_index_354_388",
}

QA_THRESHOLD = {
    "NO2": 0.75, "SO2": 0.75,
    "CO":  0.5,  "O3":  0.5,  "AER": 0.5,
}

PRODUCT_CODES = {
    "NO2": "NO2___", "CO": "CO____", "O3": "O3____",
    "SO2": "SO2___", "AER": "AER_AI",
}

def _find_group(ds: nc.Dataset, variable_name: str):
    g = ds.groups.get("PRODUCT")
    if g is not None and variable_name in g.variables:
        return g
    for gg in ds.groups.values():
        if variable_name in gg.variables:
            return gg
    return None

def read_s5p_file(filepath: str, product_key: str, bbox: dict):
    """
    Đọc 1 file S5P NetCDF, mask theo QA + bbox, trả về stats.
    
    Quan trọng: S5P overpass HN ~13:30 giờ địa phương (06:30 UTC).
    Stats được lưu kèm `overpass_hour_utc` để tránh data leakagecomm
    khi join với hourly data (không broadcast cho cả 24h).
    """
    variable_name = PRODUCT_VARS[product_key]
    qa_thresh = QA_THRESHOLD[product_key]

    ds = nc.Dataset(filepath)
    try:
        group = _find_group(ds, variable_name)
        if group is None:
            return None

        lat = group.variables["latitude"][0].data.astype(float)
        lon = group.variables["longitude"][0].data.astype(float)
        var = group.variables[variable_name][0].data.astype(float)
        qa  = group.variables.get("qa_value")
        qa  = qa[0].data.astype(float) if qa is not None else None

        fill = getattr(group.variables[variable_name], "_FillValue", 9.96921e36)
        var[var == fill] = np.nan
        var[var < -1e30] = np.nan
        if qa is not None:
            var[qa < qa_thresh] = np.nan

        bbox_mask = (
            (lat >= bbox["lat_min"]) & (lat <= bbox["lat_max"]) &
            (lon >= bbox["lon_min"]) & (lon <= bbox["lon_max"])
        )
        valid_mask = bbox_mask & np.isfinite(var)
        if not valid_mask.any():
            return None

        vals = var[valid_mask]
        fname = Path(filepath).stem
        try:
            date_str = fname.split("_")[-2][:8]
            obs_date = pd.to_datetime(date_str, format="%Y%m%d").date()
        except Exception:
            obs_date = None

        return {
            "date": obs_date,
            "product": product_key,
            "overpass_hour_utc": 6,          # S5P HN overpass ~ 06:30 UTC
            "mean": float(np.nanmean(vals)),
            "median": float(np.nanmedian(vals)),
            "max": float(np.nanmax(vals)),
            "std": float(np.nanstd(vals)),
            "pixel_count": int(valid_mask.sum()),
            "valid_pct": float(valid_mask.sum() / bbox_mask.sum() * 100),
        }
    finally:
        ds.close()

nc_files = list(Path("raw/sentinel5p_data").glob("*.nc"))
records = []
for p in sorted(nc_files):
    prod = next((k for k, code in PRODUCT_CODES.items() if code in p.stem), None)
    if prod is None:
        continue
    r = read_s5p_file(str(p), prod, HANOI_BBOX)
    if r:
        records.append(r)

df_s5p = pd.DataFrame(records)

# Pivot: 1 row/date, columns = product × stat
df_s5p_daily = df_s5p.pivot_table(
    index=["date", "overpass_hour_utc"],
    columns="product",
    values=["mean", "median", "max", "std", "pixel_count", "valid_pct"],
    aggfunc="first",
)
df_s5p_daily.columns = [f"{prod.lower()}_{stat}" for stat, prod in df_s5p_daily.columns]
df_s5p_daily = df_s5p_daily.reset_index().sort_values("date")

# Lưu kèm lat/lon grid đầy đủ (cho trajectory path sampling ở Tier 2)
df_s5p_daily.to_parquet("clean/s5p_hanoi_daily.parquet", index=False)
print("Saved: clean/s5p_hanoi_daily.parquet", df_s5p_daily.shape)
```

---

### 4) MAIAC/MODIS — HDF → Hanoi daily AOD

```python
from pathlib import Path
import numpy as np
import pandas as pd

# MAIAC HDF4-EOS: cần pyhdf hoặc GDAL
# pip install pyhdf  (nếu HDF4)
# pip install h5py   (nếu HDF5)

MAIAC_DIR = Path("crawler/maiac_data")

def parse_maiac_filename(name: str):
    """Parse MCD19A2.AYYYYDDD.hXXvYY.061.*.hdf"""
    parts = name.split(".")
    if len(parts) < 5 or not parts[1].startswith("A") or len(parts[1]) != 8:
        return None
    year = int(parts[1][1:5])
    doy  = int(parts[1][5:8])
    return {
        "date": pd.to_datetime(f"{year}-{doy:03d}", format="%Y-%j").date(),
        "tile": parts[2],        # e.g. h28v07 (HN nằm trong tile này)
        "product": parts[0],
    }

def read_maiac_aod(hdf_path: Path, bbox: dict) -> dict | None:
    """
    Đọc AOD_047 và AOD_055 từ MAIAC, mask theo bbox HN.
    
    HDF4-EOS: dùng pyhdf
    - SDS name: "Optical_Depth_047", "Optical_Depth_055"
    - Scale factor: 0.001 (giá trị raw * 0.001 = AOD thực)
    - Fill value: -28672
    - QA: dùng AOD_QA layer, bit 0-1 = retrieval quality (00=best, 01=good)
    """
    try:
        from pyhdf.SD import SD, SDC
        hdf = SD(str(hdf_path), SDC.READ)

        aod_raw = hdf.select("Optical_Depth_047")[:].astype(float)
        qa_raw  = hdf.select("AOD_QA")[:] if "AOD_QA" in hdf.datasets() else None

        # Scale + fill
        aod_raw[aod_raw == -28672] = np.nan
        aod = aod_raw * 0.001

        # QA filter: chỉ giữ bit 0-1 = 00 (best) hoặc 01 (good)
        if qa_raw is not None:
            qa_bits = qa_raw & 0b11
            aod[qa_bits > 1] = np.nan

        # Geolocation: MAIAC dùng sinusoidal projection
        # Tile h28v07: x_offset = 28, y_offset = 7
        # Resolution: 1km (MCD19A2) hoặc 500m (MCD19A3)
        # Tính lat/lon từ sinusoidal → bbox mask
        nrows, ncols = aod.shape
        tile_h = int(hdf_path.stem.split(".")[2][1:3])   # h28 → 28
        tile_v = int(hdf_path.stem.split(".")[2][4:6])   # v07 → 7
        EARTH_RADIUS = 6371007.181
        TILE_SIZE    = 10 * np.pi / 18  # 10 degrees in radians
        x0 = (tile_h - 18) * TILE_SIZE * EARTH_RADIUS
        y0 = (9 - tile_v) * TILE_SIZE * EARTH_RADIUS
        res = TILE_SIZE * EARTH_RADIUS / nrows

        cols = np.arange(ncols)
        rows = np.arange(nrows)
        X = x0 + (cols + 0.5) * res
        Y = y0 - (rows[:, None] + 0.5) * res

        lat = np.degrees(Y / EARTH_RADIUS)
        lon = np.degrees(X / (EARTH_RADIUS * np.cos(Y / EARTH_RADIUS)))

        mask = (
            (lat >= bbox["lat_min"]) & (lat <= bbox["lat_max"]) &
            (lon >= bbox["lon_min"]) & (lon <= bbox["lon_max"]) &
            np.isfinite(aod)
        )
        hdf.end()
        if not mask.any():
            return None

        vals = aod[mask]
        meta = parse_maiac_filename(hdf_path.name)
        if meta is None:
            return None

        return {**meta,
            "aod_mean":   float(np.nanmean(vals)),
            "aod_median": float(np.nanmedian(vals)),
            "aod_max":    float(np.nanmax(vals)),
            "aod_min":    float(np.nanmin(vals)),
            "aod_std":    float(np.nanstd(vals)),
            "aod_valid_pct": float(mask.sum() / (mask.shape[0]*mask.shape[1]) * 100),
        }
    except Exception as e:
        print(f"Error reading {hdf_path.name}: {e}")
        return None

records = [r for fp in sorted(MAIAC_DIR.glob("*.hdf"))
           if (r := read_maiac_aod(fp, HANOI_BBOX)) is not None]

df_maiac = pd.DataFrame(records)

# Aggregate by date (có thể nhiều tile/overpass trong 1 ngày → dùng median)
if not df_maiac.empty:
    df_maiac_daily = df_maiac.groupby("date").agg(
        aod_mean=("aod_mean", "median"),
        aod_median=("aod_median", "median"),
        aod_min=("aod_min", "min"),
        aod_max=("aod_max", "max"),
        aod_std=("aod_std", "median"),
        aod_valid_pct=("aod_valid_pct", "median"),
    ).reset_index().sort_values("date")
else:
    df_maiac_daily = pd.DataFrame(
        columns=["date","aod_mean","aod_median","aod_min","aod_max","aod_std","aod_valid_pct"]
    )

df_maiac_daily.to_parquet("clean/maiac_hanoi_daily.parquet", index=False)
print("Saved: clean/maiac_hanoi_daily.parquet", df_maiac_daily.shape)
```

---

## TIER 2 — Trajectory Engine
### 5) ERA5 wind field — download + Spark processing

**Mục đích:** ERA5 là backbone của HYSPLIT. Cần 2 loại:
- **Pressure levels** (u, v, w, z): để HYSPLIT trace trajectory theo chiều dọc (3D)
- **Single levels** (u10, v10, BLH): surface wind + boundary layer height thực (thay `pbl_proxy` heuristic ở Tier 1)
ERA5 có resolution 0.25° × 0.25°, 6-hourly (reanalysis) hoặc hourly (ERA5-Land). Cần bbox rộng đủ để trace 72h backward — air mass từ North China có thể đến HN trong ~48h.

```python
import cdsapi
# pip install cdsapi
# Setup: tạo ~/.cdsapirc với url + key từ https://cds.climate.copernicus.eu

c = cdsapi.Client()

# --- Pressure level wind (cho HYSPLIT 3D trajectory) ---
c.retrieve("reanalysis-era5-pressure-levels", {
    "product_type": "reanalysis",
    "variable": [
        "u_component_of_wind",   # zonal wind
        "v_component_of_wind",   # meridional wind
        "vertical_velocity",     # omega (Pa/s), cần convert → w (m/s)
        "geopotential",          # để tính height AGL
        "temperature",
        "specific_humidity",
    ],
    "pressure_level": ["1000", "925", "850", "700", "600", "500", "400"],
    "year":  [str(y) for y in range(2022, 2025)],
    "month": [f"{m:02d}" for m in range(1, 13)],
    "day":   [f"{d:02d}" for d in range(1, 32)],
    "time":  ["00:00", "06:00", "12:00", "18:00"],
    "area":  [ERA5_REGION["north"], ERA5_REGION["west"],
              ERA5_REGION["south"], ERA5_REGION["east"]],
    "format": "netcdf",
}, "raw/era5/era5_pressure_levels.nc")

# --- Single level (surface meteo + BLH) ---
c.retrieve("reanalysis-era5-single-levels", {
    "product_type": "reanalysis",
    "variable": [
        "10m_u_component_of_wind",
        "10m_v_component_of_wind",
        "boundary_layer_height",   # BLH thực, thay pbl_proxy heuristic
        "2m_temperature",
        "2m_dewpoint_temperature",
        "surface_pressure",
        "total_precipitation",
        "mean_sea_level_pressure",
    ],
    "year":  [str(y) for y in range(2022, 2025)],
    "month": [f"{m:02d}" for m in range(1, 13)],
    "day":   [f"{d:02d}" for d in range(1, 32)],
    "time":  [f"{h:02d}:00" for h in range(24)],
    "area":  [ERA5_REGION["north"], ERA5_REGION["west"],
              ERA5_REGION["south"], ERA5_REGION["east"]],
    "format": "netcdf",
}, "raw/era5/era5_surface.nc")

print("ERA5 download complete.")
```

**Spark processing — ERA5 surface → parquet:**

```python
import xarray as xr
from pyspark.sql import functions as F
import pandas as pd

# ERA5 surface: flatten theo time × lat × lon
ds_sfc = xr.open_dataset("raw/era5/era5_surface.nc")

def era5_surface_day(date_str: str) -> list[dict]:
    ds_day = ds_sfc.sel(time=slice(date_str, date_str))
    df = ds_day.to_dataframe().reset_index()

    # Tính BLH proxy thực từ ERA5 (thay heuristic WeatherAPI)
    # BLH unit: meters. ERA5 BLH = blh variable
    df = df.rename(columns={
        "u10": "wind_u10", "v10": "wind_v10",
        "blh": "pbl_height_m",
        "t2m": "temp_2m_k",
        "sp":  "surface_pressure_pa",
        "msl": "mslp_pa",
        "tp":  "precip_m",
    })
    df["temp_2m_c"]   = df["temp_2m_k"] - 273.15
    df["precip_mm"]   = df["precip_m"] * 1000
    df["wind_speed"]  = (df["wind_u10"]**2 + df["wind_v10"]**2)**0.5
    df["wind_dir"]    = (270 - (df["wind_v10"] / df["wind_u10"]).apply(
                            lambda x: 0 if x == 0 else 0) * 57.3) % 360  # simplified
    df["low_pbl"]     = (df["pbl_height_m"] < 300).astype(int)
    df["date"]        = date_str
    return df.to_dict("records")

dates = pd.date_range("2022-01-01", "2024-12-31", freq="D").strftime("%Y-%m-%d").tolist()
rdd = sc.parallelize(dates, numSlices=100)

df_era5_sfc = rdd.flatMap(era5_surface_day).toDF()
df_era5_sfc = df_era5_sfc.filter(
    F.col("latitude").between(ERA5_REGION["south"], ERA5_REGION["north"]) &
    F.col("longitude").between(ERA5_REGION["west"], ERA5_REGION["east"])
)
df_era5_sfc.write.partitionBy("date").parquet("clean/era5_surface_grid.parquet")

# Hanoi-center extraction (1 row/hour cho Hanoi point)
df_hn_sfc = df_era5_sfc.filter(
    F.col("latitude").between(HANOI_BBOX["lat_min"], HANOI_BBOX["lat_max"]) &
    F.col("longitude").between(HANOI_BBOX["lon_min"], HANOI_BBOX["lon_max"])
).groupBy("time", "date").agg(
    F.mean("wind_u10").alias("wind_u10"),
    F.mean("wind_v10").alias("wind_v10"),
    F.mean("pbl_height_m").alias("pbl_height_m"),
    F.mean("temp_2m_c").alias("temp_2m_c"),
    F.mean("precip_mm").alias("precip_mm"),
    F.mean("wind_speed").alias("wind_speed"),
    F.mean("low_pbl").alias("low_pbl"),
    F.mean("surface_pressure_pa").alias("surface_pressure_pa"),
)

df_hn_sfc = df_hn_sfc.withColumn(
    "hour", F.date_trunc("hour", F.col("time"))
)

df_hn_sfc.write.parquet("clean/era5_hanoi_hourly.parquet")
print("Saved: era5_surface_grid + era5_hanoi_hourly")
```

**Giải thích features:**
- `pbl_height_m` (ERA5 BLH): thay thế `pbl_proxy` heuristic. BLH thấp (<300m) → ô nhiễm bị nhốt gần mặt đất, PM2.5 tăng đột biến.
- `wind_speed` và `wind_u10/v10` (ERA5): thay wind từ WeatherAPI, chính xác hơn nhiều.
- Giữ full grid (không chỉ HN center) để dùng làm meteo context cho trajectory path.

---

### 6) HYSPLIT trajectory engine

**Mục đích:** Tính backward trajectory (tìm nguồn) và forward trajectory (dự đoán hướng đi) dùng HYSPLIT binary với ERA5 wind field. Đây là core của Tier 2.

**Setup HYSPLIT:**
```bash
# Download HYSPLIT từ NOAA: https://www.ready.noaa.gov/HYSPLIT.php
# Install binary vào /opt/hysplit/exec/
# Convert ERA5 NetCDF → ARL format (HYSPLIT input format)
/opt/hysplit/exec/era5_2arl -f raw/era5/era5_pressure_levels.nc -o raw/arl_meteo/
```

#### 6a) Backward trajectory — tìm nguồn ô nhiễm

**Chiến lược:** Với mỗi giờ có PM2.5 cao tại HN, chạy backward 72h từ 3 altitude (100m, 500m, 1000m AGL). Ensemble 9 trajectories (3 init point × 3 altitude) cho 1 timestamp — cluster lại để xác định source region.

```python
import subprocess, itertools
from pathlib import Path
import pandas as pd

HYSPLIT_EXEC = Path("/opt/hysplit/exec/hyts_std")
ARL_DIR      = Path("raw/arl_meteo")
TRAJ_OUT_DIR = Path("clean/trajectories/backward")
TRAJ_OUT_DIR.mkdir(parents=True, exist_ok=True)

def write_hysplit_control(
    date_yymmddhh: str,
    lat: float, lon: float, alt_m: float,
    duration_h: int,          # âm = backward
    arl_dir: Path,
    traj_out_path: Path,
) -> Path:
    """
    Viết HYSPLIT CONTROL file.
    date_yymmddhh: "24110306" = 2024-11-03 06:00 UTC
    duration_h: -72 (backward 72h) hoặc +24 (forward 24h)
    """
    yy, mm, dd, hh = date_yymmddhh[:2], date_yymmddhh[2:4], date_yymmddhh[4:6], date_yymmddhh[6:8]

    # Tìm ARL file cho tháng tương ứng
    arl_file = arl_dir / f"ERA5_{yy}{mm}.ARL"
    if not arl_file.exists():
        raise FileNotFoundError(f"ARL file not found: {arl_file}")

    control_content = f"""{yy} {mm} {dd} {hh}
1
{lat:.3f} {lon:.3f} {alt_m:.1f}
{duration_h}
0
{arl_dir}/
{arl_file.name}
{traj_out_path.parent}/
{traj_out_path.name}
"""
    ctrl_path = Path(f"CONTROL_{date_yymmddhh}_{int(alt_m)}m")
    ctrl_path.write_text(control_content)
    return ctrl_path

# Init points: center + 4 offsets 0.2° (ensemble)
INIT_POINTS = [
    (HANOI_CENTER["lat"],        HANOI_CENTER["lon"]),
    (HANOI_CENTER["lat"] + 0.2, HANOI_CENTER["lon"]),
    (HANOI_CENTER["lat"] - 0.2, HANOI_CENTER["lon"]),
    (HANOI_CENTER["lat"],        HANOI_CENTER["lon"] + 0.2),
    (HANOI_CENTER["lat"],        HANOI_CENTER["lon"] - 0.2),
]
ALTITUDES_BACKWARD = [100, 500, 1000]   # meters AGL
HOURS_TO_RUN       = [0, 6, 12, 18]    # UTC hours/day

# Spark parallelism: 1 task = 1 trajectory
jobs = list(itertools.product(
    ["241103", "241104"],   # dates YYMMDD
    HOURS_TO_RUN,
    INIT_POINTS,
    ALTITUDES_BACKWARD,
))

def run_backward_job(job):
    date_str, hour, (lat, lon), alt = job
    dt_key = f"{date_str}{hour:02d}"
    out_path = TRAJ_OUT_DIR / f"back_{dt_key}_{lat:.2f}_{lon:.2f}_{int(alt)}m"
    if out_path.exists():
        return str(out_path)
    try:
        ctrl = write_hysplit_control(dt_key, lat, lon, alt, -72, ARL_DIR, out_path)
        subprocess.run([str(HYSPLIT_EXEC)], check=True, timeout=120,
                       cwd=str(ctrl.parent), capture_output=True)
    except Exception as e:
        return f"ERROR:{e}"
    return str(out_path)

rdd = sc.parallelize(jobs, numSlices=len(jobs))
results = rdd.map(run_backward_job).collect()
print(f"Completed {sum(1 for r in results if not r.startswith('ERROR'))} trajectories")
```

#### 6b) Forward trajectory — dự đoán plume đi đâu

**Chiến lược:** Với thời điểm hiện tại (hoặc forecast window), chạy forward 24h từ HN với ensemble 27 particles (3 × 3 × 3 lat/lon/alt offsets). Output là probability field — vùng nào có xác suất cao nhận plume từ HN.

```python
ALTITUDES_FORWARD = [50, 200, 500]    # meters AGL (thấp hơn vì focus surface pollution)
TRAJ_FWD_DIR = Path("clean/trajectories/forward")
TRAJ_FWD_DIR.mkdir(parents=True, exist_ok=True)

# Ensemble 27 particles: 3 lat × 3 lon × 3 alt
LAT_OFFSETS = [-0.1, 0.0, 0.1]
LON_OFFSETS = [-0.1, 0.0, 0.1]

fwd_jobs = list(itertools.product(
    ["241103"],
    [0, 6, 12, 18],
    LAT_OFFSETS, LON_OFFSETS,
    ALTITUDES_FORWARD,
))

def run_forward_job(job):
    date_str, hour, dlat, dlon, alt = job
    lat = HANOI_CENTER["lat"] + dlat
    lon = HANOI_CENTER["lon"] + dlon
    dt_key = f"{date_str}{hour:02d}"
    out_path = TRAJ_FWD_DIR / f"fwd_{dt_key}_{dlat:+.1f}_{dlon:+.1f}_{int(alt)}m"
    if out_path.exists():
        return str(out_path)
    try:
        ctrl = write_hysplit_control(dt_key, lat, lon, alt, +24, ARL_DIR, out_path)
        subprocess.run([str(HYSPLIT_EXEC)], check=True, timeout=120,
                       cwd=str(ctrl.parent), capture_output=True)
    except Exception as e:
        return f"ERROR:{e}"
    return str(out_path)

rdd_fwd = sc.parallelize(fwd_jobs, numSlices=len(fwd_jobs))
results_fwd = rdd_fwd.map(run_forward_job).collect()
```

---

### 7) Parse HYSPLIT output → Spark DataFrame

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

def parse_hysplit_file(filepath: str) -> list[dict]:
    """
    Parse HYSPLIT trajectory text output.
    Format: header (grid info, traj init) + data rows.
    Mỗi row = 1 timestep của 1 trajectory.
    """
    records = []
    try:
        with open(filepath) as f:
            lines = f.readlines()
    except Exception:
        return records

    # Header parsing: số grid files
    n_grids = int(lines[0].split()[0])
    header_end = n_grids + 1
    # Số trajectories trong file
    traj_meta_line = lines[header_end].split()
    n_traj = int(traj_meta_line[0])
    data_start = header_end + n_traj + 2

    fname = Path(filepath).name
    direction = "backward" if "back_" in fname else "forward"

    for line in lines[data_start:]:
        parts = line.split()
        if len(parts) < 11:
            continue
        try:
            records.append({
                "traj_id":      fname,
                "direction":    direction,
                "traj_no":      int(parts[0]),
                "year":         int(parts[1]),
                "month":        int(parts[2]),
                "day":          int(parts[3]),
                "hour":         int(parts[4]),
                "minute":       int(parts[5]),
                "forecast_hour":int(parts[6]),
                "age_h":        int(parts[7]),    # giờ tính từ init (âm = backward)
                "lat":          float(parts[8]),
                "lon":          float(parts[9]),
                "alt_m":        float(parts[10]),
            })
        except (ValueError, IndexError):
            continue

    return records

# Load tất cả trajectory files qua Spark
all_traj_files = (
    [str(p) for p in TRAJ_OUT_DIR.glob("back_*")] +
    [str(p) for p in TRAJ_FWD_DIR.glob("fwd_*")]
)

traj_schema = StructType([
    StructField("traj_id",      StringType()),
    StructField("direction",    StringType()),
    StructField("traj_no",      IntegerType()),
    StructField("year",         IntegerType()),
    StructField("month",        IntegerType()),
    StructField("day",          IntegerType()),
    StructField("hour",         IntegerType()),
    StructField("minute",       IntegerType()),
    StructField("forecast_hour",IntegerType()),
    StructField("age_h",        IntegerType()),
    StructField("lat",          FloatType()),
    StructField("lon",          FloatType()),
    StructField("alt_m",        FloatType()),
])

rdd_parse = sc.parallelize(all_traj_files, numSlices=200)
df_traj = rdd_parse.flatMap(parse_hysplit_file).toDF(schema=traj_schema)
df_traj = df_traj.withColumn(
    "timestamp",
    F.make_timestamp("year","month","day","hour","minute", F.lit(0))
)
df_traj.write.partitionBy("direction").parquet("clean/hysplit_trajectories.parquet")
print("Saved: clean/hysplit_trajectories.parquet")
```

---

### 8) Feature engineering — Trajectory clustering + Spatial gradient (Spark)
#### 8a) Trajectory clustering (K-Means trên path)

**Mục đích:** Phân loại loại air mass theo nguồn gốc. Cluster điển hình cho HN:
- Cluster 0: Recirculation local (path ngắn, quanh đồng bằng sông Hồng)
- Cluster 1: Northwest — từ Vân Nam, Quảng Tây → thường mang bụi đất + công nghiệp
- Cluster 2: Northeast — từ Pearl River Delta → NO₂ và SO₂ cao
- Cluster 3: South China Sea — clean marine air mass
- Cluster 4: Southwest monsoon — ẩm, PM thấp
- Cluster 5: Stagnant — BLH thấp, no transport

```python
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler, StandardScaler

df_back = spark.read.parquet("clean/hysplit_trajectories.parquet") \
    .filter(F.col("direction") == "backward")

# Pivot: mỗi trajectory → vector (lat/lon tại 8 timesteps đặc trưng)
ANCHOR_HOURS = [0, -6, -12, -24, -36, -48, -60, -72]

df_anchors = df_back.filter(F.col("age_h").isin(ANCHOR_HOURS))

df_pivot = df_anchors.groupBy("traj_id").pivot(
    "age_h", ANCHOR_HOURS
).agg(
    F.first("lat").alias("lat"),
    F.first("lon").alias("lon"),
)

# Flatten column names
feature_cols = []
for h in ANCHOR_HOURS:
    df_pivot = df_pivot \
        .withColumnRenamed(f"{h}_lat", f"lat_h{abs(h):02d}") \
        .withColumnRenamed(f"{h}_lon", f"lon_h{abs(h):02d}")
    feature_cols += [f"lat_h{abs(h):02d}", f"lon_h{abs(h):02d}"]

df_pivot = df_pivot.fillna(0)
assembler = VectorAssembler(inputCols=feature_cols, outputCol="raw_features")
df_vec = assembler.transform(df_pivot)

scaler = StandardScaler(inputCol="raw_features", outputCol="features")
df_scaled = scaler.fit(df_vec).transform(df_vec)

# Chọn k=6 (số cluster điển hình cho HN)
# Trong thực tế: chạy k=3..10, chọn k theo elbow trên WCSS
kmeans = KMeans(k=6, seed=42, maxIter=50, featuresCol="features", predictionCol="cluster_id")
km_model = kmeans.fit(df_scaled)
df_clustered = km_model.transform(df_scaled).select("traj_id", "cluster_id")

# Merge cluster_id vào trajectory table
df_traj_enriched = df_back.join(df_clustered, on="traj_id", how="left")

# Thêm dominant source region (centroid của cluster tại age=-72h)
df_source = df_traj_enriched.filter(F.col("age_h") == -72).groupBy("cluster_id").agg(
    F.mean("lat").alias("source_lat"),
    F.mean("lon").alias("source_lon"),
    F.mean("alt_m").alias("source_alt_m"),
)
df_traj_enriched = df_traj_enriched.join(df_source, on="cluster_id", how="left")
df_traj_enriched.write.parquet("clean/trajectories_clustered.parquet")
```

#### 8b) Spatial gradient features từ multi-station OpenAQ

**Mục đích:** Nếu trạm phía Bắc HN có PM2.5 cao hơn trạm phía Nam → nguồn đang đến từ phía Bắc. Gradient này encode spatial context mà single-point forecast bỏ qua.

```python
from scipy.spatial import cKDTree
import numpy as np

# Load station-level data
df_stn = spark.read.parquet("clean/openaq_station_hourly.parquet")

# Pandas UDF: tính gradient theo 4 hướng bằng IDW (Inverse Distance Weighting)
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import DoubleType
import pandas as pd

# Tính cho mỗi giờ: gradient N/S/E/W
def compute_spatial_gradient(df_hour: pd.DataFrame) -> pd.DataFrame:
    """
    Input: stations trong 1 giờ (lat, lon, pm25)
    Output: 1 row với pm25_grad_N, pm25_grad_S, pm25_grad_E, pm25_grad_W
    """
    df_valid = df_hour.dropna(subset=["lat", "lon", "pm25"])
    if len(df_valid) < 3:
        return pd.DataFrame([{
            "pm25_grad_N": None, "pm25_grad_S": None,
            "pm25_grad_E": None, "pm25_grad_W": None,
            "pm25_spatial_std": None,
        }])

    center_lat = HANOI_CENTER["lat"]
    center_lon = HANOI_CENTER["lon"]
    offset = 0.25  # 0.25° ~ 28km

    def idw_at(target_lat, target_lon, k=3):
        """IDW interpolation tại điểm target."""
        coords = df_valid[["lat", "lon"]].values
        tree = cKDTree(coords)
        dists, idxs = tree.query([target_lat, target_lon], k=min(k, len(df_valid)))
        dists = np.where(dists == 0, 1e-6, dists)
        weights = 1.0 / dists
        return float((weights * df_valid["pm25"].iloc[idxs].values).sum() / weights.sum())

    pm25_N = idw_at(center_lat + offset, center_lon)
    pm25_S = idw_at(center_lat - offset, center_lon)
    pm25_E = idw_at(center_lat, center_lon + offset)
    pm25_W = idw_at(center_lat, center_lon - offset)

    return pd.DataFrame([{
        "pm25_grad_N": pm25_N,
        "pm25_grad_S": pm25_S,
        "pm25_grad_E": pm25_E,
        "pm25_grad_W": pm25_W,
        "pm25_spatial_std": float(df_valid["pm25"].std()),
        # Gradient magnitude: nếu cao → spatial inhomogeneous → có nguồn điểm
        "pm25_grad_mag": float(np.sqrt((pm25_N-pm25_S)**2 + (pm25_E-pm25_W)**2)),
    }])

df_gradient = df_stn.groupBy("hour").applyInPandas(
    compute_spatial_gradient,
    schema="hour timestamp, pm25_grad_N double, pm25_grad_S double, "
           "pm25_grad_E double, pm25_grad_W double, "
           "pm25_spatial_std double, pm25_grad_mag double"
)
df_gradient.write.parquet("clean/openaq_spatial_gradient.parquet")
```

#### 8c) Satellite sampling dọc theo trajectory path

**Mục đích:** Thay vì chỉ lấy S5P/MAIAC tại HN, extract giá trị satellite tại các điểm trajectory đi qua — biến satellite thành "fingerprint" của nguồn.

```python
def sample_s5p_along_trajectory(traj_df: pd.DataFrame, s5p_grid: pd.DataFrame) -> pd.DataFrame:
    """
    Với mỗi trajectory, extract mean S5P column tại các điểm path.
    s5p_grid: có lat, lon, date, no2_mean, aer_mean (full pixel grid, không aggregate về HN)
    """
    results = []
    for traj_id, grp in traj_df.groupby("traj_id"):
        # Chỉ lấy path segment giờ -24 đến -72 (upwind region)
        path = grp[grp["age_h"].between(-72, -24)].copy()
        if path.empty:
            continue

        date_str = str(path["timestamp"].dt.date.mode()[0])
        s5p_day = s5p_grid[s5p_grid["date"] == date_str]
        if s5p_day.empty:
            continue

        # Spatial join: lấy pixel S5P gần nhất cho từng điểm trajectory
        traj_coords = path[["lat", "lon"]].values
        s5p_coords  = s5p_day[["lat", "lon"]].values
        tree = cKDTree(s5p_coords)
        dists, idxs = tree.query(traj_coords, k=1)

        # Chỉ lấy pixel trong vòng 0.5° (~55km)
        valid = dists < 0.5
        if valid.sum() == 0:
            continue

        matched = s5p_day.iloc[idxs[valid]]
        results.append({
            "traj_id":        traj_id,
            "path_no2_mean":  float(matched["no2_mean"].mean()),
            "path_aer_mean":  float(matched["aer_mean"].mean()),
            "path_no2_max":   float(matched["no2_mean"].max()),
            "path_no2_std":   float(matched["no2_mean"].std()),
            # Ratio NO2/AER: signature của nguồn (traffic vs biomass burning)
            "path_no2_aer_ratio": float(matched["no2_mean"].mean() /
                                        (matched["aer_mean"].mean() + 1e-6)),
        })
    return pd.DataFrame(results)
```

---

### 9) Build master feature table (hourly)

**Mục đích:** Join tất cả các bảng đã tạo thành 1 table duy nhất — đây là input cho model. Mỗi row = 1 giờ tại Hà Nội, với đầy đủ features từ ground + satellite + trajectory.

```python
import numpy as np
from pyspark.sql import functions as F

# --- Load tất cả bảng sạch ---
df_aq    = spark.read.parquet("clean/openaq_hanoi_hourly.parquet")
df_era5  = spark.read.parquet("clean/era5_hanoi_hourly.parquet")
df_wx    = spark.read.parquet("clean/weather_surface_proxy.parquet")  # WeatherAPI (vis, uv)
df_s5p   = spark.read.parquet("clean/s5p_hanoi_daily.parquet")
df_maiac = spark.read.parquet("clean/maiac_hanoi_daily.parquet")
df_grad  = spark.read.parquet("clean/openaq_spatial_gradient.parquet")

# Traj features: aggregate cluster distribution per hour
df_traj_feat = spark.read.parquet("clean/trajectories_clustered.parquet") \
    .filter(F.col("direction") == "backward") \
    .groupBy("timestamp").agg(
        F.mode("cluster_id").alias("dominant_cluster"),
        F.countDistinct("traj_id").alias("n_traj"),
        F.mean("source_lat").alias("source_lat"),
        F.mean("source_lon").alias("source_lon"),
    ) \
    .withColumnRenamed("timestamp", "hour")

# --- Base join: AQ × ERA5 (hourly) ---
master = df_aq.join(df_era5, on="hour", how="left")

# WeatherAPI surface proxy (vis, uv — không có trong ERA5)
df_wx_sdf = spark.createDataFrame(df_wx) if isinstance(df_wx, pd.DataFrame) else df_wx
master = master.join(df_wx_sdf.select("hour", "vis_km", "uv", "condition_code",
                                       "chance_of_rain", "is_day"),
                     on="hour", how="left")

# Spatial gradient
master = master.join(df_grad, on="hour", how="left")

# Trajectory cluster features
master = master.join(df_traj_feat, on="hour", how="left")

# Date key để join satellite
master = master.withColumn("date", F.to_date("hour"))

# --- S5P join (chú ý: chỉ valid sau overpass hour) ---
# Tránh data leakage: chỉ dùng S5P[date] cho hour >= overpass_hour_utc
# Hour trước overpass → dùng S5P của ngày hôm trước (ffill)
df_s5p_sdf = spark.createDataFrame(df_s5p) if isinstance(df_s5p, pd.DataFrame) else df_s5p
master = master.join(df_s5p_sdf.drop("overpass_hour_utc"), on="date", how="left")

# --- MAIAC join ---
df_maiac_sdf = spark.createDataFrame(df_maiac) if isinstance(df_maiac, pd.DataFrame) else df_maiac
master = master.join(df_maiac_sdf, on="date", how="left")

# --- Time features ---
master = master \
    .withColumn("hour_of_day", F.hour("hour")) \
    .withColumn("day_of_week", F.dayofweek("hour")) \
    .withColumn("month",       F.month("hour")) \
    .withColumn("is_weekend",  (F.col("day_of_week") >= 6).cast("int")) \
    .withColumn("is_rush_hour",
        ((F.col("hour_of_day").between(7, 9)) |
         (F.col("hour_of_day").between(17, 19))).cast("int")) \
    .withColumn("hour_sin",   F.sin(2 * np.pi * F.col("hour_of_day") / 24)) \
    .withColumn("hour_cos",   F.cos(2 * np.pi * F.col("hour_of_day") / 24)) \
    .withColumn("dow_sin",    F.sin(2 * np.pi * F.col("day_of_week") / 7)) \
    .withColumn("dow_cos",    F.cos(2 * np.pi * F.col("day_of_week") / 7)) \
    .withColumn("month_sin",  F.sin(2 * np.pi * F.col("month") / 12)) \
    .withColumn("month_cos",  F.cos(2 * np.pi * F.col("month") / 12))

# --- Season ---
master = master.withColumn("season",
    F.when(F.col("month").isin([11,12,1,2]), "winter")
     .when(F.col("month").isin([3,4]),       "spring")
     .when(F.col("month").isin([5,6,7,8]),   "summer")
     .otherwise("autumn")
)

# --- Lag features (reindex theo grid trước để tránh shift sai khi có gaps) ---
# Sort + window để đảm bảo lag thực sự là 1h gap
from pyspark.sql.window import Window

w = Window.orderBy("hour")
master = master.withColumn("prev_hour", F.lag("hour", 1).over(w))
master = master.withColumn(
    "hour_gap",
    (F.col("hour").cast("long") - F.col("prev_hour").cast("long")) / 3600
)

# Chỉ tạo lag khi gap == 1h (không có missing hour)
for lag in [1, 3, 6, 12, 24]:
    w_lag = Window.orderBy("hour").rowsBetween(-lag, -lag)
    master = master.withColumn(
        f"pm25_lag_{lag}h",
        F.when(F.col("hour_gap") == 1, F.first("pm25").over(w_lag)).otherwise(None)
    )

# --- Rolling statistics ---
for window_h in [3, 6, 12, 24]:
    w_roll = Window.orderBy("hour").rowsBetween(-window_h, -1)
    master = master \
        .withColumn(f"pm25_roll_mean_{window_h}h", F.mean("pm25").over(w_roll)) \
        .withColumn(f"pm25_roll_max_{window_h}h",  F.max("pm25").over(w_roll)) \
        .withColumn(f"pm25_roll_std_{window_h}h",  F.stddev("pm25").over(w_roll))

# --- Target variables ---
w_fwd = Window.orderBy("hour")
master = master \
    .withColumn("pm25_next_6h",  F.lead("pm25",  6).over(w_fwd)) \
    .withColumn("pm25_next_12h", F.lead("pm25", 12).over(w_fwd)) \
    .withColumn("pm25_next_24h", F.lead("pm25", 24).over(w_fwd))

master.write.parquet("output/master_features_hanoi.parquet")
print("Saved: output/master_features_hanoi.parquet", master.count(), "rows")
```

---

### 10) Model — XGBoost / LightGBM forecast

**Mục đích:** Dự đoán PM2.5 tại Hà Nội 6h/12h/24h tới dựa trên toàn bộ feature table. Dùng XGBoost/LightGBM thay vì neural network — đủ mạnh cho tabular time-series, interpretable, và train nhanh trên GPU.

**Chiến lược train/eval:**
- **Không dùng random split** — dùng time-based split (train trước 2024-01-01, val sau)
- Forward chaining cross-validation: tránh data leakage từ lag features

```python
import xgboost as xgb
import lightgbm as lgb
import pandas as pd
import numpy as np
from sklearn.metrics import mean_absolute_error, mean_squared_error

# --- Load master table ---
df = pd.read_parquet("output/master_features_hanoi.parquet")
df = df.sort_values("hour").reset_index(drop=True)

# --- Feature groups ---
TRAJ_FEATURES = [
    "dominant_cluster", "source_lat", "source_lon", "n_traj",
    "path_no2_mean", "path_aer_mean", "path_no2_aer_ratio",
    "pm25_grad_N", "pm25_grad_S", "pm25_grad_E", "pm25_grad_W",
    "pm25_grad_mag", "pm25_spatial_std",
]
METEO_FEATURES = [
    "wind_u10", "wind_v10", "wind_speed",
    "pbl_height_m", "low_pbl",                   # ERA5 BLH (thay pbl_proxy)
    "temp_2m_c", "precip_mm", "surface_pressure_pa",
    "vis_km", "uv", "condition_code",             # WeatherAPI proxy
    "chance_of_rain",
]
SAT_FEATURES = [
    "no2_mean", "aer_mean", "so2_mean", "co_mean",  # S5P
    "aod_mean", "aod_max", "aod_std",               # MAIAC
]
TIME_FEATURES = [
    "hour_sin", "hour_cos", "dow_sin", "dow_cos", "month_sin", "month_cos",
    "is_weekend", "is_rush_hour", "season",
]
LAG_FEATURES = (
    [f"pm25_lag_{h}h" for h in [1, 3, 6, 12, 24]] +
    [f"pm25_roll_mean_{h}h" for h in [3, 6, 12, 24]] +
    [f"pm25_roll_max_{h}h"  for h in [3, 6, 12, 24]] +
    [f"pm25_roll_std_{h}h"  for h in [3, 6, 12, 24]]
)

ALL_FEATURES = TRAJ_FEATURES + METEO_FEATURES + SAT_FEATURES + TIME_FEATURES + LAG_FEATURES

# --- Time-based split ---
CUTOFF = "2024-01-01"
df_train = df[df["hour"] < CUTOFF].copy()
df_val   = df[df["hour"] >= CUTOFF].copy()

for target_col in ["pm25_next_6h", "pm25_next_12h", "pm25_next_24h"]:
    df_t = df_train.dropna(subset=[target_col] + ALL_FEATURES)
    df_v = df_val.dropna(subset=[target_col] + ALL_FEATURES)

    X_train, y_train = df_t[ALL_FEATURES], df_t[target_col]
    X_val,   y_val   = df_v[ALL_FEATURES], df_v[target_col]

    # --- LightGBM với GPU ---
    params_lgb = {
        "objective":     "regression",
        "metric":        "rmse",
        "learning_rate": 0.05,
        "num_leaves":    127,
        "max_depth":     -1,
        "min_data_in_leaf": 20,
        "feature_fraction": 0.8,
        "bagging_fraction": 0.8,
        "bagging_freq":  5,
        "lambda_l1":     0.1,
        "lambda_l2":     0.1,
        "device":        "gpu",    # GPU acceleration
        "verbose":       -1,
    }

    dtrain = lgb.Dataset(X_train, label=y_train,
                         categorical_feature=["dominant_cluster", "condition_code", "season"])
    dval   = lgb.Dataset(X_val, label=y_val, reference=dtrain)

    model_lgb = lgb.train(
        params_lgb, dtrain,
        num_boost_round=2000,
        valid_sets=[dval],
        callbacks=[lgb.early_stopping(50), lgb.log_evaluation(100)],
    )

    pred = model_lgb.predict(X_val)
    mae  = mean_absolute_error(y_val, pred)
    rmse = np.sqrt(mean_squared_error(y_val, pred))
    print(f"[{target_col}] MAE={mae:.2f} µg/m³  RMSE={rmse:.2f} µg/m³")

    model_lgb.save_model(f"models/lgb_{target_col}.txt")

    # --- Feature importance (trajectory vs meteo vs satellite) ---
    fi = pd.DataFrame({
        "feature":    ALL_FEATURES,
        "importance": model_lgb.feature_importance(importance_type="gain"),
    }).sort_values("importance", ascending=False)

    # Group importance theo category
    fi["group"] = fi["feature"].apply(
        lambda f: "trajectory" if f in TRAJ_FEATURES
              else ("meteo" if f in METEO_FEATURES
              else ("satellite" if f in SAT_FEATURES
              else ("lag" if f in LAG_FEATURES else "time")))
    )
    print(fi.groupby("group")["importance"].sum().sort_values(ascending=False))
```

---

## 11) Output — Source attribution + Plume forecast

### Source attribution (backward)

```python
# Cluster → source region mapping (human-labeled sau khi xem centroids)
CLUSTER_LABELS = {
    0: "Local_HN_recirculation",
    1: "Northwest_Yunnan_Guangxi",
    2: "Northeast_Pearl_River_Delta",
    3: "South_China_Sea_marine",
    4: "Southwest_monsoon",
    5: "Stagnant_local",
}

# Attribution report: với mỗi pollution episode (PM2.5 > AQI threshold)
df_episodes = df[df["pm25"] > 75].copy()  # WHO 24h guideline
df_episodes["source_label"] = df_episodes["dominant_cluster"].map(CLUSTER_LABELS)
df_episodes["contribution_pct"] = (
    df_episodes["path_no2_mean"] / df_episodes["path_no2_mean"].sum() * 100
)

# Export GeoJSON
import json
features = []
for _, row in df_episodes.iterrows():
    if pd.notna(row["source_lat"]):
        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [row["source_lon"], row["source_lat"]]},
            "properties": {
                "hour": str(row["hour"]),
                "source": row["source_label"],
                "pm25_hn": row["pm25"],
                "path_no2": row["path_no2_mean"],
                "path_aod": row["aod_mean"],
            }
        })

with open("output/source_attribution.geojson", "w") as f:
    json.dump({"type": "FeatureCollection", "features": features}, f)
print("Saved: output/source_attribution.geojson")
```

### Plume forecast grid (forward)

```python
# Forward trajectory ensemble → probability grid 0.1° resolution
df_fwd = pd.read_parquet("clean/hysplit_trajectories.parquet")
df_fwd = df_fwd[df_fwd["direction"] == "forward"].copy()

# Tại mỗi timestep (age_h = +6, +12, +24):
for forecast_h in [6, 12, 24]:
    df_slice = df_fwd[df_fwd["age_h"] == forecast_h].copy()

    # Bin lat/lon vào grid 0.1°
    df_slice["lat_bin"] = (df_slice["lat"] / 0.1).round() * 0.1
    df_slice["lon_bin"] = (df_slice["lon"] / 0.1).round() * 0.1

    # Probability = fraction của ensemble particles trong ô lưới
    prob_grid = df_slice.groupby(["lat_bin", "lon_bin"]).size().reset_index(name="count")
    prob_grid["probability"] = prob_grid["count"] / prob_grid["count"].sum()

    # Export
    prob_grid.to_csv(f"output/plume_forecast_{forecast_h}h.csv", index=False)

print("Saved: plume forecast grids for 6h / 12h / 24h")
```

---

## 12) Pipeline summary — file map

```
raw/
├── openaq_vietnam_hourly.csv
├── weather/Hanoi/*.json          (WeatherAPI)
├── sentinel5p_data/*.nc
├── era5/
│   ├── era5_pressure_levels.nc  (NEW — ERA5 3D wind)
│   └── era5_surface.nc          (NEW — ERA5 surface + BLH)
├── arl_meteo/*.ARL              (NEW — ERA5 converted for HYSPLIT)
└── crawler/maiac_data/*.hdf

clean/
├── openaq_station_hourly.parquet    (station-level, spatial gradient)
├── openaq_hanoi_hourly.parquet      (HN-level median, target)
├── weather_surface_proxy.parquet    (vis, uv, condition — WeatherAPI only)
├── era5_surface_grid.parquet        (full SE Asia grid)
├── era5_hanoi_hourly.parquet        (HN point extraction)
├── s5p_hanoi_daily.parquet
├── maiac_hanoi_daily.parquet
├── openaq_spatial_gradient.parquet  (pm25_grad_N/S/E/W)
├── hysplit_trajectories.parquet     (backward + forward)
└── trajectories_clustered.parquet   (+ cluster_id + source_lat/lon)

output/
├── master_features_hanoi.parquet    (all features joined, hourly)
├── source_attribution.geojson       (backward: nguồn gốc episodes)
└── plume_forecast_6h/12h/24h.csv   (forward: probability grid)

models/
├── lgb_pm25_next_6h.txt
├── lgb_pm25_next_12h.txt
└── lgb_pm25_next_24h.txt
```

### Join keys

| Bảng A | Bảng B | Key |
|---|---|---|
| OpenAQ hourly | ERA5 hourly | `hour` (UTC) |
| ERA5 hourly | WeatherAPI proxy | `hour` (UTC) |
| (AQ+ERA5) | Gradient | `hour` (UTC) |
| (AQ+ERA5) | Trajectory features | `hour` (UTC) |
| (AQ+ERA5) | S5P, MAIAC | `date` (derived from `hour`) |

### Vai trò từng data source

| Source | Dùng cho gì cụ thể |
|---|---|
| **OpenAQ station** | Target (pm25); node features; spatial gradient N/S/E/W |
| **WeatherAPI** | vis_km (aerosol proxy), condition_code (fog/haze), uv — không duplicate với ERA5 |
| **ERA5 pressure levels** | HYSPLIT 3D trajectory backbone (u/v/w/z) |
| **ERA5 surface** | pbl_height_m thực (thay heuristic); wind_u/v chính xác; surface pressure |
| **GFS forecast** | Forward trajectory real-time (24–384h) khi không có ERA5 reanalysis |
| **S5P NO2/AER** | HN-level column + path sampling dọc trajectory → source fingerprint |
| **MAIAC AOD** | Aerosol loading upwind; aod_no2_ratio → phân biệt nguồn cháy vs công nghiệp |
| **HYSPLIT backward** | cluster_id (loại air mass); source_lat/lon; traj features |
| **HYSPLIT forward** | Probability grid 6h/12h/24h plume dispersion |

### Lưu ý kỹ thuật quan trọng

- **S5P data leakage:** Không broadcast S5P của ngày `t` cho tất cả 24h. Chỉ valid sau giờ overpass UTC (~ 06:30). Dùng ffill từ ngày `t-1` cho các giờ trước overpass.
- **Lag features với gaps:** Reindex theo hourly grid trước khi shift. Nếu thiếu giờ, `shift(1)` cho kết quả sai.
- **HYSPLIT ARL format:** ERA5 NetCDF phải được convert sang ARL trước khi chạy HYSPLIT (`era5_2arl` tool có trong package HYSPLIT).
- **Trajectory clustering:** Chạy với k=3..10, chọn k theo elbow trên WCSS hoặc silhouette score, không hardcode k=6.
- **Train/val split:** Bắt buộc time-based split, không random. Lag features sẽ bị leak nếu dùng random split.
