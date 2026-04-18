import requests
import geopandas as gpd

adm1_meta = requests.get("https://www.geoboundaries.org/api/current/gbOpen/VNM/ADM1/", timeout=60).json()
adm2_meta = requests.get("https://www.geoboundaries.org/api/current/gbOpen/VNM/ADM2/", timeout=60).json()

adm1 = gpd.read_file(adm1_meta["gjDownloadURL"]).to_crs(epsg=4326)
adm2 = gpd.read_file(adm2_meta["gjDownloadURL"]).to_crs(epsg=4326)

# kiểm tra cột thật
print("ADM1 columns:", adm1.columns.tolist())
print("ADM2 columns:", adm2.columns.tolist())

# thường là shapeName
hanoi = adm1[adm1["shapeName"].str.contains("Ha Noi|Hanoi|Hà Nội", case=False, na=False)].copy()

hanoi_adm2 = gpd.sjoin(adm2, hanoi[["shapeName", "geometry"]], how="inner", predicate="intersects")

print("Joined columns:", hanoi_adm2.columns.tolist())

# xác định cột tên quận/huyện
district_col = None
for c in ["shapeName_left", "shapeName", "shapeNam_1", "shapeName_"]:
    if c in hanoi_adm2.columns:
        district_col = c
        break

if district_col is None:
    raise ValueError("Không tìm thấy cột tên quận/huyện. Hãy xem lại Joined columns.")

# xác định cột tên tỉnh
province_col = None
for c in ["shapeName_right", "shapeName_1"]:
    if c in hanoi_adm2.columns:
        province_col = c
        break

# đổi tên cột ngắn gọn trước khi export
rename_map = {district_col: "district"}
if province_col:
    rename_map[province_col] = "province"

hanoi_adm2 = hanoi_adm2.rename(columns=rename_map)

keep = [c for c in ["district", "province", "shapeISO", "shapeID", "shapeType"] if c in hanoi_adm2.columns]
hanoi_adm2 = hanoi_adm2[keep + ["geometry"]].drop_duplicates()

print(hanoi_adm2[["district"]].head(20))
print("Số đơn vị:", len(hanoi_adm2))
hanoi_adm2["district"] = hanoi_adm2["district"].astype(str).str.strip()
hanoi_adm2["province"] = hanoi_adm2["province"].astype(str).str.strip()
hanoi_adm2 = hanoi_adm2.drop_duplicates(subset=["district"])
hanoi_adm2.to_file("hanoi_districts_clean.geojson", driver="GeoJSON")

hanoi_adm2.to_file("hanoi_districts.geojson", driver="GeoJSON")
hanoi_adm2.to_file("hanoi_districts_shp", driver="ESRI Shapefile")