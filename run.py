import json
import math
import time
import zipfile

import geopandas as gpd
import requests

#properties = ["ADDRESS", "DIRPFX", "STNUM", "STNAME", "TYPESFX"]
properties = []

if properties:
  outFields = ",".join(properties)
else:
  outFields = "*"

r = requests.get(
    "https://mapsdev.hamiltontn.gov/hcwa03/rest/services/Live_Parcels/MapServer/1/query?where=1%3D1&text=&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&relationParam=&outFields=&returnGeometry=true&returnTrueCurves=false&maxAllowableOffset=&geometryPrecision=&outSR=&having=&returnIdsOnly=true&returnCountOnly=true&orderByFields=&groupByFieldsForStatistics=&outStatistics=&returnZ=false&returnM=false&gdbVersion=&historicMoment=&returnDistinctValues=false&resultOffset=&resultRecordCount=&queryByDistance=&returnExtentOnly=false&datumTransformation=&parameterValues=&rangeValues=&quantizationParameters=&f=json"
).json()

if "count" not in r:
  print(r)
  raise Exception("missing count")

count = r["count"]

num_pages = math.ceil(count / 1000)
print("number of pages:", num_pages)

# inspired by https://github.com/danieljdufour/get-depth
# determines how many nested levels are in a list
# ex: an array of coordinate pairs [(x0, y0), (x1, y1)] has a depth of 2
def get_depth(arr):
    depth = 0
    part = arr
    while isinstance(part, list) or isinstance(part, tuple):
        depth += 1
        part = part[0]
    return depth

# remove unnecessary decimal points
def trim_precision(it, num_digits=7):
    depth = get_depth(it)
    if isinstance(it, float):
        return round(it, num_digits)
    elif depth == 1:
        return [trim_precision(n, num_digits) for n in it]
    elif depth == 2 and len(it) == 2:
        # line segment, so we need to preserve both the starting and end point
        return [trim_precision(n, num_digits) for n in it]
    elif depth == 2:
        results = []
        for i in it:
            i = trim_precision(i, num_digits)
            # don't add the trimmed result if it's the same as the preceding vertex point
            if len(results) == 0 or i != results[-1]:
                results.append(i)
        return results
    else:
        # for polygons or multipolygons, recursively trim the containing rings of coordinates
        return [trim_precision(n, num_digits) for n in it]

features = []
for i in range(num_pages):
    print("i:", i)
    time.sleep(5)
    url = f"https://mapsdev.hamiltontn.gov/hcwa03/rest/services/Live_Parcels/MapServer/1/query?where=1%3D1&geometryType=esriGeometryEnvelope&spatialRel=esriSpatialRelIntersects&outFields={outFields}&resultOffset={(i * 1000)}&returnGeometry=true&returnTrueCurves=false&returnIdsOnly=false&returnCountOnly=false&returnZ=false&returnM=false&returnDistinctValues=false&returnExtentOnly=false&f=geojson"
    for feature in requests.get(url).json()["features"]:
        geometry = feature["geometry"]

        geometry['coordinates'] = trim_precision(geometry["coordinates"])

        features.append(feature)

featureCollection = { "type": "FeatureCollection", "features": features }

# save zipped csv version without debugging for additional compression
with zipfile.ZipFile("live_parcels.geojson.zip", mode="w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zip_file: 
    dumped = json.dumps(featureCollection, ensure_ascii=False)
    zip_file.writestr("live_parcels.geojson", data=dumped)

# save zipped csv version with indentation for debugging
with zipfile.ZipFile("live_parcels.debug.geojson.zip", mode="w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zip_file: 
    dumped = json.dumps(featureCollection, ensure_ascii=False, indent=4)
    zip_file.writestr("live_parcels.debug.geojson", data=dumped)

# save parquet version
gdf = gpd.GeoDataFrame.from_features(features)
gdf = gdf.set_crs(epsg=4326)
gdf.to_parquet("live_parcels.parquet")

# save as gzipped csv
gdf.to_csv("live_parcels.csv.gz", compression='gzip')

# save as shapefile
gdf.to_file("live_parcels.shp")
