import json
import math
import requests
import time

#properties = ["ADDRESS", "DIRPFX", "STNUM", "STNAME", "TYPESFX"]
properties = []

if properties:
  outFields = ",".join(properties)
else:
  outFields = "*"

count = requests.get(
    "https://mapsdev.hamiltontn.gov/hcwa03/rest/services/Live_Parcels/MapServer/1/query?where=1%3D1&text=&objectIds=&time=&geometry=&geometryType=esriGeometryEnvelope&inSR=&spatialRel=esriSpatialRelIntersects&relationParam=&outFields=&returnGeometry=true&returnTrueCurves=false&maxAllowableOffset=&geometryPrecision=&outSR=&having=&returnIdsOnly=true&returnCountOnly=true&orderByFields=&groupByFieldsForStatistics=&outStatistics=&returnZ=false&returnM=false&gdbVersion=&historicMoment=&returnDistinctValues=false&resultOffset=&resultRecordCount=&queryByDistance=&returnExtentOnly=false&datumTransformation=&parameterValues=&rangeValues=&quantizationParameters=&f=json"
).json()["count"]

num_pages = math.ceil(count / 1000)
print("number of pages:", num_pages)

def trim_coord(coord, num_digits=7):
    lon = str(coord[0])
    lat = str(coord[1])
    if "." in lon and len(lon.split(".")[1]) > num_digits:
        coord[0] = float(lon[: lon.index(".") + num_digits + 1])
    if "." in lat and len(lat.split(".")[1]) > num_digits:
        coord[1] = float(lat[: lat.index(".") + num_digits + 1])

features = []
for i in range(num_pages):
    print("i:", i)
    time.sleep(5)
    url = f"https://mapsdev.hamiltontn.gov/hcwa03/rest/services/Live_Parcels/MapServer/1/query?where=1%3D1&geometryType=esriGeometryEnvelope&spatialRel=esriSpatialRelIntersects&outFields={outFields}&resultOffset={(i * 1000)}&returnGeometry=true&returnTrueCurves=false&returnIdsOnly=false&returnCountOnly=false&returnZ=false&returnM=false&returnDistinctValues=false&returnExtentOnly=false&f=geojson"
    for feature in requests.get(url).json()["features"]:
        geometry = feature["geometry"]
        if geometry["type"] == "Polygon":
            for ring in geometry["coordinates"]:
                for coord in ring:
                    trim_coord(coord)
        elif geometry["type"] == "MultiPolygon":
            for polygon in geometry["coordinates"]:
                for ring in polygon:
                    for coord in ring:
                        trim_coord(coord)

        features.append(feature)

feature_collection = {"type": "FeatureCollection", "features": features}

with open("./live_parcels.geojson", "w") as f:
    json.dump(feature_collection, f)
