# live-parcels
Export of Hamilton County "Live_Parcels" Dataset

# direct download links
- gzipped csv: https://raw.githubusercontent.com/officeofperformancemanagement/live-parcels/refs/heads/main/live_parcels.csv.gz
- zipped geojson: https://raw.githubusercontent.com/officeofperformancemanagement/live-parcels/refs/heads/main/live_parcels.geojson.zip
- geoparquet: https://raw.githubusercontent.com/officeofperformancemanagement/live-parcels/refs/heads/main/live_parcels.parquet

# query
https://shell.duckdb.org/#queries=v0,CREATE-VIEW-parcels-as-SELECT-*-FROM-%22https%3A%2F%2Fraw.githubusercontent.com%2Fofficeofperformancemanagement%2Flive%20parcels%2Frefs%2Fheads%2Fmain%2Flive_parcels.parquet%22~,DESCRIBE-parcels~
