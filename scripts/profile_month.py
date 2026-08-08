"""Read-only real-data profiling before choosing analytical quality policies."""
import json
import sys
import duckdb
from metroflow.ingest import Manifest

root = sys.argv[1] if len(sys.argv) > 1 else 'data'
m = Manifest(root)
r, _ = m.fetch('tlc', '2025-01', 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet')
c = duckdb.connect()
c.read_parquet(str(m.root / r['local_path'])).create_view('trips')
print(c.sql('describe trips').fetchall())
q = '''select count(*) source_rows,
 count(*) filter(where tpep_pickup_datetime is null or tpep_dropoff_datetime is null) null_timestamps,
 count(*) filter(where tpep_dropoff_datetime<tpep_pickup_datetime) backwards,
 count(*) filter(where strftime(tpep_pickup_datetime,'%Y-%m')<>'2025-01') outside_month,
 count(*) filter(where trip_distance<0) negative_distance,
 count(*) filter(where fare_amount<0) negative_fare,
 count(*) filter(where passenger_count is null) null_passengers,
 count(*) filter(where trip_distance=0) zero_distance,
 max(date_diff('minute',tpep_pickup_datetime,tpep_dropoff_datetime)) max_duration
 from trips'''
print(json.dumps(dict(zip([d[0] for d in c.execute(q).description],c.fetchone())),indent=2))
