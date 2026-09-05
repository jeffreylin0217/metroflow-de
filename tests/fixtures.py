"""Tiny synthetic fixtures, intentionally distinct from measured real source data."""
import calendar
import csv
from datetime import date, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from metroflow.ingest import Manifest, schema_contract, sha256


def seed(root, period='2025-01', all_invalid=False):
    root = Path(root)
    m = Manifest(root)
    folder = root/'raw'/'fixtures'
    folder.mkdir(parents=True,exist_ok=True)
    if not m.get('reference','zones'):
        zones = folder/'zones.csv'
        zones.write_text('LocationID,Borough,Zone,service_zone\n1,Manhattan,Example A,Yellow\n2,Queens,Example B,Yellow\n')
        m.register('reference','zones','fixture://zones',zones)
    day = datetime.fromisoformat(period+'-02T12:00:00')
    base = dict(tpep_pickup_datetime=day,tpep_dropoff_datetime=day+timedelta(minutes=15),
                PULocationID=1,DOLocationID=2,passenger_count=1.0,trip_distance=2.5,
                fare_amount=15.0,tip_amount=3.0,total_amount=20.0,payment_type=1)
    rows = [base.copy(),base.copy(),base | {'fare_amount':-10.0},base | {'passenger_count':None},
            base | {'tpep_dropoff_datetime':day-timedelta(minutes=1)},
            base | {'tpep_pickup_datetime':None},base | {'PULocationID':999},
            base | {'trip_distance':-1.0},base | {'total_amount':float('nan')},
            base | {'tpep_pickup_datetime':day-timedelta(days=10)},
            base | {'trip_distance':0.0}]
    if all_invalid:
        rows = [r | {'trip_distance':-1.0} for r in rows]
    trips = folder/(period+'-candidate.parquet')
    pq.write_table(pa.Table.from_pylist(rows),trips)
    target = folder/(sha256(trips)+'.parquet')
    trips.replace(target)
    m.register('tlc',period,'fixture://'+period,target,schema_contract(target))
    weather = folder/(period+'-weather.csv')
    y,month = map(int,period.split('-'))
    with open(weather,'w',newline='') as f:
        w = csv.writer(f)
        w.writerow(['STATION','DATE','PRCP','SNOW','TMIN','TMAX','PRCP_ATTRIBUTES'])
        for d in range(1,calendar.monthrange(y,month)[1]+1):
            w.writerow(['USW00094728',date(y,month,d),0,0,1,9,',,W,'])
    m.register('weather',period,'fixture://weather/'+period,weather)
    m.close()
