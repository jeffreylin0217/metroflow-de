"""Typed, source-aligned files. Business transformations belong to dbt."""
import calendar
import csv
import os
from datetime import date
from pathlib import Path
from urllib.parse import urlencode

import pyarrow as pa
import pyarrow.parquet as pq

from metroflow.ingest import REQUIRED, schema_contract


def weather_url(period):
    y, m = map(int, period.split('-'))
    return 'https://www.ncei.noaa.gov/access/services/data/v1?' + urlencode({
        'dataset': 'daily-summaries', 'stations': 'USW00094728',
        'startDate': f'{period}-01', 'endDate': f'{period}-{calendar.monthrange(y,m)[1]}',
        'dataTypes': 'PRCP,SNOW,TMIN,TMAX', 'units': 'metric',
        'format': 'csv', 'includeAttributes': 'true', 'includeStationName': 'true'})


def build_trips(raw, target, period, digest):
    info = schema_contract(raw)
    target = Path(target)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix('.partial')
    offset = 0
    fields = [pa.field(name, pa.timestamp('us') if kind == 'timestamp' else
              (pa.int64() if name in ('PULocationID','DOLocationID','payment_type') else pa.float64()))
              for name, kind in REQUIRED.items()]
    schema = pa.schema(fields + [pa.field('source_row', pa.int64()),
                                pa.field('source_period', pa.string()), pa.field('source_sha256', pa.string())])
    try:
        with pq.ParquetWriter(tmp, schema, compression='zstd') as writer:
            for batch in pq.ParquetFile(raw).iter_batches(batch_size=131072, columns=list(REQUIRED)):
                arrays = [batch.column(batch.schema.get_field_index(f.name)).cast(f.type, safe=True) for f in fields]
                arrays += [pa.array(range(offset, offset+batch.num_rows), type=pa.int64()),
                           pa.array([period]*batch.num_rows), pa.array([digest]*batch.num_rows)]
                writer.write_table(pa.Table.from_arrays(arrays, schema=schema))
                offset += batch.num_rows
        os.replace(tmp, target)
    finally:
        tmp.unlink(missing_ok=True)
    return info


def write_table(rows, schema, target):
    target = Path(target)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix('.partial')
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), tmp, compression='zstd')
    os.replace(tmp, target)


def build_zones(raw, target):
    with open(raw, newline='') as f:
        rows = [{'zone_key': int(r['LocationID']), 'borough': r['Borough'],
                 'zone': r['Zone'], 'service_zone': r['service_zone']} for r in csv.DictReader(f)]
    keys = [r['zone_key'] for r in rows]
    if not rows or len(keys) != len(set(keys)):
        raise ValueError('reference: empty or duplicate zone keys')
    write_table(rows, pa.schema([('zone_key',pa.int64()),('borough',pa.string()),
                                 ('zone',pa.string()),('service_zone',pa.string())]), target)
    return len(rows)


def build_weather(raw, target, period):
    rows = []
    with open(raw, newline='') as f:
        for r in csv.DictReader(f):
            if r['STATION'] != 'USW00094728':
                raise ValueError('weather: unexpected station')
            day = date.fromisoformat(r['DATE'])
            if day.strftime('%Y-%m') != period:
                raise ValueError('weather: date outside source period')
            item = {'date': day, 'station': r['STATION'], 'quality_flagged_values': 0}
            for source, field in [('PRCP','precipitation_mm'),('SNOW','snowfall_mm'),
                                   ('TMIN','min_temperature_c'),('TMAX','max_temperature_c')]:
                attributes = r.get(source+'_ATTRIBUTES','').split(',')
                flagged = len(attributes)>1 and bool(attributes[1].strip())
                item['quality_flagged_values'] += int(flagged)
                item[field] = float(r[source]) if r.get(source) and not flagged else None
            rows.append(item)
    days = [r['date'] for r in rows]
    expected = calendar.monthrange(*map(int,period.split('-')))[1]
    if len(days) != expected or len(set(days)) != expected:
        raise ValueError(f'weather: missing or duplicate dates for {period}; expected {expected}')
    write_table(rows, pa.schema([('date',pa.date32()),('station',pa.string()),
                                ('quality_flagged_values',pa.int64()),
                                *[(n,pa.float64()) for n in ['precipitation_mm','snowfall_mm',
                                  'min_temperature_c','max_temperature_c']]]),target)
    return len(rows)
