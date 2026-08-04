"""Immutable downloads and a small SQLite source ledger; no transformation SQL."""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

REQUIRED = {
    'tpep_pickup_datetime': 'timestamp', 'tpep_dropoff_datetime': 'timestamp',
    'PULocationID': 'number', 'DOLocationID': 'number', 'passenger_count': 'number',
    'trip_distance': 'number', 'fare_amount': 'number', 'tip_amount': 'number',
    'total_amount': 'number', 'payment_type': 'number',
}


def now():
    return datetime.now(timezone.utc).isoformat()


def sha256(path):
    with open(path, 'rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix('.tmp')
    tmp.write_text(json.dumps(value, indent=2, default=str) + '\n')
    os.replace(tmp, path)


def schema_contract(path):
    schema = pq.read_schema(path)
    for name, family in REQUIRED.items():
        if name not in schema.names:
            raise ValueError(f'required field missing: {name}')
        typ = schema.field(name).type
        valid = pa.types.is_timestamp(typ) if family == 'timestamp' else (
            pa.types.is_integer(typ) or pa.types.is_floating(typ) or pa.types.is_decimal(typ))
        if not valid:
            raise ValueError(f'incompatible field {name}: {typ}; expected {family}')
    description = [(f.name, str(f.type)) for f in schema]
    return {'schema_fingerprint': hashlib.sha256(json.dumps(description).encode()).hexdigest(),
            'schema': description, 'source_row_count': pq.ParquetFile(path).metadata.num_rows}


class Manifest:
    def __init__(self, root):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(self.root / 'manifest.sqlite')
        self.db.row_factory = sqlite3.Row
        self.db.execute('''create table if not exists sources (
          source_name text, logical_period text, source_url text, local_path text,
          file_size integer, sha256 text, downloaded_at text, source_row_count integer,
          schema_fingerprint text, ingestion_status text,
          primary key(source_name, logical_period))''')
        self.db.execute('''create table if not exists history (
          source_name text, logical_period text, sha256 text, metadata text,
          primary key(source_name, logical_period, sha256))''')

    def get(self, source, period):
        row = self.db.execute('select * from sources where source_name=? and logical_period=?',
                              [source, period]).fetchone()
        return dict(row) if row else None

    def register(self, source, period, url, path, metadata=None):
        metadata = metadata or {}
        digest = sha256(path)
        other = self.db.execute('select logical_period from sources where source_name=? and sha256=? and logical_period<>?', [source,digest,period]).fetchone()
        if other:
            raise ValueError(f'duplicate source bytes already registered for {source}/{other[0]}')
        record = dict(source_name=source, logical_period=period, source_url=url,
                      local_path=str(Path(path).relative_to(self.root)), file_size=Path(path).stat().st_size,
                      sha256=sha256(path), downloaded_at=now(),
                      source_row_count=metadata.get('source_row_count'),
                      schema_fingerprint=metadata.get('schema_fingerprint'), ingestion_status='validated')
        with self.db:
            self.db.execute('insert or replace into sources values (?,?,?,?,?,?,?,?,?,?)', list(record.values()))
            self.db.execute('insert or ignore into history values (?,?,?,?)',
                            [source, period, record['sha256'], json.dumps(record | metadata)])
        return record

    def fetch(self, source, period, url, *, check_remote=False):
        old = self.get(source, period)
        if old:
            path = self.root / old['local_path']
            if not path.exists() or sha256(path) != old['sha256']:
                raise ValueError(f'raw integrity failure: {source}/{period}; restore the immutable file')
            if not check_remote:
                return old, False
        suffix = '.parquet' if source == 'tlc' else '.csv'
        relative = f'tlc/year={period[:4]}/month={period[5:]}' if source == 'tlc' else f'{source}/{period}'
        folder = self.root / 'raw' / relative
        folder.mkdir(parents=True, exist_ok=True)
        fd, name = tempfile.mkstemp(dir=folder, suffix='.partial')
        path = Path(name)
        try:
            session = requests.Session()
            session.mount('https://', HTTPAdapter(max_retries=Retry(total=3, backoff_factor=1,
                          status_forcelist=[429, 500, 502, 503, 504])))
            with os.fdopen(fd, 'wb') as out, session.get(url, stream=True, timeout=(15, 120)) as response:
                response.raise_for_status()
                for chunk in response.iter_content(1024 * 1024):
                    out.write(chunk)
            metadata = schema_contract(path) if source == 'tlc' else {}
            digest = sha256(path)
            target = folder / (digest + suffix)
            if target.exists():
                path.unlink()
            else:
                os.replace(path, target)
            if old and digest == old['sha256']:
                return old, False
            return self.register(source, period, url, target, metadata), True
        finally:
            path.unlink(missing_ok=True)

    def close(self):
        self.db.close()
