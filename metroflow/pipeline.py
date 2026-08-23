"""One local writer: ingest -> Bronze -> dbt -> tested warehouse -> reporting."""
from __future__ import annotations

import argparse
import calendar
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
import uuid
from contextlib import contextmanager
from datetime import date

import duckdb

from metroflow.bronze import build_trips, build_weather, build_zones, weather_url
from metroflow.ingest import Manifest, atomic_json, now, sha256

PROJECT = Path(__file__).resolve().parents[1]
EXPORTS = ['dim_date', 'dim_zone', 'mart_zone_daily', 'mart_borough_daily', 'mart_weather_demand', 'mart_quality']


def periods_between(start, end):
    first, last = date.fromisoformat(start+'-01'), date.fromisoformat(end+'-01')
    if first > last:
        raise ValueError('start must precede end')
    result = []
    while first <= last:
        result.append(first.strftime('%Y-%m'))
        first = date(first.year + (first.month == 12), first.month % 12 + 1, 1)
    return result


def sql_literal(value):
    return "'" + str(value).replace("'", "''") + "'"


@contextmanager
def writer_lock(root):
    root.mkdir(parents=True, exist_ok=True)
    with open(root/'pipeline.lock','a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError('another MetroFlow writer is active') from exc
        try:
            yield
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def code_fingerprint():
    files = sorted([*PROJECT.joinpath('metroflow').glob('*.py'),
                    *PROJECT.joinpath('dbt').rglob('*.sql'), *PROJECT.joinpath('dbt/models').rglob('*.yml'),
                    PROJECT/'dbt/dbt_project.yml', PROJECT/'dbt/profiles.yml', PROJECT/'requirements.lock'])
    # Exclude generated compiled dbt SQL, which lives in target.
    files = [p for p in files if 'target' not in p.parts]
    return hashlib.sha256(''.join(str(p.relative_to(PROJECT))+sha256(p) for p in files).encode()).hexdigest()


def dbt_command(root, command, changed, full_refresh=False, database=None, log_path=None):
    env = os.environ.copy()
    env['METROFLOW_DB'] = str(database or root/'warehouse.duckdb')
    env['DBT_SEND_ANONYMOUS_USAGE_STATS'] = 'false'
    args = [str(Path(sys.executable).with_name('dbt')), command,
            '--project-dir', str(PROJECT/'dbt'), '--profiles-dir', str(PROJECT/'dbt'),
            '--target-path', str(root/'dbt_target'), '--log-path', str(root/'dbt_logs'),
            '--vars', json.dumps({'changed_periods': changed})]
    if full_refresh and command == 'build':
        args.append('--full-refresh')
    with open(log_path or root/f'dbt_{command}.log','w') as log:
        result = subprocess.run(args, env=env, stdout=log, stderr=subprocess.STDOUT)
    if result.returncode:
        raise RuntimeError(f'dbt {command} failed; see {log_path or root/f"dbt_{command}.log"}')


def attach_bronze(database, root):
    with duckdb.connect(str(database)) as c:
        c.execute('create schema if not exists bronze')
        for name, pattern in [('trips','tlc/year=*/month=*/*.parquet'),('zones','reference/*.parquet'),
                              ('weather','weather/*.parquet')]:
            c.execute(f'create or replace view bronze.{name} as select * from read_parquet('
                      f'{sql_literal(root/"bronze"/pattern)}, union_by_name=true, hive_partitioning=false)')


def export_reporting(database, folder):
    folder.mkdir(parents=True, exist_ok=True)
    counts, schemas = {}, {}
    with duckdb.connect(str(database), read_only=True) as c:
        for name in EXPORTS:
            counts[name] = c.execute(f'select count(*) from {name}').fetchone()[0]
            schemas[name] = [{'name': row[0], 'type': row[1]} for row in c.execute(f'describe {name}').fetchall()]
            for ext, options in [('csv','FORMAT CSV, HEADER'),('parquet','FORMAT PARQUET, COMPRESSION ZSTD')]:
                c.execute(f'copy (select * from {name} order by 1,2) to {sql_literal(folder/(name+"."+ext))} ({options})')
        counts['fct_trips'] = c.execute('select count(*) from fct_trips').fetchone()[0]
        quality = [dict(zip([x[0] for x in c.description],r)) for r in c.execute('select * from mart_quality order by 1,2').fetchall()]
    atomic_json(folder/'schemas.json',schemas)
    atomic_json(folder/'quality.json',quality)
    return counts, quality


def run(start, end, root='data', full_refresh=False, force_month=None, check_remote=False, offline=False):
    root = Path(root).resolve()
    requested = periods_between(start,end)
    force_month = force_month or []
    if not set(force_month) <= set(requested):
        raise ValueError('--force-month must lie inside requested range')
    with writer_lock(root):
        return _run(root, requested, full_refresh, force_month, check_remote, offline)


def _run(root, requested, full_refresh, force_month, check_remote, offline):
    started = time.monotonic()
    run_id = uuid.uuid4().hex
    report = dict(run_id=run_id, start_time=now(), run_mode='full_refresh' if full_refresh else 'incremental',
                  periods_requested=requested, files_downloaded=0, files_skipped=0, status='running')
    stage, source, period = 'initialize', None, None
    ledger = Manifest(root)
    old = json.loads((root/'state.json').read_text()) if (root/'state.json').exists() else {}
    state = json.loads(json.dumps(old))
    fingerprint = code_fingerprint()
    rebuild_all = full_refresh or old.get('code_fingerprint') != fingerprint or not (root/'warehouse.duckdb').exists()
    changed = set()
    try:
        # Include all previously loaded months; a backfill does not accidentally shrink the warehouse.
        periods = sorted(set(requested) | set(old.get('periods',[])))
        state['periods'] = periods
        state.setdefault('inputs',{})
        for source, period in [('reference','zones')] + [(s,p) for p in periods for s in ('tlc','weather')]:
            stage = 'ingestion'
            print(json.dumps({'run_id':run_id,'stage':stage,'source':source,'period':period}),flush=True)
            url = ('https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv' if source=='reference' else
                   f'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{period}.parquet' if source=='tlc' else weather_url(period))
            if offline:
                record = ledger.get(source,period)
                if not record:
                    raise ValueError(f'offline source missing: {source}/{period}')
                if sha256(root/record['local_path']) != record['sha256']:
                    raise ValueError('raw checksum mismatch')
                downloaded = False
            else:
                record, downloaded = ledger.fetch(source,period,url,check_remote=check_remote and period in requested)
            report['files_downloaded' if downloaded else 'files_skipped'] += 1
            key = f'{source}/{period}'
            target = root/'bronze'/ (f'tlc/year={period[:4]}/month={period[5:]}/trips.parquet' if source=='tlc' else
                                     f'weather/{period}.parquet' if source=='weather' else 'reference/zones.parquet')
            previous = old.get('inputs',{}).get(key,{})
            needs_build = (rebuild_all or period in force_month or previous.get('raw_sha256') != record['sha256']
                           or not target.exists() or previous.get('bronze_sha256') != sha256(target))
            if needs_build:
                stage = 'bronze_validation'
                raw = root/record['local_path']
                if source == 'tlc':
                    build_trips(raw,target,period,record['sha256'])
                    changed.add(period)
                elif source == 'weather':
                    count = build_weather(raw,target,period)
                    ledger.db.execute('update sources set source_row_count=? where source_name=? and logical_period=?',[count,source,period])
                    ledger.db.commit()
                else:
                    count = build_zones(raw,target)
                    ledger.db.execute('update sources set source_row_count=? where source_name=? and logical_period=?',[count,source,period])
                    ledger.db.commit()
                    # Changed zone membership can alter validation of every fact partition.
                    changed.update(periods)
                state['inputs'][key] = {'raw_sha256':record['sha256'],'bronze_sha256':sha256(target)}
        if rebuild_all:
            changed.update(periods)
        state['code_fingerprint'] = fingerprint
        report['partitions_processed'] = sorted(changed)
        report['input_rows'] = ledger.db.execute("select sum(source_row_count) from sources where source_name='tlc' and logical_period in (" + ",".join("?" for _ in periods) + ")", periods).fetchone()[0]
        report['bronze_rows'] = report['input_rows']
        if state != old or changed or not (root/'reporting').exists():
            stage, source, period = 'dbt_build', None, None
            candidate = root/'candidate'/'warehouse.duckdb'
            candidate.parent.mkdir(exist_ok=True)
            candidate.unlink(missing_ok=True)
            if (root/'warehouse.duckdb').exists() and not rebuild_all:
                shutil.copy2(root/'warehouse.duckdb',candidate)
            attach_bronze(candidate,root)
            print(json.dumps({'run_id':run_id,'stage':stage,'partitions':sorted(changed)}),flush=True)
            dbt_command(root,'build',sorted(changed),rebuild_all,candidate)
            stage = 'dbt_test'
            dbt_command(root,'test',sorted(changed),database=candidate)
            results = json.loads((root/'dbt_target/run_results.json').read_text())['results']
            report['dbt_tests'] = {status: sum(r['status']==status for r in results) for status in ['pass','warn','fail','error']}
            stage = 'export_reporting'
            folder = root/'reports'/run_id
            counts, quality = export_reporting(candidate,folder)
            atomic_json(folder/'source_manifest.json',[dict(r) for r in ledger.db.execute('select * from sources order by source_name,logical_period')])
            report['gold_rows'] = counts
            report['quality_warnings'] = sum(r['trip_count'] for r in quality if r['quality_status']=='SUSPICIOUS')
            report['invalid_rows'] = sum(r['trip_count'] for r in quality if r['quality_status']=='INVALID')
            # Publish only after all models, tests and exports succeed. Keep previous export generations.
            os.replace(candidate,root/'warehouse.duckdb')
            link = root/'reporting.next'
            link.unlink(missing_ok=True)
            link.symlink_to(Path('reports')/run_id, target_is_directory=True)
            os.replace(link,root/'reporting')
            atomic_json(root/'state.json',state)
        else:
            previous_run = json.loads((root/'latest_run.json').read_text())
            for field in ['gold_rows','quality_warnings','invalid_rows','dbt_tests']:
                report[field] = previous_run[field]
            report['dbt_skipped'] = True
        report['status'] = 'success'
    except Exception as exc:
        report.update(status='failed',error={'stage':stage,'source':source,'period':period,'reason':str(exc)})
        raise
    finally:
        ledger.close()
        report.update(end_time=now(),duration_seconds=round(time.monotonic()-started,3))
        atomic_json(root/'runs'/f'{run_id}.json',report)
        if report['status']=='success':
            atomic_json(root/'latest_run.json',report)
        print(json.dumps(report,default=str),flush=True)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--start',required=True)
    parser.add_argument('--end',required=True)
    parser.add_argument('--data-dir',default='data')
    parser.add_argument('--full-refresh',action='store_true')
    parser.add_argument('--force-month',action='append',default=[])
    parser.add_argument('--check-remote',action='store_true')
    parser.add_argument('--offline',action='store_true',help='Use only registered immutable local files')
    args = parser.parse_args()
    run(args.start,args.end,args.data_dir,args.full_refresh,args.force_month,args.check_remote,args.offline)


if __name__ == '__main__':
    main()
