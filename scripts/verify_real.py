"""Measure rerun/backfill/full-refresh equivalence on already downloaded real data."""
import json
import math
import platform
import sqlite3
from pathlib import Path

import duckdb

from metroflow.ingest import atomic_json
from metroflow.pipeline import EXPORTS, run

ROOT=Path('data').resolve()


def snapshot():
    with duckdb.connect(str(ROOT/'warehouse.duckdb'),read_only=True) as c:
        c.execute("set memory_limit='3GB'")
        columns=[r[0] for r in c.execute('describe fct_trips').fetchall()]
        # Order-independent, bounded-memory content fingerprints; fixture test compares exact rows.
        facts=c.execute('select source_period,count(*),sum(hash('+','.join(columns)+'))::varchar '
                        'from fct_trips group by 1 order by 1').fetchall()
        marts={}
        for name in EXPORTS:
            values=c.execute(f'select * from {name} order by all').fetchall()
            marts[name]=values
        return {'facts':facts,'marts':marts}


def equivalent(before,after):
    assert before['facts']==after['facts'], 'Fact partition content changed'
    max_delta=0.0
    for name,rows in before['marts'].items():
        other=after['marts'][name]
        assert len(rows)==len(other), name
        for left,right in zip(rows,other):
            for a,b in zip(left,right):
                if isinstance(a,float) and isinstance(b,float):
                    assert math.isclose(a,b,rel_tol=1e-12,abs_tol=1e-9), (name,a,b)
                    max_delta=max(max_delta,abs(a-b))
                else:
                    assert a==b, (name,a,b)
    return max_delta


def main():
    runs=[json.loads(p.read_text()) for p in (ROOT/'runs').glob('*.json')]
    initial=min((r for r in runs if r['status']=='success' and len(r['periods_requested'])==6 and r['files_downloaded']>0),key=lambda r:r['start_time'])
    before=snapshot()
    rerun=run('2025-01','2025-06',ROOT,offline=True)
    assert rerun.get('dbt_skipped') and rerun['partitions_processed']==[]
    equivalent(before,snapshot())
    forced=run('2025-01','2025-06',ROOT,force_month=['2025-03'],offline=True)
    assert forced['partitions_processed']==['2025-03']
    backfill_delta=equivalent(before,snapshot())
    refresh=run('2025-01','2025-06',ROOT,full_refresh=True,offline=True)
    refresh_delta=equivalent(before,snapshot())
    def size(folder): return sum(p.stat().st_size for p in folder.rglob('*') if p.is_file())
    with sqlite3.connect(ROOT/'manifest.sqlite') as c:
        c.row_factory=sqlite3.Row
        sources=[dict(r) for r in c.execute('select * from sources order by source_name,logical_period')]
    result={'platform':platform.platform(),'python':platform.python_version(),'duckdb':duckdb.__version__,
            'first_six_month_run':initial,'unchanged_rerun':rerun,'forced_march_backfill':forced,
            'full_refresh':refresh,'equivalence':'passed',
            'verification_method':'Exact fixture row equality; real per-partition count/hash-sum plus every sorted mart cell compared exactly except FLOAT values (relative tolerance 1e-12, absolute 1e-9). Hash-sums are bounded-memory evidence, not mathematical proof of equality.',
            'fact_content_fingerprints':before['facts'],'max_mart_float_delta':{'backfill':backfill_delta,'full_refresh':refresh_delta},'sources':sources,
            'storage_bytes':{'raw':size(ROOT/'raw'),'bronze':size(ROOT/'bronze'),
                             'warehouse':(ROOT/'warehouse.duckdb').stat().st_size,
                             'current_reporting':size((ROOT/'reporting').resolve())}}
    atomic_json('docs/measurements/real_validation.json',result)
    print(json.dumps(result['storage_bytes']))


if __name__=='__main__': main()
