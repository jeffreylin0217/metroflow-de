import json

import duckdb
import pytest

from metroflow import pipeline
from metroflow.ingest import sha256
from tests.fixtures import seed


def snapshot(root):
    with duckdb.connect(str(root/'warehouse.duckdb'),read_only=True) as c:
        return {n:c.execute(f'select * from {n} order by all').fetchall()
                for n in ['fct_trips',*pipeline.EXPORTS]}


def test_lifecycle(tmp_path,monkeypatch):
    seed(tmp_path)
    first=pipeline.run('2025-01','2025-01',tmp_path,offline=True)
    one=snapshot(tmp_path)
    assert first['input_rows']==11 and first['gold_rows']['fct_trips']==5
    assert first['invalid_rows']==6 and first['quality_warnings']==3
    second=pipeline.run('2025-01','2025-01',tmp_path,offline=True)
    assert second['dbt_skipped'] and second['partitions_processed']==[]
    assert snapshot(tmp_path)==one
    seed(tmp_path,'2025-02')
    added=pipeline.run('2025-01','2025-02',tmp_path,offline=True)
    assert added['partitions_processed']==['2025-02']
    two=snapshot(tmp_path)
    assert added['gold_rows']['fct_trips']==10
    forced=pipeline.run('2025-01','2025-02',tmp_path,force_month=['2025-01'],offline=True)
    assert forced['partitions_processed']==['2025-01']
    assert snapshot(tmp_path)==two
    pipeline.run('2025-01','2025-02',tmp_path,full_refresh=True,offline=True)
    assert snapshot(tmp_path)==two
    schemas=json.loads((tmp_path/'reporting/schemas.json').read_text())
    assert [x['name'] for x in schemas['mart_weather_demand']][:3]==['date','trip_count','total_amount']
    assert len(list((tmp_path/'reporting').glob('*.csv')))==6
    assert len(list((tmp_path/'runs').glob('*.json')))==5
    # A failed candidate never replaces the published warehouse/state/reporting pointer.
    digest=sha256(tmp_path/'warehouse.duckdb')
    old_state=(tmp_path/'state.json').read_text()
    old_link=(tmp_path/'reporting').readlink()
    original=pipeline.export_reporting
    def fail(*args): raise RuntimeError('injected export failure')
    monkeypatch.setattr(pipeline,'export_reporting',fail)
    with pytest.raises(RuntimeError,match='injected'):
        pipeline.run('2025-01','2025-02',tmp_path,force_month=['2025-01'],offline=True)
    assert sha256(tmp_path/'warehouse.duckdb')==digest
    assert (tmp_path/'state.json').read_text()==old_state
    assert (tmp_path/'reporting').readlink()==old_link
    failed=[json.loads(p.read_text()) for p in (tmp_path/'runs').glob('*.json')]
    assert any(r['status']=='failed' and r['error']['stage']=='export_reporting' for r in failed)
    monkeypatch.setattr(pipeline,'export_reporting',original)
    # Corrected source can make an existing partition entirely invalid. Old facts must vanish.
    seed(tmp_path,'2025-01',all_invalid=True)
    recovered=pipeline.run('2025-01','2025-02',tmp_path,offline=True)
    assert recovered['gold_rows']['fct_trips']==5
    with duckdb.connect(str(tmp_path/'warehouse.duckdb'),read_only=True) as c:
        assert c.execute("select count(*) from fct_trips where source_period='2025-01'").fetchone()[0]==0


def test_offline_failure_metadata(tmp_path):
    with pytest.raises(ValueError,match='offline source missing'):
        pipeline.run('2025-01','2025-01',tmp_path,offline=True)
    r=json.loads(next((tmp_path/'runs').glob('*.json')).read_text())
    assert r['error']['stage']=='ingestion' and r['status']=='failed'


def test_force_range(tmp_path):
    with pytest.raises(ValueError,match='inside requested'):
        pipeline.run('2025-01','2025-01',tmp_path,force_month=['2025-02'])
