import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from metroflow.bronze import build_trips, build_weather, build_zones
from metroflow.ingest import Manifest, schema_contract, sha256
from metroflow.pipeline import periods_between, writer_lock
from tests.fixtures import seed


@pytest.fixture
def source(tmp_path):
    seed(tmp_path)
    m=Manifest(tmp_path)
    row=m.get('tlc','2025-01')
    m.close()
    return tmp_path/row['local_path']


@pytest.mark.parametrize('column',['trip_distance','PULocationID','tpep_pickup_datetime','total_amount'])
def test_missing_required(source,tmp_path,column):
    t=pq.read_table(source).drop([column]); p=tmp_path/'bad.parquet'; pq.write_table(t,p)
    with pytest.raises(ValueError,match='required field missing'):
        schema_contract(p)


@pytest.mark.parametrize('column',['trip_distance','tpep_pickup_datetime'])
def test_incompatible_type(source,tmp_path,column):
    t=pq.read_table(source); t=t.set_column(t.schema.get_field_index(column),column,pa.array(['bad']*len(t)))
    p=tmp_path/'bad.parquet'; pq.write_table(t,p)
    with pytest.raises(ValueError,match='incompatible'):
        schema_contract(p)


def test_optional_schema_change(source,tmp_path):
    before=schema_contract(source)
    t=pq.read_table(source).append_column('cbd_congestion_fee',pa.array([2.5]*11))
    p=tmp_path/'optional.parquet'; pq.write_table(t,p)
    after=schema_contract(p)
    assert before['source_row_count']==after['source_row_count']==11
    assert before['schema_fingerprint']!=after['schema_fingerprint']


def test_bronze_row_identity(source,tmp_path):
    target=tmp_path/'bronze.parquet'; build_trips(source,target,'2025-01',sha256(source))
    t=pq.read_table(target)
    assert t['source_row'].to_pylist()==list(range(11))
    assert set(t['source_sha256'].to_pylist())=={sha256(source)}


def test_manifest_skip_and_tamper(source,tmp_path):
    m=Manifest(tmp_path)
    row,downloaded=m.fetch('tlc','2025-01','https://unused.invalid')
    assert not downloaded
    assert row['source_row_count']==11
    with open(source,'ab') as f: f.write(b'changed')
    with pytest.raises(ValueError,match='integrity'):
        m.fetch('tlc','2025-01','https://unused.invalid')
    m.close()


def test_duplicate_registration(source,tmp_path):
    m=Manifest(tmp_path)
    m.register('tlc','2025-01','fixture://same',source,schema_contract(source))
    assert m.db.execute("select count(*) from sources where source_name='tlc'").fetchone()[0]==1
    assert m.db.execute("select count(*) from history where source_name='tlc'").fetchone()[0]==1
    m.close()


def test_zone_uniqueness(tmp_path):
    p=tmp_path/'zones.csv'; p.write_text('LocationID,Borough,Zone,service_zone\n1,A,A,A\n1,B,B,B\n')
    with pytest.raises(ValueError,match='duplicate'):
        build_zones(p,tmp_path/'zones.parquet')


def test_weather_coverage_and_quality(source,tmp_path):
    raw=tmp_path/'raw/fixtures/2025-01-weather.csv'
    raw.write_text(raw.read_text().replace('",,W,"','",X,W,"'))
    target=tmp_path/'weather.parquet'
    build_weather(raw,target,'2025-01')
    assert pq.read_table(target)['precipitation_mm'].null_count==31
    raw.write_text('\n'.join(raw.read_text().splitlines()[:-1]))
    with pytest.raises(ValueError,match='missing or duplicate'):
        build_weather(raw,target,'2025-01')


def test_month_range():
    assert periods_between('2024-12','2025-02')==['2024-12','2025-01','2025-02']
    with pytest.raises(ValueError): periods_between('2025-03','2025-01')
    with pytest.raises(ValueError): periods_between('2025-13','2026-01')


def test_single_writer(tmp_path):
    with writer_lock(tmp_path):
        with pytest.raises(RuntimeError,match='writer'):
            with writer_lock(tmp_path): pass


def test_duplicate_source_across_periods(source,tmp_path):
    m=Manifest(tmp_path)
    with pytest.raises(ValueError,match='duplicate source bytes'):
        m.register('tlc','2025-02','fixture://duplicate',source,schema_contract(source))
    m.close()


class FakeResponse:
    def __init__(self,content,fail=False): self.content,self.fail=content,fail
    def __enter__(self): return self
    def __exit__(self,*args): pass
    def raise_for_status(self):
        if self.fail: raise RuntimeError('HTTP fixture failure')
    def iter_content(self,size): yield self.content


class FakeSession:
    def __init__(self,response): self.response=response
    def mount(self,*args): pass
    def get(self,*args,**kwargs): return self.response


def test_remote_unchanged_and_corrected_versions(source,tmp_path,monkeypatch):
    import metroflow.ingest as ingest
    m=Manifest(tmp_path)
    old=m.get('tlc','2025-01')
    monkeypatch.setattr(ingest.requests,'Session',lambda:FakeSession(FakeResponse(source.read_bytes())))
    same,changed=m.fetch('tlc','2025-01','https://fixture.invalid',check_remote=True)
    assert not changed and same==old
    table=pq.read_table(source).append_column('new_optional',pa.array([1]*11))
    new=tmp_path/'corrected.parquet';pq.write_table(table,new)
    monkeypatch.setattr(ingest.requests,'Session',lambda:FakeSession(FakeResponse(new.read_bytes())))
    corrected,changed=m.fetch('tlc','2025-01','https://fixture.invalid',check_remote=True)
    assert changed and corrected['sha256']!=old['sha256']
    assert sha256(source)==old['sha256']
    assert m.db.execute("select count(*) from history where source_name='tlc'").fetchone()[0]==2
    m.close()


def test_failed_download_preserves_manifest(source,tmp_path,monkeypatch):
    import metroflow.ingest as ingest
    m=Manifest(tmp_path);old=m.get('tlc','2025-01')
    monkeypatch.setattr(ingest.requests,'Session',lambda:FakeSession(FakeResponse(b'',fail=True)))
    with pytest.raises(RuntimeError,match='HTTP fixture'):
        m.fetch('tlc','2025-01','https://fixture.invalid',check_remote=True)
    assert m.get('tlc','2025-01')==old
    assert not list((tmp_path/'raw').rglob('*.partial'))
    m.close()


def test_invalid_download_not_registered(source,tmp_path,monkeypatch):
    import metroflow.ingest as ingest
    bad=tmp_path/'invalid.parquet';pq.write_table(pa.table({'wrong':[1]}),bad)
    monkeypatch.setattr(ingest.requests,'Session',lambda:FakeSession(FakeResponse(bad.read_bytes())))
    m=Manifest(tmp_path)
    with pytest.raises(ValueError,match='required field'):
        m.fetch('tlc','2025-02','https://fixture.invalid')
    assert m.get('tlc','2025-02') is None
    assert not list((tmp_path/'raw').rglob('*.partial'))
    m.close()
