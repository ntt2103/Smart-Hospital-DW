"""Run the real warehouse SQL against independent hand-calculated fixtures.

Uses a dedicated hospital_fixture database; never truncates the demo warehouse.
"""
import json
import os
import uuid
from datetime import datetime,timezone
import psycopg
from psycopg import sql
from operations.foundation import ROOT,connect,migration
from data_sources.generator.historical import TABLE_KEYS

AGG={
 'OP01':('mart.outpatient_daily','sum(visit_count)'),
 'OP02':('mart.outpatient_daily','sum(waiting_minutes_sum)/nullif(sum(waiting_valid_count),0)'),
 'OP03':('mart.appointment_daily','100.0*sum(no_show_count)/nullif(sum(classified_count),0)'),
 'IP01':('mart.inpatient_admission_daily','sum(admission_count)'),
 'IP03':('mart.inpatient_discharge_daily','sum(los_days_sum)/nullif(sum(los_valid_count),0)'),
 'LAB01':('mart.lab_daily','sum(completed_orders)'),
 'LAB02':('mart.lab_daily','sum(tat_minutes_sum)/nullif(sum(tat_valid_count),0)'),
 'LAB03':('mart.lab_daily','100.0*sum(ontime_orders)/nullif(sum(ontime_valid_count),0)'),
}


def ensure_fixture():
    with connect('dwh','admin') as admin:
        admin.autocommit=True
        if not admin.execute("SELECT 1 FROM pg_database WHERE datname='hospital_fixture'").fetchone():
            admin.execute('CREATE DATABASE hospital_fixture')
        definitions={}
        for name in TABLE_KEYS:
            definitions[name]=admin.execute("SELECT attname,format_type(atttypid,atttypmod) FROM pg_attribute WHERE attrelid=%s::regclass AND attnum>0 AND NOT attisdropped ORDER BY attnum",('stg.'+name,)).fetchall()
    conn=psycopg.connect(host='postgres-dwh',dbname='hospital_fixture',user='postgres',password=os.environ['DWH_ADMIN_PASSWORD'],autocommit=True)
    # DDL files are applied once; the main demo database is not touched.
    if not conn.execute("SELECT to_regclass('ops.pipeline_runs')").fetchone()[0]:
        migration(conn,'001_ops',ROOT/'sql/design/ops_schema_draft.sql')
    if not conn.execute("SELECT to_regclass('raw.events')").fetchone()[0]:
        migration(conn,'002_raw',ROOT/'sql/design/ingestion_schema_draft.sql')
    if not conn.execute("SELECT to_regclass('dw.dim_date')").fetchone()[0]:
        migration(conn,'003_warehouse',ROOT/'sql/design/warehouse_schema_draft.sql')
        migration(conn,'004_kpis',ROOT/'sql/design/kpi_queries.sql')
    if not conn.execute("SELECT to_regprocedure('dw.merge_current(regclass,regclass,text[])')").fetchone()[0]:
        migration(conn,'005_batch_helpers',ROOT/'sql/warehouse/ddl/batch_helpers.sql')
    migration(conn,'007_phase5_warehouse',ROOT/'sql/migrations/007_phase5_warehouse.sql')
    migration(conn,'006_streaming_views',ROOT/'sql/warehouse/ddl/streaming_helpers.sql')
    migration(conn,'009_phase5_canonical_rt',ROOT/'sql/migrations/009_phase5_canonical_rt.sql')
    for name,columns in definitions.items():
        definition=sql.SQL(',').join(sql.SQL('{} {}').format(sql.Identifier(c),sql.SQL(t)) for c,t in columns)
        conn.execute(sql.SQL('CREATE TABLE IF NOT EXISTS stg.{} ({})').format(sql.Identifier(name),definition))
    migration(conn,'008_phase5_staging_indexes',ROOT/'sql/migrations/008_phase5_staging_indexes.sql')
    return conn,definitions


def run():
    conn,definitions=ensure_fixture()
    fixtures=json.loads((ROOT/'tests/fixtures/phase1_cases.json').read_text())
    checks=[]
    for case in fixtures['cases']:
        expected={k:v for k,v in case['expected'].items() if k in AGG or k=='IP02'}
        if not expected: continue
        conn.execute('BEGIN')
        try:
            for name,columns in definitions.items():
                for i,row in enumerate(case['input'].get(name,fixtures['base'].get(name,[])),1):
                    values={c:None for c,t in columns}
                    values.update(created_at='2020-01-01T00:00:00Z',updated_at=fixtures['defaults']['as_of'],is_active=True)
                    values.update(row)
                    names=[c for c,t in columns]
                    conn.execute(sql.SQL('INSERT INTO stg.{} ({}) VALUES ({})').format(sql.Identifier(name),sql.SQL(',').join(map(sql.Identifier,names)),sql.SQL(',').join(sql.Placeholder() for _ in names)),[values[c] for c in names])
            conn.execute('CREATE TEMP TABLE run_context(run_id UUID,as_of TIMESTAMPTZ,observed_at TIMESTAMPTZ,snapshot_start DATE,snapshot_end DATE) ON COMMIT DROP')
            conn.execute('INSERT INTO run_context(run_id,as_of,observed_at) VALUES (%s,%s,%s)',(uuid.uuid4(),case.get('as_of',fixtures['defaults']['as_of']),fixtures['defaults']['as_of']))
            conn.execute((ROOT/'sql/warehouse/transformations/batch.sql').read_text())
            for kpi,want in expected.items():
                if kpi=='IP02':
                    snapshot=case.get('snapshot_at',fixtures['defaults']['snapshot_at'])
                    got=conn.execute("SELECT CASE WHEN sum(invalid_snapshot_count)=0 THEN 100.0*sum(occupied_beds)/nullif(sum(active_capacity),0) END FROM mart.bed_occupancy_daily WHERE report_date=(%s::timestamptz AT TIME ZONE 'Asia/Ho_Chi_Minh')::date",(snapshot,)).fetchone()[0]
                else:
                    view,expression=AGG[kpi]
                    suffix=' WHERE report_date=%s' if case.get('report_date') else ''
                    got=conn.execute(f'SELECT {expression} FROM {view}'+suffix,(case['report_date'],) if suffix else None).fetchone()[0]
                    if got is None and kpi in ['OP01','IP01','LAB01']: got=0
                if want is None: assert got is None,(case['id'],kpi,got,want)
                else: assert got is not None and abs(float(got)-want)<1e-8,(case['id'],kpi,got,want)
                checks.append({'case':case['id'],'kpi':kpi,'expected':want,'actual':float(got) if got is not None else None})
        finally:
            conn.execute('ROLLBACK')
    conn.close()
    coverage=sorted({r['kpi'] for r in checks})
    assert len(coverage)==9,coverage
    out=ROOT/'artifacts/phase5/sql_fixtures.json'
    out.parent.mkdir(parents=True,exist_ok=True)
    out.write_text(json.dumps({'passed':len(checks),'failed':0,'kpi_coverage':coverage,'checks':checks},indent=2),encoding='utf-8')
    print(json.dumps({'sql_fixture_assertions_passed':len(checks),'kpi_coverage':coverage}))


if __name__=='__main__': run()
