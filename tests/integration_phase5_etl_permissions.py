"""Real ETL-role batch against admin-owned staging in a separate warehouse.

This covers fresh setup, where GRANT ALL does not confer table ownership and
the application role cannot create indexes on postgres-owned staging tables.
The immutable source COPY fixture is read only; main databases are untouched.
"""
import contextlib
import io
import json
import os

import psycopg
from psycopg import sql

from batch_pipeline.jobs import batch, near_rt
from operations.foundation import ROOT, connect, migration

SOURCE_DB = 'hospital_phase5_source_copy_smoke'
TARGET_DB = 'hospital_phase5_etl_permissions'


def connect_fixture(kind, role):
    user = 'hospital_etl' if kind == 'dwh' and role == 'etl' else 'postgres'
    password_key = 'DWH_ETL_PASSWORD' if user == 'hospital_etl' else kind.upper()+'_ADMIN_PASSWORD'
    return psycopg.connect(host='postgres-'+kind, dbname=SOURCE_DB if kind == 'oltp' else TARGET_DB,
                           user=user, password=os.environ[password_key], options='-c timezone=UTC', connect_timeout=15)


def run():
    with connect('dwh','admin') as admin:
        admin.autocommit = True
        if not admin.execute('SELECT 1 FROM pg_database WHERE datname=%s',(TARGET_DB,)).fetchone():
            admin.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(TARGET_DB)))
        admin.execute(sql.SQL('GRANT CONNECT,TEMPORARY ON DATABASE {} TO hospital_etl').format(sql.Identifier(TARGET_DB)))
    with connect_fixture('dwh','admin') as admin:
        migration(admin,'001_ops',ROOT/'sql/design/ops_schema_draft.sql')
        migration(admin,'002_raw',ROOT/'sql/design/ingestion_schema_draft.sql')
        for schema in ('raw','stg','dw','mart','ops'):
            admin.execute(sql.SQL('CREATE SCHEMA IF NOT EXISTS {}').format(sql.Identifier(schema)))
            admin.execute(sql.SQL('GRANT USAGE,CREATE ON SCHEMA {} TO hospital_etl').format(sql.Identifier(schema)))
    previous_batch,previous_rt = batch.connect,near_rt.connect
    previous_attempt = os.environ.get('BATCH_ATTEMPT')
    batch.connect = near_rt.connect = connect_fixture
    os.environ['BATCH_ATTEMPT'] = 'etl-permissions-fixture'
    try:
        batch.setup()
        near_rt.setup()
        with connect_fixture('dwh','admin') as admin:
            owners = dict(admin.execute("SELECT c.relname,pg_get_userbyid(c.relowner) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='stg' AND c.relkind='r'").fetchall())
            assert len(owners) == 13 and set(owners.values()) == {'postgres'}, owners
            indexes = admin.execute("SELECT indexname,indexdef FROM pg_indexes WHERE schemaname='stg' AND indexname IN ('stage_status_bed_time','stage_assignment_bed_time') ORDER BY indexname").fetchall()
            assert len(indexes) == 2
        with connect_fixture('dwh','etl') as application:
            assert application.execute('SELECT current_user').fetchone()[0] == 'hospital_etl'
            try:
                with application.transaction():
                    application.execute('CREATE INDEX phase5_forbidden_owner_probe ON stg.bed_status_history(bed_id)')
            except psycopg.errors.InsufficientPrivilege:
                owner_ddl_denied = True
            else:
                raise AssertionError('ETL unexpectedly owns administrative index DDL')
        with contextlib.redirect_stdout(io.StringIO()):
            first = batch.execute(mode='initial')
            repeated = batch.execute(mode='incremental')
        assert first['source_counts'] == repeated['source_counts']
        assert first['fact_counts'] == repeated['fact_counts']
        result = {'passed': True, 'source_database':SOURCE_DB,'warehouse_database':TARGET_DB,
                  'main_databases_modified':False,'application_role':'hospital_etl','staging_owners':owners,
                  'administrative_indexes':indexes,'index_ddl_denied_to_etl':owner_ddl_denied,
                  'initial':first,'rerun':repeated}
    finally:
        batch.connect,near_rt.connect = previous_batch,previous_rt
        if previous_attempt is None:
            os.environ.pop('BATCH_ATTEMPT',None)
        else:
            os.environ['BATCH_ATTEMPT'] = previous_attempt
    output = ROOT/'artifacts/phase5/etl_permissions.json'
    output.parent.mkdir(parents=True,exist_ok=True)
    output.write_text(json.dumps(result,indent=2),encoding='utf-8')
    print(json.dumps({'etl_permissions_passed':True,'source_rows':sum(first['source_counts'].values()),'staging_owner':'postgres','application_role':'hospital_etl'}))


if __name__ == '__main__':
    run()
