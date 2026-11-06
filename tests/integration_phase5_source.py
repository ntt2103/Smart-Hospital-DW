"""Phase 5 COPY/resume/SQL DQ test in its own database, never hospital_oltp."""
import json
import os
from pathlib import Path

import psycopg
from psycopg import sql

from data_sources.generator.phase5 import Config, write_dataset
from data_sources.loader import load_dataset
from data_quality.source_checks import validate_database
from operations.foundation import ROOT, connect, migration


def run():
    dbname='hospital_phase5_source_copy_smoke'
    with connect('oltp','admin') as admin:
        admin.autocommit=True
        if not admin.execute('SELECT 1 FROM pg_database WHERE datname=%s',(dbname,)).fetchone():
            admin.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(dbname)))
    directory=ROOT/'artifacts/generated/phase5_source_copy_smoke'
    manifest=write_dataset(directory,Config(encounters_per_day=30,horizon_days=30),chunk_size=113)
    with psycopg.connect(host='postgres-oltp',dbname=dbname,user='postgres',password=os.environ['OLTP_ADMIN_PASSWORD'],options='-c timezone=UTC') as conn:
        migration(conn,'001_source',ROOT/'sql/design/source_schema_draft.sql')
        migration(conn,'005_source_relations',ROOT/'sql/migrations/005_source_relations.sql')
        first=load_dataset(conn,directory,isolated_bench=True)
        second=load_dataset(conn,directory,isolated_bench=True)
        assert second['loaded_rows']==0 and second['resumed_rows']==manifest['oltp_total_rows']
        assert not validate_database(conn,manifest['as_of'])
        denied={}
        for name,query in {
            'physical_delete':'DELETE FROM public.encounter_diagnoses WHERE association_id=1',
            'immutable_created_at':"UPDATE public.encounter_diagnoses SET created_at=created_at-interval '1 day' WHERE association_id=1",
            'primary_doctor_mismatch':'UPDATE public.encounter_doctors SET doctor_id=doctor_id%600+1 WHERE association_id=1',
            'participation_outside_encounter':"UPDATE public.encounter_doctors SET joined_at=joined_at-interval '100 days' WHERE association_id=1",
        }.items():
            try:
                with conn.transaction(): conn.execute(query)
            except psycopg.Error:
                denied[name]=True
            else:
                raise AssertionError(f'Mutation guard missing: {name}')
        # Prove full SQL DQ evaluates semantic errors beyond local table constraints.
        with conn.transaction(force_rollback=True):
            conn.execute("UPDATE public.encounter_diagnoses SET is_primary=false WHERE association_id=1")
            assert 'DIAGNOSIS_PRIMARY' in {i['rule'] for i in validate_database(conn,manifest['as_of'])}
        result={'completed':True,'database':dbname,'first_load':first,'resume':second,'mutation_guards':denied,'full_sql_dq_negative_case':True}
    path=ROOT/'artifacts/phase5/source_smoke.json'
    path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(result,indent=2),encoding='utf-8')
    print(json.dumps({'completed':True,'evidence':str(path)}))


if __name__=='__main__': run()
