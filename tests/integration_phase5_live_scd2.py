"""Observe actual OLTP changes and link fresh facts/bridges in separate fixture DBs.

Bootstraps its own source and warehouse databases, never the frozen hospital_oltp.
The source fixture receives three walk-ins and their associations; all deltas
and real source/warehouse timestamps are recorded in the evidence.
"""
import contextlib
import io
import json
import os
import re
import psycopg
from psycopg import sql
from operations.foundation import ROOT, migration
from batch_pipeline.jobs import batch, near_rt
from data_sources.generator.historical import TABLE_KEYS
from data_sources.generator.phase5 import Config, write_dataset
from data_sources.loader import load_dataset


SOURCE_DB = 'hospital_phase5_scd2_source'
TARGET_DB = 'hospital_phase5_scd2_warehouse'


def bootstrap_source():
    """Seed only this proof's database; reruns preserve its valid live tail."""
    with psycopg.connect(host='postgres-oltp', dbname='hospital_oltp', user='postgres',
                         password=os.environ['OLTP_ADMIN_PASSWORD'], autocommit=True) as admin:
        if not admin.execute('SELECT 1 FROM pg_database WHERE datname=%s', (SOURCE_DB,)).fetchone():
            admin.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(SOURCE_DB)))
    directory = ROOT/'artifacts/generated/phase5_scd2_source'
    with contextlib.redirect_stdout(io.StringIO()):
        manifest = write_dataset(directory, Config(encounters_per_day=30, horizon_days=30), chunk_size=113)
    with connect_fixture('oltp', 'admin') as source:
        migration(source, '001_source', ROOT/'sql/design/source_schema_draft.sql')
        migration(source, '005_source_relations', ROOT/'sql/migrations/005_source_relations.sql')
        seeded = source.execute("SELECT to_regclass('_seed.datasets')").fetchone()[0]
        source.commit()
        if seeded is None:
            with contextlib.redirect_stdout(io.StringIO()):
                load_dataset(source, directory, isolated_bench=True)
        else:
            # Live examples from previous or interrupted proof runs belong here.
            # Do not reapply the immutable seed or alter another test's receipts.
            owners = source.execute('SELECT dataset_sha256 FROM _seed.datasets').fetchall()
            assert owners == [(manifest['business_sha256'],)], 'Unexpected fixture dataset owner'
            receipt_rows = source.execute('SELECT coalesce(sum(row_count),0) FROM _seed.chunks WHERE dataset_sha256=%s', (manifest['business_sha256'],)).fetchone()[0]
            assert receipt_rows == manifest['oltp_total_rows'], 'Fixture seed is incomplete'
    return manifest


def connect_fixture(kind, role):
    return psycopg.connect(host=f'postgres-{kind}', dbname=SOURCE_DB if kind == 'oltp' else TARGET_DB,
                           user='postgres', password=os.environ[f'{kind.upper()}_ADMIN_PASSWORD'],
                           options='-c timezone=UTC', connect_timeout=15)


def execute(fail_after_stage=False):
    # The test uses database owners for setup. App-role permissions are verified
    # separately; source FK/CHECK/exclusion/context/update guards stay enabled.
    previous = os.environ.get('BATCH_ATTEMPT')
    os.environ['BATCH_ATTEMPT'] = 'fixture'
    try:
        with contextlib.redirect_stdout(io.StringIO()):
            return batch.execute(fail_after_stage=fail_after_stage)
    finally:
        if previous is None:
            os.environ.pop('BATCH_ATTEMPT', None)
        else:
            os.environ['BATCH_ATTEMPT'] = previous


def counts(connection):
    return {name: connection.execute(sql.SQL('SELECT count(*) FROM public.{}').format(sql.Identifier(name))).fetchone()[0]
            for name in TABLE_KEYS}


def run():
    for name in (SOURCE_DB, TARGET_DB):
        if not re.fullmatch(r'hospital_phase5_[a-z_][a-z_0-9]*', name):
            raise ValueError('Dedicated Phase 5 fixture database names required')
    manifest = bootstrap_source()
    with psycopg.connect(host='postgres-dwh', dbname='hospital_dwh', user='postgres',
                         password=os.environ['DWH_ADMIN_PASSWORD'], autocommit=True) as admin:
        if not admin.execute('SELECT 1 FROM pg_database WHERE datname=%s', (TARGET_DB,)).fetchone():
            admin.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(TARGET_DB)))
    original_batch_connect, original_rt_connect = batch.connect, near_rt.connect
    batch.connect = near_rt.connect = connect_fixture
    try:
        with connect_fixture('dwh', 'admin') as c:
            migration(c, '001_ops', ROOT/'sql/design/ops_schema_draft.sql')
            migration(c, '002_raw', ROOT/'sql/design/ingestion_schema_draft.sql')
        batch.setup()
        near_rt.setup()
        with connect_fixture('oltp', 'admin') as source:
            before = counts(source)
            doctor, department = source.execute('SELECT doctor_id,department_id FROM doctors ORDER BY doctor_id LIMIT 1').fetchone()
            consultant = source.execute('SELECT doctor_id FROM doctors WHERE department_id<>%s ORDER BY doctor_id LIMIT 1', (department,)).fetchone()[0]
            patient = source.execute('SELECT patient_id FROM patients ORDER BY patient_id LIMIT 1').fetchone()[0]
            diagnoses = [row[0] for row in source.execute('SELECT diagnosis_id FROM diagnoses ORDER BY diagnosis_id LIMIT 3')]
            assert len(diagnoses) == 3
            source.execute('UPDATE doctors SET is_active=true WHERE doctor_id=%s AND NOT is_active', (doctor,))
        baseline = execute()
        examples, change_observations, encounters, association_ids = [], [], [], []
        for index, active in enumerate((True, False, True)):
            if index:
                with connect_fixture('oltp', 'admin') as source:
                    changed = source.execute('UPDATE doctors SET is_active=%s WHERE doctor_id=%s RETURNING doctor_id,is_active,updated_at', (active, doctor)).fetchone()
                version_run = execute()
                change_observations.append({'doctor_id': doctor, 'is_active': active, 'source_updated_at': changed[2].isoformat(), 'observed_batch_run': version_run['run_id']})
            with connect_fixture('oltp', 'admin') as source:
                at = source.execute('SELECT clock_timestamp()').fetchone()[0]
                encounter = source.execute("INSERT INTO encounters(patient_id,department_id,doctor_id,encounter_type,status,status_changed_at,checked_in_at,consultation_started_at) VALUES(%s,%s,%s,'OUTPATIENT','IN_CONSULTATION',%s,%s,%s) RETURNING encounter_id", (patient, department, doctor, at, at, at)).fetchone()[0]
                association = source.execute("INSERT INTO encounter_doctors(encounter_id,doctor_id,role,joined_at) VALUES(%s,%s,'PRIMARY',%s) RETURNING association_id", (encounter, doctor, at)).fetchone()[0]
                encounters.append(encounter)
                association_ids.append(association)
                if index == 0:
                    secondary = source.execute("INSERT INTO encounter_doctors(encounter_id,doctor_id,role,joined_at) VALUES(%s,%s,'CONSULTING',%s) RETURNING association_id", (encounter, consultant, at)).fetchone()[0]
                    diagnosis_associations = []
                    for offset, diagnosis in enumerate(diagnoses):
                        diagnosis_associations.append(source.execute('INSERT INTO encounter_diagnoses(encounter_id,diagnosis_id,is_primary,recorded_at) VALUES(%s,%s,%s,%s) RETURNING association_id', (encounter, diagnosis, offset == 0, at)).fetchone()[0])
            current = execute()
            with connect_fixture('dwh', 'admin') as c:
                fact = c.execute('SELECT f.doctor_sk,d.is_active,d.effective_from,d.effective_to FROM dw.fact_encounter f JOIN dw.dim_doctor d USING(doctor_sk) WHERE f.encounter_id=%s', (encounter,)).fetchone()
                bridge = c.execute('SELECT doctor_sk FROM dw.bridge_encounter_doctor WHERE association_id=%s', (association,)).fetchone()[0]
                assert fact[0] != 0 and fact[0] == bridge and fact[1] == active
                assert fact[2] <= at and (fact[3] is None or at < fact[3])
                examples.append({'encounter_id': encounter, 'association_id': association, 'doctor_id': doctor,
                                 'doctor_sk': fact[0], 'bridge_doctor_sk': bridge, 'is_active': active,
                                 'source_business_time': at.isoformat(), 'effective_from': fact[2].isoformat(),
                                 'batch_run_id': current['run_id']})
        assert len({example['doctor_sk'] for example in examples}) == 3
        with connect_fixture('dwh', 'admin') as c:
            initial = encounters[0]
            assert c.execute('SELECT count(*) FROM mart.encounters_by_doctor_diagnosis(%s,%s) WHERE encounter_id=%s', (doctor, diagnoses[0], initial)).fetchone()[0] == 1
            assert c.execute('SELECT count(*) FROM dw.bridge_encounter_doctor d JOIN dw.bridge_encounter_diagnosis x USING(encounter_id) WHERE d.encounter_id=%s', (initial,)).fetchone()[0] == 6
            assert c.execute('SELECT f.department_sk<>d.department_sk FROM dw.fact_encounter f JOIN dw.bridge_encounter_doctor b USING(encounter_id) JOIN dw.dim_doctor d ON b.doctor_sk=d.doctor_sk WHERE b.association_id=%s', (secondary,)).fetchone()[0]
            version_count = c.execute('SELECT count(*) FROM dw.dim_doctor WHERE doctor_id=%s', (doctor,)).fetchone()[0]
        # Exercise real source UPDATE/void and propagation; no physical deletion.
        with connect_fixture('oltp', 'admin') as source:
            source.execute('UPDATE encounter_doctors SET is_active=false WHERE association_id=%s', (secondary,))
            source.execute('UPDATE encounter_diagnoses SET is_active=false WHERE association_id=%s', (diagnosis_associations[-1],))
        execute()
        with connect_fixture('dwh', 'admin') as c:
            watermarks_before_failure = c.execute('SELECT source_name,high_water_updated_at,high_water_pk,version,last_run_id FROM ops.watermarks ORDER BY source_name').fetchall()
        try:
            execute(fail_after_stage=True)
        except RuntimeError as exc:
            assert 'Injected failure' in str(exc)
        else:
            raise AssertionError('Failure injection unexpectedly committed')
        with connect_fixture('dwh', 'admin') as c:
            assert c.execute('SELECT source_name,high_water_updated_at,high_water_pk,version,last_run_id FROM ops.watermarks ORDER BY source_name').fetchall() == watermarks_before_failure
            failure = c.execute("SELECT run_id,started_at,finished_at,input_bounds FROM ops.pipeline_runs WHERE status='FAILED' AND error_code='RuntimeError' ORDER BY finished_at DESC LIMIT 1").fetchone()
            assert failure and failure[2] >= failure[1]
            assert failure[3]['source_observed_at'] and failure[3]['warehouse_started_at']
            assert failure[3]['elapsed_seconds'] >= 0
            failure_audit = dict(zip(('run_id','warehouse_started_at','warehouse_finished_at','input_bounds'), failure))
        repeated = execute()
        with connect_fixture('dwh', 'admin') as c:
            assert c.execute('SELECT count(*) FROM dw.dim_doctor WHERE doctor_id=%s', (doctor,)).fetchone()[0] == version_count
            assert c.execute('SELECT is_active FROM dw.bridge_encounter_doctor WHERE association_id=%s', (secondary,)).fetchone()[0] is False
            assert c.execute('SELECT is_active FROM dw.bridge_encounter_diagnosis WHERE association_id=%s', (diagnosis_associations[-1],)).fetchone()[0] is False
            assert c.execute('SELECT count(*) FROM dw.fact_encounter WHERE encounter_id=ANY(%s)', (encounters,)).fetchone()[0] == 3
            assert c.execute('SELECT count(*) FROM dw.dim_doctor WHERE doctor_id=%s AND effective_to IS NULL', (doctor,)).fetchone()[0] == 1
            assert not c.execute("SELECT EXISTS(SELECT 1 FROM dw.dim_doctor a JOIN dw.dim_doctor b ON a.doctor_id=b.doctor_id AND a.doctor_sk<b.doctor_sk AND tstzrange(a.effective_from,a.effective_to,'[)') && tstzrange(b.effective_from,b.effective_to,'[)'))").fetchone()[0]
            history = [dict(zip(('doctor_id','doctor_sk','is_active','effective_from','effective_to'), row))
                       for row in c.execute('SELECT doctor_id,doctor_sk,is_active,effective_from,effective_to FROM dw.dim_doctor WHERE doctor_id=%s ORDER BY effective_from', (doctor,))]
            audit_clocks = [dict(zip(('run_id','warehouse_started_at','warehouse_finished_at','input_bounds'), row))
                            for row in c.execute('SELECT run_id,started_at,finished_at,input_bounds FROM ops.pipeline_runs WHERE run_id=ANY(%s::uuid[]) ORDER BY started_at',
                                                 ([baseline['run_id'], repeated['run_id'], *[example['batch_run_id'] for example in examples]],))]
        with connect_fixture('oltp', 'admin') as source:
            after = counts(source)
        result = {'passed': True, 'proof_scope': 'live OLTP changes in dedicated source/warehouse fixture databases',
                  'source_database': SOURCE_DB, 'warehouse_database': TARGET_DB, 'frozen_main_dataset_modified': False,
                  'seed_dataset_sha256': manifest['business_sha256'],
                  'source_counts_before': before, 'source_counts_after': after,
                  'source_added_rows': {table: after[table]-before[table] for table in before},
                  'doctor_id': doctor, 'changes': change_observations, 'versions': history,
                  'fact_bridge_examples': examples, 'two_doctors_three_diagnoses_count': 1,
                  'void_propagation': True, 'retry_added_versions': 0, 'baseline_run_id': baseline['run_id'],
                  'final_run_id': repeated['run_id'],
                  'clock_policy': 'source observation clock for business/SCD2; warehouse clock for both audit endpoints; monotonic elapsed',
                  'batch_clock_evidence': {key: repeated[key] for key in ('source_clock','warehouse_clock','source_minus_warehouse_seconds','elapsed_seconds')},
                  'audit_clock_examples': audit_clocks,
                  'injected_failure_audit': failure_audit, 'rollback_preserved_watermarks': True,
                  'fresh_encounter_kpi_note': 'Fresh visits remain behind the normal two-minute batch eligibility cutoff; this proof checks temporal version joins and encounter grain.'}
        path = ROOT/'artifacts/phase5/live_scd2_evidence.json'
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(result, indent=2, default=str), encoding='utf-8')
        print(json.dumps({'live_scd2_passed': True, 'source_added_rows': result['source_added_rows'], 'fact_bridge_examples': examples}))
    finally:
        batch.connect, near_rt.connect = original_batch_connect, original_rt_connect


if __name__ == '__main__':
    run()
