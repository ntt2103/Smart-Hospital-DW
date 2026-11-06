"""Real-clock SCD2 and multi-valued bridge proof in the disposable fixture DB.

The observed profile changes are staged fixture observations, not invented
historical source versions. Nothing is added to the benchmark's frozen source.
"""
import json
import uuid
from datetime import timedelta
from psycopg import sql
from operations.foundation import ROOT
from tests.integration_warehouse import ensure_fixture


def run():
    c, definitions = ensure_fixture()
    c.execute('BEGIN')
    observations = []

    def insert(table, row):
        names = [name for name, kind in definitions[table]]
        values = {name: None for name in names}
        values.update(created_at=first, updated_at=first, is_active=True)
        values.update(row)
        c.execute(sql.SQL('INSERT INTO stg.{} ({}) VALUES ({})').format(
            sql.Identifier(table), sql.SQL(',').join(map(sql.Identifier, names)),
            sql.SQL(',').join(sql.Placeholder() for name in names)), [values[name] for name in names])

    def restate():
        # Each version boundary is the actual database observation clock.
        observed = c.execute('SELECT clock_timestamp()').fetchone()[0]
        c.execute('DELETE FROM run_context')
        c.execute('INSERT INTO run_context(run_id,as_of,observed_at) VALUES(%s,%s,%s)',
                  (uuid.uuid4(), observed, observed))
        c.execute((ROOT/'sql/warehouse/transformations/batch.sql').read_text(encoding='utf-8'))
        for name in ('next_patient','next_department','next_bed','next_test','next_appointment',
                     'next_encounter','next_diagnosis','next_doctor_bridge','next_diagnosis_bridge',
                     'next_assignment','next_lab','next_snapshot'):
            c.execute(sql.SQL('DROP TABLE {}').format(sql.Identifier(name)))
        observations.append(observed.isoformat())
        return observed

    def encounter(key, at):
        insert('encounters', {'encounter_id': key, 'patient_id': 1, 'department_id': 1,
               'doctor_id': 1, 'encounter_type': 'OUTPATIENT', 'status': 'IN_CONSULTATION',
               'status_changed_at': at, 'checked_in_at': at, 'consultation_started_at': at})
        insert('encounter_doctors', {'association_id': key*10, 'encounter_id': key,
               'doctor_id': 1, 'role': 'PRIMARY', 'joined_at': at})

    try:
        first = c.execute('SELECT clock_timestamp()').fetchone()[0]
        c.execute('CREATE TEMP TABLE run_context(run_id UUID,as_of TIMESTAMPTZ,observed_at TIMESTAMPTZ,snapshot_start DATE,snapshot_end DATE) ON COMMIT DROP')
        insert('patients', {'patient_id': 1, 'patient_code': 'FIX-P1', 'gender': 'UNKNOWN', 'date_of_birth': '1990-01-01'})
        for key in (1, 2):
            insert('departments', {'department_id': key, 'department_code': f'FIX-D{key}', 'department_name': f'Fixture {key}'})
            insert('doctors', {'doctor_id': key, 'doctor_code': f'FIX-DR{key}', 'department_id': key})
        for key in (1, 2, 3):
            insert('diagnoses', {'diagnosis_id': key, 'diagnosis_code': f'SYN-FIX-{key}', 'diagnosis_name': f'Synthetic fixture {key}'})
        encounter(1, first-timedelta(days=1))
        restate()
        assert c.execute('SELECT doctor_sk FROM dw.fact_encounter WHERE encounter_id=1').fetchone()[0] == 0

        versions = []
        for index, active in enumerate((True, False, True), 2):
            if index > 2:
                c.execute('UPDATE stg.doctors SET is_active=%s,updated_at=clock_timestamp() WHERE doctor_id=1', (active,))
                restate()
            at = c.execute('SELECT clock_timestamp()').fetchone()[0]
            encounter(index, at)
            if index == 2:
                insert('encounter_doctors', {'association_id': 21, 'encounter_id': 2, 'doctor_id': 2, 'role': 'CONSULTING', 'joined_at': at})
                for key in (1, 2, 3):
                    insert('encounter_diagnoses', {'association_id': 20+key, 'encounter_id': 2, 'diagnosis_id': key,
                           'is_primary': key == 1, 'recorded_at': at})
            restate()
            fact = c.execute('SELECT doctor_sk FROM dw.fact_encounter WHERE encounter_id=%s', (index,)).fetchone()[0]
            bridge = c.execute('SELECT doctor_sk FROM dw.bridge_encounter_doctor WHERE association_id=%s', (index*10,)).fetchone()[0]
            row = c.execute('SELECT doctor_sk,is_active,effective_from,effective_to FROM dw.dim_doctor WHERE doctor_sk=%s', (fact,)).fetchone()
            assert fact != 0 and bridge == fact and row[1] == active
            versions.append({'encounter_id': index, 'doctor_sk': fact, 'bridge_doctor_sk': bridge,
                             'business_time': at.isoformat(), 'is_active': active})
        assert len({v['doctor_sk'] for v in versions}) == 3
        assert c.execute('SELECT count(*) FROM mart.encounters_by_doctor_diagnosis(1,1) WHERE encounter_id=2').fetchone()[0] == 1
        assert c.execute('SELECT count(*) FROM dw.fact_encounter f JOIN dw.bridge_encounter_doctor d USING(encounter_id) JOIN dw.bridge_encounter_diagnosis x USING(encounter_id) WHERE f.encounter_id=2').fetchone()[0] == 6
        assert c.execute('SELECT count(*) FROM dw.bridge_encounter_doctor WHERE encounter_id=2 AND doctor_sk=0').fetchone()[0] == 0
        assert c.execute('SELECT count(*) FROM mart.encounters_by_doctor_diagnosis(NULL,NULL)').fetchone()[0] == 4
        # Owning department is retained when a consulting doctor belongs elsewhere.
        assert c.execute('SELECT f.department_sk<>d.department_sk FROM dw.fact_encounter f JOIN dw.bridge_encounter_doctor b USING(encounter_id) JOIN dw.dim_doctor d ON d.doctor_sk=b.doctor_sk WHERE b.association_id=21').fetchone()[0]
        before = c.execute('SELECT count(*) FROM dw.dim_doctor WHERE doctor_id=1').fetchone()[0]
        c.execute('UPDATE stg.encounter_doctors SET is_active=false,updated_at=clock_timestamp() WHERE association_id=21')
        c.execute('UPDATE stg.encounter_diagnoses SET is_active=false,updated_at=clock_timestamp() WHERE association_id=23')
        c.execute("UPDATE stg.diagnoses SET diagnosis_name='Updated synthetic description',updated_at=clock_timestamp() WHERE diagnosis_id=1")
        restate()
        restate()
        assert c.execute('SELECT count(*) FROM dw.dim_doctor WHERE doctor_id=1').fetchone()[0] == before
        assert c.execute('SELECT is_active FROM dw.bridge_encounter_doctor WHERE association_id=21').fetchone()[0] is False
        assert c.execute('SELECT is_active FROM dw.bridge_encounter_diagnosis WHERE association_id=23').fetchone()[0] is False
        assert c.execute('SELECT count(*) FROM dw.dim_diagnosis WHERE diagnosis_id=1').fetchone()[0] == 1
        assert c.execute('SELECT diagnosis_name FROM dw.dim_diagnosis WHERE diagnosis_id=1').fetchone()[0] == 'Updated synthetic description'
        assert c.execute('SELECT count(*) FROM dw.fact_encounter').fetchone()[0] == 4
        assert c.execute('SELECT count(*) FROM dw.bridge_encounter_doctor').fetchone()[0] == 5
        assert c.execute('SELECT count(*) FROM dw.bridge_encounter_diagnosis').fetchone()[0] == 3
        assert c.execute('SELECT count(*) FROM dw.dim_doctor WHERE doctor_id=1 AND effective_to IS NULL').fetchone()[0] == 1
        assert not c.execute("SELECT EXISTS(SELECT 1 FROM dw.dim_doctor a JOIN dw.dim_doctor b ON a.doctor_id=b.doctor_id AND a.doctor_sk<b.doctor_sk AND tstzrange(a.effective_from,a.effective_to,'[)') && tstzrange(b.effective_from,b.effective_to,'[)'))").fetchone()[0]
        inventory = dict(c.execute("SELECT CASE WHEN relname LIKE 'fact_%' THEN 'facts' WHEN relname LIKE 'dim_%' THEN 'dimensions' WHEN relname LIKE 'bridge_%' THEN 'bridges' END,count(*) FROM pg_class JOIN pg_namespace n ON n.oid=relnamespace WHERE n.nspname='dw' AND relkind='r' AND (relname LIKE 'fact_%' OR relname LIKE 'dim_%' OR relname LIKE 'bridge_%') GROUP BY 1").fetchall())
        assert inventory == {'facts': 5, 'dimensions': 7, 'bridges': 2}, inventory
        history = [dict(zip(('doctor_id','doctor_sk','is_active','effective_from','effective_to'), row))
                   for row in c.execute('SELECT doctor_id,doctor_sk,is_active,effective_from,effective_to FROM dw.dim_doctor WHERE doctor_id=1 ORDER BY effective_from')]
        result = {'passed': True, 'proof_scope': 'disposable typed-staging fixture with actual PostgreSQL observation clock',
                  'source_benchmark_rows_added': 0, 'inventory': inventory, 'scd2_observations': observations,
                  'doctor_history': history, 'fact_bridge_examples': versions,
                  'unknown_historical_encounters': 1, 'covered_live_encounters': 3,
                  'two_doctors_three_diagnoses_encounter_count': 1,
                  'cartesian_rows_avoided': 6, 'void_and_scd1_rerun_checked': True}
    finally:
        c.execute('ROLLBACK')
        c.close()
    path = ROOT/'artifacts/phase5/scd2_bridge_evidence.json'
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(result, indent=2, default=str), encoding='utf-8')
    print(json.dumps({'phase5_model_passed': True, 'inventory': inventory}))


if __name__ == '__main__':
    run()
