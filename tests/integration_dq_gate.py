import json
from operations.foundation import ROOT,connect
from batch_pipeline.jobs.batch import execute
from tests.integration_batch import watermarks


def run():
    before=watermarks()
    with connect('oltp','writer') as c:
        encounter,patient=c.execute('SELECT encounter_id,patient_id FROM encounters WHERE parent_encounter_id IS NOT NULL ORDER BY encounter_id LIMIT 1 FOR UPDATE').fetchone()
        different=c.execute('SELECT patient_id FROM patients WHERE patient_id<>%s LIMIT 1',(patient,)).fetchone()[0]
        c.execute('UPDATE encounters SET patient_id=%s WHERE encounter_id=%s',(different,encounter))
    try:
        try:execute()
        except ValueError as exc:assert 'OP_TO_IP' in str(exc)
        else:raise AssertionError('Cross-table DQ violation was accepted')
        assert watermarks()==before
        with connect('dwh','etl') as c:
            row=c.execute("SELECT payload FROM ops.quarantined_records WHERE payload ? 'issues' ORDER BY quarantine_id DESC LIMIT 1").fetchone()
            assert row and any(i['rule']=='OP_TO_IP' for i in row[0]['issues'])
            assert row[0]['records']['encounters']
    finally:
        with connect('oltp','writer') as c:c.execute('UPDATE encounters SET patient_id=%s WHERE encounter_id=%s',(patient,encounter))
    execute()
    result={'cross_table_violation_blocked':True,'watermark_unchanged_on_dq_failure':True,'offending_payload_quarantined':True,'source_restored_and_batch_recovered':True}
    (ROOT/'artifacts/phase3/dq_gate.json').write_text(json.dumps(result,indent=2),encoding='utf-8')
    print(json.dumps(result))


if __name__=='__main__':run()
