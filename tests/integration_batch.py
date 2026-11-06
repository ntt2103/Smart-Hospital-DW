import json
from batch_pipeline.jobs.batch import execute
from operations.foundation import ROOT,connect


def watermarks():
    with connect('dwh','etl') as c:
        return c.execute("SELECT source_name,high_water_updated_at,high_water_pk,version FROM ops.watermarks WHERE pipeline_name='hospital_batch' ORDER BY source_name").fetchall()


def run():
    before=watermarks()
    try: execute(fail_after_stage=True)
    except RuntimeError: pass
    else: raise AssertionError('Failure injection did not fail')
    assert watermarks()==before,'Watermark advanced across failed run'
    baseline=execute()
    with connect('dwh','etl') as c:doctor_versions_before=c.execute('SELECT count(*) FROM dw.dim_doctor WHERE doctor_id=1').fetchone()[0]
    with connect('oltp','writer') as c:
        appointment=c.execute("SELECT appointment_id,scheduled_at FROM appointments WHERE status='SCHEDULED' AND scheduled_at<now()-interval '25 hours' ORDER BY appointment_id LIMIT 1 FOR UPDATE").fetchone()
        assert appointment
        c.execute("UPDATE appointments SET status='NO_SHOW',status_changed_at=scheduled_at+interval '24 hours' WHERE appointment_id=%s",(appointment[0],))
        c.execute('UPDATE doctors SET is_active=false WHERE doctor_id=1')
        assignment=c.execute("SELECT bed_assignment_id,bed_id,(released_at AT TIME ZONE 'Asia/Ho_Chi_Minh')::date FROM bed_assignments WHERE released_at-assigned_at>interval '1 hour' AND (released_at AT TIME ZONE 'Asia/Ho_Chi_Minh')::time BETWEEN time '00:00:01' AND time '00:10:00' ORDER BY bed_assignment_id LIMIT 1 FOR UPDATE").fetchone()
        assert assignment
        c.execute("UPDATE bed_assignments SET released_at=(released_at AT TIME ZONE 'Asia/Ho_Chi_Minh')::date::timestamp AT TIME ZONE 'Asia/Ho_Chi_Minh'-interval '1 minute' WHERE bed_assignment_id=%s",(assignment[0],))
    updated=execute()
    assert updated['extracted']<151433,'Incremental unexpectedly extracted full history'
    with connect('dwh','etl') as c:
        assert c.execute('SELECT status FROM dw.fact_appointment WHERE appointment_id=%s',(appointment[0],)).fetchone()[0]=='NO_SHOW'
        assert c.execute('SELECT count(*) FROM dw.dim_doctor WHERE doctor_id=1').fetchone()[0]==doctor_versions_before+1
        assert c.execute('SELECT is_occupied FROM dw.fact_bed_snapshot_daily f JOIN dw.dim_bed b USING(bed_sk) JOIN dw.dim_date d ON d.date_key=f.snapshot_date_key WHERE b.bed_id=%s AND d.calendar_date=%s',(assignment[1],assignment[2])).fetchone()[0] is False
    with connect('oltp','writer') as c: c.execute('UPDATE doctors SET is_active=true WHERE doctor_id=1')
    backfill=execute('backfill',bounds={'start':str(assignment[2]),'end':str(assignment[2])})
    repeat=execute()
    assert repeat['fact_counts']==updated['fact_counts']==baseline['fact_counts']
    result={'rollback_preserved_watermarks':True,'incremental_status_update':True,'doctor_scd2_observed':True,
            'historical_snapshot_correction':True,'bounded_snapshot_backfill':True,'rerun_fact_grains_unchanged':True,
            'incremental_rows_extracted':updated['extracted'],'runs':[baseline,updated,backfill,repeat]}
    (ROOT/'artifacts/phase3/batch_recovery.json').write_text(json.dumps(result,indent=2),encoding='utf-8')
    print(json.dumps({k:v for k,v in result.items() if k!='runs'}))


if __name__=='__main__':run()
