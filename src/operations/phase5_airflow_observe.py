"""Read current warehouse/DAG evidence after the isolated acceptance runs."""
import json
import os

import psycopg
from psycopg import sql
from operations.foundation import ROOT, connect


def run():
    if os.environ.get('COMPOSE_PROJECT_NAME') != 'smart_hospital_dw_phase5':
        raise ValueError('Observation is restricted to the isolated Phase 5 project')
    report={'scope':'Current warehouse after real DAG acceptance; distinct from earlier CLI batch evidence',
            'source_data_modified_by_observer':False,'historical_core_encounter_upper_id':1080000}
    with connect('dwh','etl') as connection:
        connection.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
        report['warehouse_clock']=connection.execute('SELECT clock_timestamp()').fetchone()[0]
        inventory=connection.execute("""SELECT CASE WHEN c.relname LIKE 'fact_%' THEN 'facts'
            WHEN c.relname LIKE 'dim_%' THEN 'dimensions' ELSE 'bridges' END,count(*)
            FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
            WHERE n.nspname='dw' AND c.relkind IN('r','p')
            AND (c.relname LIKE 'fact_%' OR c.relname LIKE 'dim_%' OR c.relname LIKE 'bridge_%')
            GROUP BY 1 ORDER BY 1""").fetchall()
        report['physical_inventory']=dict(inventory)
        tables=['fact_appointment','fact_encounter','fact_lab_order','fact_bed_assignment',
                'fact_bed_snapshot_daily','bridge_encounter_doctor','bridge_encounter_diagnosis']
        report['warehouse_counts']={table:connection.execute(sql.SQL('SELECT count(*) FROM dw.{}').format(
            sql.Identifier(table))).fetchone()[0] for table in tables}
        report['doctor_coverage']={}
        for table in ('fact_encounter','bridge_encounter_doctor'):
            rows=connection.execute(sql.SQL("""SELECT CASE WHEN encounter_id<=1080000 THEN 'core_history'
                ELSE 'fresh_live' END,count(*),count(*) FILTER(WHERE doctor_id IS NOT NULL),
                count(*) FILTER(WHERE doctor_sk=0),count(*) FILTER(WHERE doctor_sk<>0)
                FROM dw.{} GROUP BY 1 ORDER BY 1""").format(sql.Identifier(table))).fetchall()
            report['doctor_coverage'][table]={scope:{'rows':total,'assigned_business_ids':assigned,
                'unknown_rows':unknown,'known_rows':known,
                'unknown_percent':100.0*unknown/total if total else None}
                for scope,total,assigned,unknown,known in rows}
            for scope in ('core_history','fresh_live'):
                report['doctor_coverage'][table].setdefault(scope,{'rows':0,'assigned_business_ids':0,
                    'unknown_rows':0,'known_rows':0,'unknown_percent':None})
        row=connection.execute("""SELECT min(d.calendar_date),max(d.calendar_date),count(*),
            count(*) FILTER(WHERE NOT f.is_snapshot_valid),count(*) FILTER(WHERE f.snapshot_at>clock_timestamp())
            FROM dw.fact_bed_snapshot_daily f JOIN dw.dim_date d ON d.date_key=f.snapshot_date_key""").fetchone()
        report['bed_snapshots']=dict(zip(('first_date','last_date','rows','invalid_rows','future_rows'),row))
        row=connection.execute("""SELECT min(calendar_date),max(calendar_date),count(*) FILTER(WHERE date_key<>0),
            (SELECT max((scheduled_at AT TIME ZONE 'Asia/Ho_Chi_Minh')::date) FROM stg.appointments)
            FROM dw.dim_date""").fetchone()
        report['date_dimension']=dict(zip(('first_date','last_date','calendar_rows','latest_source_appointment_date'),row))
        report['date_dimension']['future_appointments_covered']=row[1]>=row[3]
        columns=('pipeline_name','run_id','status','started_at','finished_at','extracted_count',
                 'upserted_count','input_bounds','error_code')
        rows=connection.execute("""SELECT DISTINCT ON(pipeline_name) pipeline_name,run_id,status,started_at,
            finished_at,extracted_count,upserted_count,input_bounds,error_code FROM ops.pipeline_runs
            WHERE pipeline_name IN('hospital_batch','hospital_near_rt')
            ORDER BY pipeline_name,finished_at DESC NULLS LAST""").fetchall()
        report['latest_pipeline_runs']=[dict(zip(columns,row)) for row in rows]
        snapshot=connection.execute('SELECT max(as_of) FROM mart.realtime_department_snapshot').fetchone()[0]
        row=connection.execute("""SELECT count(*),sum(checkins_last_15m),sum(lab_results_last_15m)
            FROM mart.realtime_department_snapshot WHERE as_of=%s""",(snapshot,)).fetchone()
        report['current_rt_snapshot']={'as_of':snapshot,'department_rows':row[0],
            'checkins':int(row[1] or 0),'lab_results':int(row[2] or 0),
            'scope':'Current operating window, distinct from the paired SQL benchmark locked as_of'}
    with psycopg.connect(host='postgres-meta',dbname='airflow_meta',user='postgres',
                         password=os.environ['META_ADMIN_PASSWORD'],options='-c timezone=UTC') as metadata:
        metadata.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
        report['airflow_runs']=[dict(zip(('dag_id','run_id','run_type','state'),row)) for row in metadata.execute(
            "SELECT dag_id,run_id,run_type,state FROM dag_run ORDER BY id")]
        report['dag_paused']={dag:paused for dag,paused in metadata.execute(
            "SELECT dag_id,is_paused FROM dag WHERE dag_id IN('hospital_batch','hospital_near_rt') ORDER BY dag_id")}
    assert report['physical_inventory']=={'bridges':2,'dimensions':7,'facts':5}
    assert report['bed_snapshots']['invalid_rows']==0 and report['bed_snapshots']['future_rows']==0
    assert report['date_dimension']['future_appointments_covered']
    assert len(report['latest_pipeline_runs'])==2 and all(r['status']=='SUCCEEDED' for r in report['latest_pipeline_runs'])
    assert all(row['run_type']=='manual' and row['state']=='success' for row in report['airflow_runs'])
    output=ROOT/'artifacts/phase5/warehouse_after_dags.json'
    output.write_text(json.dumps(report,indent=2,default=str),encoding='utf-8')
    print(json.dumps({'evidence':str(output),'inventory':report['physical_inventory'],
                     'counts':report['warehouse_counts'],'current_rt':report['current_rt_snapshot']},default=str))


if __name__=='__main__': run()
