import json
import uuid
from psycopg.types.json import Jsonb
from operations.foundation import ROOT,connect,migration


def setup():
    with connect('dwh','admin') as c:
        migration(c,'006_streaming_views',ROOT/'sql/warehouse/ddl/streaming_helpers.sql')
        migration(c,'009_phase5_canonical_rt',ROOT/'sql/migrations/009_phase5_canonical_rt.sql')
        c.execute('GRANT SELECT ON ALL TABLES IN SCHEMA mart TO hospital_reporter')


def refresh():
    run=uuid.uuid4()
    with connect('dwh','etl') as c:
        # One MVCC input snapshot for receipts, both KPIs and every department.
        # A canonical event arriving during the refresh waits for the next run.
        c.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ')
        c.execute('SELECT pg_advisory_xact_lock(12447804)')
        # Use the same database clock as raw.ingested_at and refresh publication.
        as_of=c.execute('SELECT clock_timestamp()').fetchone()[0]
        c.execute("INSERT INTO ops.pipeline_runs(run_id,pipeline_name,run_kind,status,started_at,input_bounds) VALUES (%s,'hospital_near_rt','STREAM_REFRESH','RUNNING',%s,%s)",(run,as_of,Jsonb({'window_minutes':15,'as_of':as_of.isoformat()})))
        loaded=c.execute("""INSERT INTO ops.rt_event_receipts(run_id,event_id,event_type,entity_id,department_sk,event_time,published_at,ingested_at,as_of)
        SELECT %s,e.event_id,e.event_type,e.entity_id,d.department_sk,e.event_time,e.published_at,e.ingested_at,%s
        FROM raw.events e JOIN dw.dim_department d ON d.department_id=(e.payload->>'department_id')::bigint
        WHERE ((e.topic='encounters.events' AND e.event_type='checked_in')
          OR (e.topic='laboratory.events' AND e.event_type='result_completed'))
        AND e.event_time>%s-interval '15 minutes' AND e.event_time<=%s AND e.ingested_at<=%s""",(run,as_of,as_of,as_of,as_of)).rowcount
        c.execute('DELETE FROM mart.realtime_department_snapshot WHERE as_of=%s',(as_of,))
        c.execute("""INSERT INTO mart.realtime_department_snapshot(as_of,department_sk,checkins_last_15m,lab_results_last_15m,refreshed_at)
        SELECT %s,d.department_sk,a.value,b.value,clock_timestamp() FROM mart.rt01_checkins_last_15m(%s) a JOIN mart.rt02_lab_results_last_15m(%s) b USING(department_id) JOIN dw.dim_department d ON coalesce(d.department_id,0)=a.department_id""",(as_of,as_of,as_of))
        count=c.execute('SELECT count(*) FROM mart.realtime_department_snapshot WHERE as_of=%s',(as_of,)).fetchone()[0]
        c.execute("INSERT INTO ops.mart_refreshes(refresh_id,run_id,mart_name,as_of,input_cutoff,published_at,row_count) VALUES (%s,%s,'realtime_department_snapshot',%s,%s,clock_timestamp(),%s)",(uuid.uuid4(),run,as_of,as_of,count))
        c.execute("UPDATE ops.pipeline_runs SET status='SUCCEEDED',finished_at=clock_timestamp(),extracted_count=%s,upserted_count=%s WHERE run_id=%s",(loaded,count,run))
    result={'run_id':str(run),'as_of':as_of.isoformat(),'canonical_signal_receipts':loaded,'department_rows':count}
    print(json.dumps(result));return result


if __name__=='__main__':
    import sys
    if '--setup' in sys.argv:setup()
    refresh()
