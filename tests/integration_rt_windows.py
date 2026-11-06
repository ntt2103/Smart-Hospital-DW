import json
import os
import uuid
import hashlib
from datetime import datetime,timedelta,timezone
import psycopg
from psycopg.types.json import Jsonb
from operations.foundation import ROOT


def run():
    from tests.integration_warehouse import ensure_fixture
    fixture,_=ensure_fixture();fixture.close()
    checks=[]
    with psycopg.connect(host='postgres-dwh',dbname='hospital_fixture',user='postgres',password=os.environ['DWH_ADMIN_PASSWORD']) as c:
        c.execute("INSERT INTO dw.dim_department(department_sk,department_id,department_code) VALUES (1,1,'TEST') ON CONFLICT DO NOTHING")
        as_of=datetime(2026,10,23,1,tzinfo=timezone.utc)
        c.execute("INSERT INTO dw.dim_department(department_sk,department_id,department_code) VALUES (2,2,'IDLE') ON CONFLICT DO NOTHING")
        ids=[]
        for offset,(entity,kind,delta,ingest_delta) in enumerate([(1,'checked_in',-900,-1),(2,'checked_in',0,-1),(3,'checked_in',1,-1),(4,'checked_in',-899,-1),(4,'checked_in',-898,-1),(5,'checked_in',-1,1),(10,'result_completed',0,-1)]):
            lab=kind=='result_completed'
            event_id=uuid.uuid4();ids.append(event_id)
            event_time=as_of+timedelta(seconds=delta)
            published=as_of+timedelta(seconds=max(delta,0))
            payload={'department_id':1,'encounter_id':1 if lab else entity}
            body={'event_id':str(event_id),'event_type':kind,'entity_id':entity,'schema_version':'1.0.0',
                  'event_time':event_time.isoformat(),'published_at':published.isoformat(),
                  'source_system':'hospital_simulator','payload':payload}
            digest=hashlib.sha256(json.dumps(body,sort_keys=True,separators=(',',':')).encode()).hexdigest()
            c.execute("""INSERT INTO raw.events(event_id,topic,event_type,entity_id,source_system,schema_version,event_time,published_at,ingested_at,payload,event_body,body_sha256,partition_id,kafka_offset)
            VALUES(%s,%s,%s,%s,'hospital_simulator','1.0.0',%s,%s,%s,%s,%s,%s,0,%s)""",(event_id,'laboratory.events' if lab else 'encounters.events',kind,entity,event_time,published,as_of+timedelta(seconds=ingest_delta),Jsonb(payload),Jsonb(body),digest,offset))
        for name,t,expected in [('rt01_checkins_last_15m',as_of,2),('rt02_lab_results_last_15m',as_of,1),('rt01_checkins_last_15m',as_of+timedelta(minutes=20),0),('rt02_lab_results_last_15m',as_of+timedelta(minutes=20),0)]:
            got=c.execute(f'SELECT sum(value) FROM mart.{name}(%s)',(t,)).fetchone()[0]
            assert got==expected,(name,got,expected)
            checks.append({'function':name,'as_of':t.isoformat(),'expected':expected,'actual':int(got)})
        assert c.execute('SELECT value FROM mart.rt01_checkins_last_15m(%s,2)',(as_of,)).fetchone()[0]==0
        # Raw arrival is insufficient evidence of processing, even within a window.
        assert c.execute('SELECT count(*) FROM mart.event_visibility WHERE event_id=ANY(%s)',(ids,)).fetchone()[0]==0
        run_id=uuid.uuid4()
        c.execute("INSERT INTO ops.pipeline_runs(run_id,pipeline_name,run_kind,status,started_at) VALUES(%s,'hospital_near_rt','STREAM_REFRESH','RUNNING',%s)",(run_id,as_of))
        c.execute("""INSERT INTO ops.rt_event_receipts(run_id,event_id,event_type,entity_id,department_sk,event_time,published_at,ingested_at,as_of)
          SELECT %s,event_id,event_type,entity_id,1,event_time,published_at,ingested_at,%s FROM raw.events WHERE event_id=%s""",(run_id,as_of,ids[1]))
        assert c.execute('SELECT count(*) FROM mart.event_visibility WHERE event_id=%s',(ids[1],)).fetchone()[0]==0
        c.execute("UPDATE ops.pipeline_runs SET status='SUCCEEDED',finished_at=%s WHERE run_id=%s",(as_of,run_id))
        assert c.execute('SELECT visible_as_of FROM mart.event_visibility WHERE event_id=%s',(ids[1],)).fetchone()[0]==as_of
        assert c.execute('SELECT count(*) FROM mart.event_visibility WHERE event_id=%s',(ids[3],)).fetchone()[0]==0
        checks.append({'receipt_requires_successful_refresh':True,'raw_arrival_never_infers_processed':True,'idle_department_zero':True})
        c.rollback()
    folder=ROOT/'artifacts/phase5';folder.mkdir(parents=True,exist_ok=True)
    (folder/'window_sql_tests.json').write_text(json.dumps({'passed':len(checks),'boundary_lower_exclusive':True,'boundary_upper_inclusive':True,'business_distinct':True,'future_and_ingest_cutoff':True,'idle_expiry_to_zero':True,'checks':checks},indent=2),encoding='utf-8')
    print('Canonical raw event-time window and snapshot receipt assertions passed.')


if __name__=='__main__':run()
