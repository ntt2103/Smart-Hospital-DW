import json
import time
import uuid
from datetime import timedelta
from confluent_kafka import Producer,Consumer,TopicPartition
from psycopg.types.json import Jsonb
from operations.foundation import ROOT,connect
from streaming.producer import Journal
from streaming.common import body,stamp,utc,parse
from streaming.consumer import ingest
from batch_pipeline.jobs.near_rt import refresh


def send(topic,key,value):
    p=Producer({'bootstrap.servers':'kafka:9092','acks':'all'})
    errors=[]
    p.produce(topic,key=key.encode(),value=value,on_delivery=lambda e,m:errors.append(e))
    assert p.flush(20)==0 and errors==[None]


def wait_for(predicate,seconds=45):
    for _ in range(seconds*2):
        if predicate():return
        time.sleep(.5)
    raise AssertionError('Consumer did not reach expected durable state')


def sqlval(query,params=None):
    with connect('dwh','etl') as c:return c.execute(query,params).fetchone()[0]


def snapshot_after_actual_clock(out,timeout_seconds=360):
    """Run the real refresh, keeping pending/failed attempts as clock evidence.

    The input timestamps remain immutable. A future event or ingestion waits
    for a later real job; an expired corpus fails instead of selecting an old
    favorable as_of or altering the input window.
    """
    attempts=[]
    deadline=time.monotonic()+timeout_seconds
    path=out/'rt_clock_attempts.json'
    while time.monotonic()<deadline:
        began=time.monotonic()
        try:
            snapshot=refresh()
        except Exception as exc:
            diagnostic=getattr(exc,'diag',None)
            constraint=getattr(diagnostic,'constraint_name',None)
            clock_check=False
            if getattr(diagnostic,'table_name',None)=='pipeline_runs' and constraint:
                definition=sqlval("SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid='ops.pipeline_runs'::regclass AND conname=%s",(constraint,))
                clock_check=bool(definition and 'finished_at >= started_at' in definition)
            attempts.append({'status':'refresh_failed','error_type':type(exc).__name__,
                             'constraint_name':constraint,'clock_check_failure':clock_check,
                             'monotonic_elapsed_seconds':time.monotonic()-began})
            path.write_text(json.dumps({'attempts':attempts,'timestamps_modified':False},indent=2),encoding='utf-8')
            if not clock_check:
                raise
            time.sleep(10)
            continue
        as_of=parse(snapshot['as_of'])
        with connect('dwh','etl') as c:
            first_event,last_event,last_ingestion=c.execute("SELECT min(event_time),max(event_time),max(ingested_at) FROM raw.events WHERE event_type IN ('checked_in','result_completed')").fetchone()
            checkins,labs=c.execute('SELECT sum(checkins_last_15m),sum(lab_results_last_15m) FROM mart.realtime_department_snapshot WHERE as_of=%s',(as_of,)).fetchone()
        checkins,labs=int(checkins or 0),int(labs or 0)
        pending=as_of<max(last_event,last_ingestion)
        expired=as_of>=last_event+timedelta(minutes=15)
        attempt={'status':'expired' if expired else 'inputs_pending_real_clock' if pending else 'eligible',
                 'snapshot':snapshot,'checkins':checkins,'lab_results':labs,
                 'first_event_time':first_event.isoformat(),'last_event_time':last_event.isoformat(),
                 'last_ingested_at':last_ingestion.isoformat(),
                 'monotonic_elapsed_seconds':time.monotonic()-began}
        attempts.append(attempt)
        path.write_text(json.dumps({'attempts':attempts,'timestamps_modified':False},indent=2),encoding='utf-8')
        if expired:
            raise AssertionError('RT corpus expired while waiting for the actual warehouse clock; recorded as incomplete')
        if not pending:
            assert checkins>0 and labs>0,attempt
            return snapshot
        time.sleep(10)
    raise AssertionError('Actual RT refreshes did not reach immutable input timestamps within the fixed budget')


def run(stage='before'):
    journal=Journal()
    out=ROOT/'artifacts/phase5';out.mkdir(parents=True,exist_ok=True)
    if stage=='before':
        command=journal.prepare(full=True)
        try:journal.apply(command,crash_after_commit=True)
        except RuntimeError:pass
        assert journal.db.execute('SELECT state FROM commands WHERE id=?',(command,)).fetchone()[0]=='PREPARED'
        with connect('oltp','reader') as c:before=c.execute('SELECT count(*) FROM encounters').fetchone()[0]
        journal.recover(publish=False)
        with connect('oltp','reader') as c:assert c.execute('SELECT count(*) FROM encounters').fetchone()[0]==before
        try:journal.recover(bootstrap='kafka:1',ack_timeout=3)
        except RuntimeError:pass
        else:raise AssertionError('Broken broker test unexpectedly succeeded')
        assert journal.db.execute('SELECT count(*) FROM messages WHERE published=0').fetchone()[0]>0
        journal.recover()
        ids=[r[0] for r in journal.db.execute('SELECT event_id FROM messages')]
        wait_for(lambda:sqlval('SELECT count(*) FROM raw.events WHERE event_id=ANY(%s::uuid[])',(ids,))==len(ids))
        record=journal.db.execute("SELECT topic,key_text,body FROM messages WHERE body LIKE '%checked_in%' ORDER BY rowid LIMIT 1").fetchone()
        if not record:
            record=next(r for r in journal.db.execute('SELECT topic,key_text,body FROM messages') if json.loads(r[2])['event_type']=='checked_in')
        topic,key,value=record
        canonical_before=sqlval('SELECT count(*) FROM raw.events')
        delivery_before=sqlval('SELECT count(*) FROM raw.event_deliveries')
        send(topic,key,value);send(topic,key,value)
        wait_for(lambda:sqlval('SELECT count(*) FROM raw.event_deliveries')>=delivery_before+2)
        assert sqlval('SELECT count(*) FROM raw.events')==canonical_before
        bad=json.loads(value);bad['published_at']=stamp(parse(bad['published_at'])+timedelta(microseconds=1))
        qbefore=sqlval("SELECT count(*) FROM ops.quarantined_records WHERE source_kind='KAFKA'")
        send(topic,key,body(bad));send(topic,key,b'\xffnot-json')
        wait_for(lambda:sqlval("SELECT count(*) FROM ops.quarantined_records WHERE source_kind='KAFKA'")>=qbefore+2)
        assert sqlval('SELECT count(*) FROM raw.events')==canonical_before
        semantic=json.loads(value);semantic['event_id']=str(uuid.uuid4())
        send(topic,key,body(semantic))
        wait_for(lambda:sqlval('SELECT count(*) FROM raw.events WHERE event_id=%s',(semantic['event_id'],))==1)
        # Publish a delayed command in reverse event order, with valid milestones.
        late=journal.prepare();journal.apply(late);time.sleep(2);journal.recover(reverse=True)
        ids=[r[0] for r in journal.db.execute('SELECT event_id FROM messages')]
        wait_for(lambda:sqlval('SELECT count(*) FROM raw.events WHERE event_id=ANY(%s::uuid[])',(ids,))==len(ids))
        snapshot=snapshot_after_actual_clock(out)
        distinct=sqlval("SELECT count(DISTINCT entity_id) FROM raw.events WHERE event_type='checked_in' AND event_time>%s::timestamptz-interval '15 minutes' AND event_time<=%s AND ingested_at<=%s",(snapshot['as_of'],snapshot['as_of'],snapshot['as_of']))
        assert sqlval('SELECT sum(checkins_last_15m) FROM mart.realtime_department_snapshot WHERE as_of=%s',(snapshot['as_of'],))==distinct
        assert sqlval("SELECT count(*) FROM mart.event_visibility WHERE event_id=ANY(%s::uuid[])",(ids,))>0
        assert sqlval("SELECT count(*) FROM ops.rt_event_receipts r JOIN raw.events e USING(event_id) WHERE r.run_id=%s AND (r.event_time<=r.as_of-interval '15 minutes' OR r.event_time>r.as_of OR r.ingested_at>r.as_of)",(snapshot['run_id'],))==0
        assert sqlval('SELECT count(DISTINCT event_type) FROM raw.events')==13
        result={'source_commit_crash_recovered':True,'publish_failure_retained_and_resend_succeeded':True,'same_uuid_duplicate_idempotent':True,
                'uuid_conflict_and_invalid_bytes_quarantined':True,'different_uuid_same_entity_deduplicated_in_rt':True,
                'late_in_window_and_reverse_order_processed':True,'all_13_event_types_ingested':sqlval('SELECT count(DISTINCT event_type) FROM raw.events')==13}
        (out/'streaming_failure_tests.json').write_text(json.dumps(result,indent=2),encoding='utf-8')
    else:
        result=json.loads((out/'streaming_failure_tests.json').read_text())
        command=journal.prepare();journal.apply(command);journal.recover()
        ids=[r[0] for r in journal.db.execute('SELECT event_id FROM messages')]
        wait_for(lambda:sqlval('SELECT count(*) FROM raw.events WHERE event_id=ANY(%s::uuid[])',(ids,))==len(ids))
        canonical=sqlval('SELECT count(*) FROM raw.events');deliveries=sqlval('SELECT count(*) FROM raw.event_deliveries')
        reader=Consumer({'bootstrap.servers':'kafka:9092','group.id':'hospital-replay-validation','enable.auto.commit':False})
        partitions=[];ends={}
        for topic in ['appointments.events','encounters.events','laboratory.events']:
            for p in range(3):
                tp=TopicPartition(topic,p,0);_,end=reader.get_watermark_offsets(tp,timeout=10)
                if end:partitions.append(tp);ends[(topic,p)]=end
        reader.assign(partitions);seen={};replayed=0
        with connect('oltp','reader') as src,connect('dwh','consumer') as dst:
            src.autocommit=True;dst.autocommit=True
            stop=time.monotonic()+45
            while time.monotonic()<stop and any(seen.get(k,0)<end for k,end in ends.items()):
                msg=reader.poll(1)
                if not msg:continue
                if msg.error():raise RuntimeError(str(msg.error()))
                assert ingest(msg,src,dst)=='replayed_position'
                seen[(msg.topic(),msg.partition())]=msg.offset()+1;replayed+=1
        reader.close()
        assert all(seen.get(k,0)>=end for k,end in ends.items())
        assert sqlval('SELECT count(*) FROM raw.events')==canonical and sqlval('SELECT count(*) FROM raw.event_deliveries')==deliveries
        result.update(consumer_restart_caught_up=True,position_replay_safe=True,replayed_positions=replayed)
        (out/'streaming_failure_tests.json').write_text(json.dumps(result,indent=2),encoding='utf-8')
    print(json.dumps(result))


if __name__=='__main__':
    import sys
    run(sys.argv[1] if len(sys.argv)>1 else 'before')
