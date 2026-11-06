"""Durable receipt ledger; offsets advance only after DB transaction commit."""
import base64
import json
import signal
import time
import uuid
from confluent_kafka import Consumer
from psycopg.types.json import Jsonb
from operations.foundation import ROOT,connect
from streaming.common import TOPICS,validate,sha,utc


def ingest(message,source,destination):
    topic,partition,offset=message.topic(),message.partition(),message.offset()
    value=message.value() or b''
    event=None;claimed=None;quarantine_id=None;event_id=None
    key=message.key().decode('utf-8','replace') if message.key() else None
    with destination.transaction():
        if destination.execute('SELECT 1 FROM raw.event_deliveries WHERE topic=%s AND partition_id=%s AND kafka_offset=%s',(topic,partition,offset)).fetchone():return 'replayed_position'
        try:
            event=json.loads(value)
            if isinstance(event,dict):
                try:claimed=uuid.UUID(event.get('event_id',''))
                except (ValueError,TypeError,AttributeError):pass
            validate(event,topic,key,source)
            canonical=destination.execute('SELECT event_body FROM raw.events WHERE event_id=%s',(claimed,)).fetchone()
            if canonical and canonical[0]!=event:raise ValueError('Same event_id with conflicting immutable body')
            disposition='duplicate' if canonical else 'canonical'
            if not canonical:
                destination.execute('INSERT INTO raw.events(event_id,event_type,entity_id,schema_version,event_time,published_at,source_system,payload,event_body,body_sha256,topic,partition_id,kafka_offset) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',(claimed,event['event_type'],event['entity_id'],event['schema_version'],event['event_time'],event['published_at'],event['source_system'],Jsonb(event['payload']),Jsonb(event),sha(event),topic,partition,offset))
            event_id=claimed
        except (ValueError,TypeError,KeyError,UnicodeError) as exc:
            disposition='quarantined'
            quarantine_id=destination.execute("INSERT INTO ops.quarantined_records(source_kind,source_name,source_key,rule_id,severity,reason,payload) VALUES ('KAFKA',%s,%s,'EVENT_VALIDATION','ERROR',%s,%s) RETURNING quarantine_id",(topic,f'{partition}:{offset}',str(exc)[:1000],Jsonb({'event':event,'raw_base64':base64.b64encode(value).decode()}))).fetchone()[0]
        except Exception as exc:
            # JSON Schema ValidationError is not a ValueError; DB/network faults
            # must bubble up and leave the Kafka offset uncommitted.
            from jsonschema.exceptions import ValidationError
            if not isinstance(exc,ValidationError):raise
            disposition='quarantined'
            quarantine_id=destination.execute("INSERT INTO ops.quarantined_records(source_kind,source_name,source_key,rule_id,severity,reason,payload) VALUES ('KAFKA',%s,%s,'EVENT_SCHEMA','ERROR',%s,%s) RETURNING quarantine_id",(topic,f'{partition}:{offset}',exc.message[:1000],Jsonb({'event':event,'raw_base64':base64.b64encode(value).decode()}))).fetchone()[0]
        destination.execute('INSERT INTO raw.event_deliveries(topic,partition_id,kafka_offset,key_text,raw_value,event_body,claimed_event_id,disposition,event_id,quarantine_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)',(topic,partition,offset,key,value,Jsonb(event) if event is not None else None,claimed,disposition,event_id,quarantine_id))
    return disposition


def run():
    client=Consumer({'bootstrap.servers':'kafka:9092','group.id':'hospital-raw-v1','auto.offset.reset':'earliest','enable.auto.commit':False,'enable.auto.offset.store':False})
    client.subscribe(sorted(set(TOPICS.values())))
    running=True
    def stop(*_):
        nonlocal running
        running=False
    signal.signal(signal.SIGTERM,stop);signal.signal(signal.SIGINT,stop)
    counts={};last=0
    try:
        with connect('oltp','reader') as source,connect('dwh','consumer') as destination:
            source.autocommit=True;destination.autocommit=True
            while running:
                msg=client.poll(1)
                if msg is not None:
                    if msg.error():raise RuntimeError(str(msg.error()))
                    disposition=ingest(msg,source,destination)
                    client.commit(message=msg,asynchronous=False)
                    counts[disposition]=counts.get(disposition,0)+1
                if time.monotonic()-last>5:
                    out=ROOT/'artifacts/phase4';out.mkdir(parents=True,exist_ok=True)
                    (out/'consumer_health.json').write_text(json.dumps({'heartbeat':utc().isoformat(),'counts':counts,'offset_commit_policy':'after durable receipt transaction'}),encoding='utf-8')
                    last=time.monotonic()
    finally:client.close()


if __name__=='__main__':run()
