"""Single writer, durable command journal and immutable event bytes across retries."""
import argparse
import fcntl
import json
import os
import sqlite3
import time
import uuid
import copy
from datetime import timedelta
from pathlib import Path
from psycopg import sql
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient,NewTopic
from operations.foundation import ROOT,connect
from streaming.common import TOPICS,VALIDATOR,utc,stamp,body,parse


def topics():
    admin=AdminClient({'bootstrap.servers':'kafka:9092'})
    existing=admin.list_topics(timeout=15).topics
    missing=[NewTopic(t,3,1) for t in sorted(set(TOPICS.values())) if t not in existing]
    if missing:
        for name,future in admin.create_topics(missing).items():future.result()
    return sorted(set(TOPICS.values()))


class Journal:
    def __init__(self,path=None):
        path=Path(path or os.environ.get('PRODUCER_JOURNAL_PATH','/var/lib/hospital-producer/journal.sqlite3'))
        path.parent.mkdir(parents=True,exist_ok=True)
        self.lock=open(str(path)+'.lock','a')
        fcntl.flock(self.lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        self.db=sqlite3.connect(path)
        self.publish_references={}
        self.db.execute('PRAGMA journal_mode=WAL');self.db.execute('PRAGMA synchronous=FULL')
        self.db.executescript('CREATE TABLE IF NOT EXISTS commands(id TEXT PRIMARY KEY,state TEXT NOT NULL,intent TEXT NOT NULL); CREATE TABLE IF NOT EXISTS messages(event_id TEXT PRIMARY KEY,command_id TEXT NOT NULL,topic TEXT NOT NULL,key_text TEXT NOT NULL,body BLOB NOT NULL,published INTEGER NOT NULL DEFAULT 0);')

    def prepare(self,full=False):
        with connect('oltp','writer') as c:
            def reserve(table,pk):return c.execute('SELECT nextval(pg_get_serial_sequence(%s,%s))',('public.'+table,pk)).fetchone()[0]
            eid=reserve('encounters','encounter_id');lid=reserve('lab_orders','lab_order_id')
            patient=c.execute('SELECT patient_id FROM patients WHERE is_active ORDER BY patient_id LIMIT 1').fetchone()[0]
            doctor,dept=c.execute('SELECT doctor_id,department_id FROM doctors WHERE is_active ORDER BY doctor_id LIMIT 1').fetchone()
            test,target=c.execute('SELECT lab_test_type_id,default_tat_minutes FROM lab_test_types WHERE is_active ORDER BY lab_test_type_id LIMIT 1').fetchone()
            aid=reserve('appointments','appointment_id') if full else None
            ipid=reserve('encounters','encounter_id') if full else None
            cancelled_id=reserve('lab_orders','lab_order_id') if full else None
            assignment_id=reserve('bed_assignments','bed_assignment_id') if full else None
            reference=c.execute('SELECT clock_timestamp()').fetchone()[0]
            window_start=reference-timedelta(seconds=5)
            bed=c.execute("SELECT b.bed_id FROM beds b JOIN bed_status_history h USING(bed_id) WHERE h.status='ACTIVE' AND h.effective_from<=%s AND h.effective_to IS NULL AND NOT EXISTS(SELECT 1 FROM bed_assignments a WHERE a.bed_id=b.bed_id AND (a.released_at IS NULL OR a.released_at>%s)) ORDER BY b.bed_id LIMIT 1",(window_start,window_start)).fetchone()[0] if full else None
        t=reference-timedelta(seconds=5)
        tm=lambda seconds:stamp(t+timedelta(seconds=seconds))
        def encounter(id,type,parent=None):
            return {'encounter_id':id,'patient_id':patient,'department_id':dept,'doctor_id':doctor,'appointment_id':aid if type=='OUTPATIENT' else None,'parent_encounter_id':parent,'encounter_type':type,'status':'COMPLETED' if type=='OUTPATIENT' else 'DISCHARGED','status_changed_at':tm(2 if type=='OUTPATIENT' else 4),'checked_in_at':tm(0) if type=='OUTPATIENT' else None,'consultation_started_at':tm(1) if type=='OUTPATIENT' else None,'consultation_completed_at':tm(2) if type=='OUTPATIENT' else None,'admitted_at':tm(2) if type=='INPATIENT' else None,'discharged_at':tm(4) if type=='INPATIENT' else None,'created_at':tm(0 if type=='OUTPATIENT' else 2),'updated_at':tm(2 if type=='OUTPATIENT' else 4)}
        op=encounter(eid,'OUTPATIENT')
        lab={'lab_order_id':lid,'encounter_id':eid,'department_id':dept,'lab_test_type_id':test,'target_tat_minutes_at_order':target,'status':'COMPLETED','ordered_at':tm(1.1),'collected_at':tm(1.2),'processing_started_at':tm(1.3),'result_completed_at':tm(4.5),'status_changed_at':tm(4.5),'created_at':tm(1.1),'updated_at':tm(4.5)}
        rows={'encounters':[op],'lab_orders':[lab]}
        contexts={'checked_in':op,'consultation_started':op,'order_created':lab,'sample_collected':lab,'processing_started':lab,'result_completed':lab}
        if full:
            appointment={'appointment_id':aid,'patient_id':patient,'department_id':dept,'doctor_id':doctor,'scheduled_at':tm(0),'status':'ATTENDED','status_changed_at':tm(0),'rescheduled_from_appointment_id':None,'created_at':tm(-3600),'updated_at':tm(0)}
            ip=encounter(ipid,'INPATIENT',eid)
            assignment={'bed_assignment_id':assignment_id,'bed_id':bed,'encounter_id':ipid,'assigned_at':tm(2),'released_at':tm(3),'created_at':tm(2),'updated_at':tm(3)}
            cancelled={**lab,'lab_order_id':cancelled_id,'status':'CANCELLED','collected_at':None,'processing_started_at':None,'result_completed_at':None,'status_changed_at':tm(2),'updated_at':tm(2)}
            rows={'appointments':[appointment],'encounters':[op,ip],'bed_assignments':[assignment],'lab_orders':[lab,cancelled]}
            contexts.update(created=appointment,status_changed=appointment,admitted=ip,discharged=ip,bed_assigned={**ip,**assignment},bed_released={**ip,**assignment},cancelled=cancelled)
        events=[]
        milestones={'created':'created_at','status_changed':'status_changed_at','checked_in':'checked_in_at','consultation_started':'consultation_started_at','admitted':'admitted_at','bed_assigned':'assigned_at','bed_released':'released_at','discharged':'discharged_at','order_created':'ordered_at','sample_collected':'collected_at','processing_started':'processing_started_at','result_completed':'result_completed_at','cancelled':'status_changed_at'}
        for path in sorted((ROOT/'src/streaming/contracts/sample_events').glob('*.json')):
            if int(path.name[:2])>13:continue
            template=json.loads(path.read_text());kind=template['event_type']
            if kind not in contexts:continue
            context=contexts[kind]
            payload={key:context.get(key,patient if key=='patient_id' else value) for key,value in template['payload'].items()}
            for key in ['status','previous_status']:
                if key in template['payload']:payload[key]=template['payload'][key]
            if kind=='created':payload['status_changed_at']=appointment['created_at']
            if kind=='bed_assigned':payload['released_at']=None
            entity=context['appointment_id'] if TOPICS[kind]=='appointments.events' else context['lab_order_id'] if TOPICS[kind]=='laboratory.events' else context['encounter_id']
            event={'event_id':str(uuid.uuid4()),'event_type':kind,'entity_id':entity,'schema_version':'1.0.0','event_time':context[milestones[kind]],'published_at':stamp(reference),'source_system':'hospital_simulator','payload':payload}
            VALIDATOR.validate(event);events.append(event)
        command=str(uuid.uuid4())
        with self.db:
            self.db.execute('INSERT INTO commands VALUES (?,?,?)',(command,'PREPARED',json.dumps({'rows':rows,'events':events})))
        return command

    def prepare_batch(self,count):
        if not 1<=count<=1000:raise ValueError('Burst size must be 1..1000')
        command=self.prepare(False)
        intent=json.loads(self.db.execute('SELECT intent FROM commands WHERE id=?',(command,)).fetchone()[0])
        original_op=intent['rows']['encounters'][0];original_lab=intent['rows']['lab_orders'][0]
        with connect('oltp','writer') as c:
            eids=[original_op['encounter_id']]+[r[0] for r in c.execute("SELECT nextval(pg_get_serial_sequence('public.encounters','encounter_id')) FROM generate_series(1,%s)",(count-1,))]
            lids=[original_lab['lab_order_id']]+[r[0] for r in c.execute("SELECT nextval(pg_get_serial_sequence('public.lab_orders','lab_order_id')) FROM generate_series(1,%s)",(count-1,))]
        rows={'encounters':[],'lab_orders':[]};events=[]
        for eid,lid in zip(eids,lids):
            rows['encounters'].append({**original_op,'encounter_id':eid})
            rows['lab_orders'].append({**original_lab,'encounter_id':eid,'lab_order_id':lid})
            for base in intent['events']:
                event=copy.deepcopy(base);event['event_id']=str(uuid.uuid4())
                event['payload']['encounter_id']=eid
                if 'lab_order_id' in event['payload']:event['payload']['lab_order_id']=lid
                event['entity_id']=eid if TOPICS[event['event_type']]=='encounters.events' else lid
                VALIDATOR.validate(event);events.append(event)
        with self.db:self.db.execute('UPDATE commands SET intent=? WHERE id=?',(json.dumps({'rows':rows,'events':events}),command))
        return command

    def apply(self,command,crash_after_commit=False):
        state,intent=self.db.execute('SELECT state,intent FROM commands WHERE id=?',(command,)).fetchone()
        intent=json.loads(intent)
        if state=='PREPARED':
            statuses=[]
            with connect('oltp','writer') as c:
                for table,rows in intent['rows'].items():
                    pk={'appointments':'appointment_id','encounters':'encounter_id','bed_assignments':'bed_assignment_id','lab_orders':'lab_order_id'}[table]
                    for row in rows:
                        existing=c.execute(sql.SQL('SELECT to_jsonb(t) FROM public.{} t WHERE {}=%s').format(sql.Identifier(table),sql.Identifier(pk)),(row[pk],)).fetchone()
                        if existing:
                            for key,value in row.items():
                                if key=='updated_at':continue
                                actual=existing[0][key]
                                same=parse(actual)==parse(value) if isinstance(value,str) and value.endswith('Z') else actual==value
                                if not same:raise ValueError(f'Journal source conflict: {table}/{row[pk]}/{key}')
                            statuses.append(True)
                        else:statuses.append(False)
                if any(statuses) and not all(statuses):raise ValueError('Partial source effect requires reconciliation')
                if not any(statuses):
                    for table,rows in intent['rows'].items():
                        for row in rows:
                            fields=list(row)
                            c.execute(sql.SQL('INSERT INTO public.{} ({}) VALUES ({})').format(sql.Identifier(table),sql.SQL(',').join(map(sql.Identifier,fields)),sql.SQL(',').join(sql.Placeholder() for _ in fields)),list(row.values()))
            if crash_after_commit:raise RuntimeError('Injected crash after source commit before journal acknowledgement')
            latest=max(parse(event['event_time']) for event in intent['events'])
            for attempt in range(90):
                before=time.monotonic()
                with connect('oltp','reader') as c:published=c.execute('SELECT clock_timestamp()').fetchone()[0]
                reference_mono=(before+time.monotonic())/2
                if published>=latest:break
                time.sleep(2)
            else:raise RuntimeError('Source clock moved backwards for more than 180 seconds; journal retained')
            with self.db:
                for event in intent['events']:
                    event['published_at']=stamp(published)
                    self.publish_references[event['event_id']]=reference_mono
                    self.db.execute('INSERT OR IGNORE INTO messages(event_id,command_id,topic,key_text,body) VALUES (?,?,?,?,?)',(event['event_id'],command,TOPICS[event['event_type']],str(event['entity_id']),body(event)))
                self.db.execute("UPDATE commands SET state='DB_APPLIED' WHERE id=?",(command,))

    def recover(self,publish=True,reverse=False,bootstrap='kafka:9092',ack_timeout=30,prepare_pending=True):
        if prepare_pending:
            for (command,) in self.db.execute("SELECT id FROM commands WHERE state='PREPARED'").fetchall():self.apply(command)
        if not publish:return
        producer=Producer({'bootstrap.servers':bootstrap,'enable.idempotence':True,'acks':'all','delivery.timeout.ms':ack_timeout*1000,'request.timeout.ms':min(10000,ack_timeout*1000)})
        pending=self.db.execute('SELECT event_id,topic,key_text,body FROM messages WHERE published=0 ORDER BY rowid '+('DESC' if reverse else 'ASC')).fetchall()
        delivered={}
        for event_id,topic,key,value in pending:
            producer.produce(topic,key=key.encode(),value=value,on_delivery=lambda error,msg,event_id=event_id:delivered.__setitem__(event_id,error))
        producer.flush(ack_timeout+5)
        with self.db:
            for event_id,error in delivered.items():
                if error is None:self.db.execute('UPDATE messages SET published=1 WHERE event_id=?',(event_id,))
        if any(event_id not in delivered or delivered[event_id] is not None for event_id,_,_,_ in pending):
            raise RuntimeError('Publish failed; immutable journal retained for retry')
        with self.db:self.db.execute("UPDATE commands SET state='PUBLISHED' WHERE state='DB_APPLIED' AND NOT EXISTS(SELECT 1 FROM messages WHERE command_id=commands.id AND published=0)")

    def reconcile(self):
        ids=[r[0] for r in self.db.execute('SELECT event_id FROM messages WHERE published=1')]
        with connect('dwh','consumer') as c:
            existing={str(r[0]) for r in c.execute('SELECT event_id FROM raw.events WHERE event_id=ANY(%s::uuid[])',(ids,))} if ids else set()
        missing=set(ids)-existing
        with self.db:
            for event_id in missing:self.db.execute('UPDATE messages SET published=0 WHERE event_id=?',(event_id,))
        self.recover()
        return len(missing)


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--count',type=int,default=1);p.add_argument('--full-journey',action='store_true');p.add_argument('--rate',type=float,default=2);p.add_argument('--recover-only',action='store_true');a=p.parse_args()
    topics();journal=Journal();journal.recover()
    if not a.recover_only:
        for _ in range(a.count):
            command=journal.prepare(a.full_journey);journal.apply(command);journal.recover();time.sleep(1/max(a.rate,.1))
    print(json.dumps({'pending':journal.db.execute('SELECT count(*) FROM messages WHERE published=0').fetchone()[0],'commands':journal.db.execute('SELECT count(*) FROM commands').fetchone()[0]}))
