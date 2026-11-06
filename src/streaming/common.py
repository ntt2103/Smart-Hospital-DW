import json
import hashlib
from datetime import datetime,timezone
from jsonschema import Draft202012Validator,FormatChecker
from operations.foundation import ROOT

SCHEMA=json.loads((ROOT/'src/streaming/contracts/event.schema.json').read_text())
VALIDATOR=Draft202012Validator(SCHEMA,format_checker=FormatChecker())
TOPICS={'created':'appointments.events','status_changed':'appointments.events',
        **{t:'encounters.events' for t in ['checked_in','consultation_started','admitted','bed_assigned','bed_released','discharged']},
        **{t:'laboratory.events' for t in ['order_created','sample_collected','processing_started','result_completed','cancelled']}}


def utc():return datetime.now(timezone.utc)
def stamp(t):return t.isoformat().replace('+00:00','Z')
def parse(t):return datetime.fromisoformat(t.replace('Z','+00:00'))
def body(event):return json.dumps(event,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()
def sha(event):return hashlib.sha256(body(event)).hexdigest()


def validate(event,topic,key,source):
    VALIDATOR.validate(event)
    if TOPICS[event['event_type']]!=topic or str(event['entity_id'])!=key:raise ValueError('Topic/key/entity mismatch')
    if parse(event['published_at'])<parse(event['event_time']):raise ValueError('Publish before business event')
    reference=source.execute('SELECT clock_timestamp()').fetchone()[0]
    if (parse(event['event_time'])-reference).total_seconds()>30:raise ValueError('Future event exceeds source database clock tolerance')
    p=event['payload'];etype=event['event_type']
    if topic=='appointments.events':table,pk='appointments','appointment_id'
    elif topic=='encounters.events':table,pk='encounters','encounter_id'
    else:table,pk='lab_orders','lab_order_id'
    from psycopg import sql
    row=source.execute(sql.SQL('SELECT to_jsonb(t) FROM public.{} t WHERE {}=%s').format(sql.Identifier(table),sql.Identifier(pk)),(event['entity_id'],)).fetchone()
    if not row:raise ValueError('Entity not present in canonical source')
    row=row[0]
    for column in ['department_id','patient_id','encounter_id','appointment_id','parent_encounter_id','doctor_id','lab_test_type_id','target_tat_minutes_at_order']:
        if column in p and column in row and p[column]!=row[column]:raise ValueError(f'Source context conflict: {column}')
    if topic=='laboratory.events':
        context=source.execute('SELECT patient_id FROM encounters WHERE encounter_id=%s',(p['encounter_id'],)).fetchone()
        if not context or context[0]!=p['patient_id']:raise ValueError('Lab patient mismatch')
    milestone={'created':'created_at','status_changed':'status_changed_at','checked_in':'checked_in_at','consultation_started':'consultation_started_at','admitted':'admitted_at','bed_assigned':'assigned_at','bed_released':'released_at','discharged':'discharged_at','order_created':'ordered_at','sample_collected':'collected_at','processing_started':'processing_started_at','result_completed':'result_completed_at','cancelled':'status_changed_at'}[etype]
    if etype in ['bed_assigned','bed_released']:
        assignment=source.execute('SELECT to_jsonb(a) FROM bed_assignments a WHERE bed_assignment_id=%s',(p['bed_assignment_id'],)).fetchone()
        if not assignment or any(assignment[0][k]!=p[k] for k in ['bed_id','encounter_id']):raise ValueError('Assignment mismatch')
        row=assignment[0]
    if not row.get(milestone) or parse(row[milestone])!=parse(event['event_time']):raise ValueError('Event milestone does not match canonical source')
