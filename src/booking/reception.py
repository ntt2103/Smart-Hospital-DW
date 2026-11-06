"""Transactional reception workflow and source-side durable Kafka outbox."""
import json
import uuid
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from psycopg.types.json import Jsonb
from operations.foundation import connect
from streaming.common import VALIDATOR, TOPICS, stamp

PURPOSES = {'GENERAL': 'Khám sức khỏe tổng quát', 'FOLLOW_UP': 'Tái khám theo lịch hẹn',
            'SYMPTOMS': 'Khám vì có triệu chứng', 'PERIODIC': 'Khám sức khỏe định kỳ'}


def normalized(data):
    from booking.service import normalize, BookingError
    if not isinstance(data, dict) or set(data) != {
        'request_id', 'patient_type', 'patient_name', 'patient_code', 'purpose',
        'department_id', 'doctor_id', 'scheduled_at'
    }:
        raise BookingError('Vui lòng điền đủ thông tin đăng ký.')
    if data['patient_type'] not in ('FIRST', 'RETURNING'):
        raise BookingError('Chọn khám lần đầu hoặc đã có mã bệnh nhân.')
    name = data['patient_name']
    if not isinstance(name, str) or not 2 <= len(name.strip()) <= 120 or any(ord(c) < 32 for c in name):
        raise BookingError('Họ tên phải có từ 2 đến 120 ký tự.')
    if not isinstance(data['purpose'], str) or data['purpose'] not in PURPOSES:
        raise BookingError('Chọn mục đích khám hợp lệ.')
    if data['patient_type'] == 'FIRST' and (data['patient_code'] or data['purpose'] == 'FOLLOW_UP'):
        raise BookingError('Khám lần đầu không dùng mã bệnh nhân hoặc mục đích tái khám.')
    command, request = normalize({key: data[key] for key in
        ('request_id', 'department_id', 'doctor_id', 'scheduled_at')} |
        {'patient_code': data['patient_code'] if data['patient_type'] == 'RETURNING' else 'NEW'})
    request.update(patient_type=data['patient_type'], patient_name=name.strip(),
                   purpose=data['purpose'], patient_code=request['patient_code'] if data['patient_type'] == 'RETURNING' else '')
    return command, request


def validate_slot(now, scheduled):
    from booking.service import BookingError
    if not now + timedelta(minutes=30) <= scheduled <= now + timedelta(days=365):
        raise BookingError('Phải đặt trước giờ hẹn ít nhất 30 phút và không quá 365 ngày.')


def validate_checkin(now, scheduled):
    from booking.errors import BookingError
    opens = scheduled - timedelta(minutes=30)
    closes = scheduled + timedelta(minutes=30)
    zone = ZoneInfo('Asia/Ho_Chi_Minh')
    if now < opens:
        raise BookingError('Chưa đến giờ check-in. Có thể tiếp nhận từ '
                           + opens.astimezone(zone).strftime('%H:%M ngày %d/%m/%Y')
                           + ' (30 phút trước giờ hẹn).')
    if now > closes:
        raise BookingError('Đã quá thời gian check-in: '
                           + closes.astimezone(zone).strftime('%H:%M ngày %d/%m/%Y')
                           + ' (30 phút sau giờ hẹn). Vui lòng liên hệ quầy lễ tân.')


def event(connection, kind, entity, payload, now):
    value = dict(event_id=str(uuid.uuid4()), event_type=kind, entity_id=entity,
                 schema_version='1.0.0', event_time=stamp(now), published_at=stamp(now),
                 source_system='hospital_simulator', payload=payload)
    VALIDATOR.validate(value)
    connection.execute('INSERT INTO booking_event_outbox(event_id,event_body) VALUES (%s,%s)',
                       (value['event_id'], Jsonb(value)))


def appointment(connection, request, patient, now):
    row = connection.execute('''INSERT INTO appointments
        (patient_id,department_id,doctor_id,scheduled_at,status,status_changed_at,created_at,updated_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s) RETURNING to_jsonb(appointments)''',
        (patient, request['department_id'], request['doctor_id'], request['scheduled_at'],
         'SCHEDULED', now, now, now)).fetchone()[0]
    payload = {key: value for key, value in row.items() if key not in ('created_at', 'updated_at')}
    for key in ('scheduled_at', 'status_changed_at'):
        payload[key] = stamp(datetime.fromisoformat(payload[key]))
    event(connection, 'created', row['appointment_id'], payload, now)
    return row['appointment_id']


def provisional_patient(connection, now):
    patient = connection.execute("SELECT nextval(pg_get_serial_sequence('patients','patient_id'))").fetchone()[0]
    connection.execute("""INSERT INTO patients(patient_id,patient_code,date_of_birth,gender,created_at,updated_at)
        VALUES (%s,%s,NULL,'UNKNOWN',%s,%s)""", (patient, f'TMP-{patient:08d}', now, now))
    return patient


def status_event(connection, aid, previous, now):
    row = connection.execute('SELECT to_jsonb(a) FROM appointments a WHERE appointment_id=%s', (aid,)).fetchone()[0]
    payload = {key: value for key,value in row.items() if key not in ('created_at','updated_at')}
    for key in ('scheduled_at','status_changed_at'):
        payload[key] = stamp(datetime.fromisoformat(payload[key]))
    event(connection,'status_changed',aid,payload | dict(previous_status=previous),now)


def submit(data):
    from booking.service import BookingError
    command, request = normalized(data)
    with connect('oltp', 'writer') as c:
        c.execute('SELECT pg_advisory_xact_lock(hashtextextended(%s,0))', (command,))
        existing = c.execute('SELECT request FROM booking_registrations WHERE request_id=%s', (command,)).fetchone()
        if existing:
            if existing[0] != request:
                raise BookingError('Mã yêu cầu đã dùng cho nội dung khác.', 409)
        else:
            now = c.execute('SELECT clock_timestamp()').fetchone()[0]
            validate_slot(now, datetime.fromisoformat(request['scheduled_at'].replace('Z', '+00:00')))
            if not c.execute('''SELECT 1 FROM doctors d JOIN departments p USING(department_id)
                WHERE d.doctor_id=%s AND d.department_id=%s AND d.is_active AND p.is_active''',
                (request['doctor_id'], request['department_id'])).fetchone():
                raise BookingError('Bác sĩ không thuộc chuyên khoa đã chọn hoặc đã ngừng hoạt động.')
            patient = aid = None
            if request['patient_type'] == 'RETURNING':
                row = c.execute('SELECT patient_id FROM patients WHERE patient_code=%s AND is_active',
                                (request['patient_code'],)).fetchone()
                if not row:
                    raise BookingError('Không tìm thấy mã bệnh nhân đang hoạt động. Vui lòng kiểm tra lại.')
                patient = row[0]
            else:
                patient = provisional_patient(c, now)
            aid = appointment(c, request, patient, now)
            c.execute('''INSERT INTO booking_registrations(request_id,request,patient_id,appointment_id)
                VALUES (%s,%s,%s,%s)''', (command, Jsonb(request), patient, aid))
    return status(command)


def status(command):
    from booking.service import request_id, BookingError
    command = request_id(command)
    with connect('oltp', 'reader') as c:
        row = c.execute('''SELECT r.request,r.patient_id,r.appointment_id,r.encounter_id,p.patient_code,a.status
            FROM booking_registrations r LEFT JOIN patients p USING(patient_id)
            LEFT JOIN appointments a USING(appointment_id) WHERE request_id=%s''', (command,)).fetchone()
    if not row:
        raise BookingError('Không tìm thấy mã đăng ký.', 404)
    request, patient, aid, encounter, code, appointment_status = row
    awaiting_code = request['patient_type'] == 'FIRST' and not encounter
    return dict(request | dict(patient_code='' if awaiting_code else (code or request['patient_code'])), request_id=command, registration_saved=True, patient_id=None if awaiting_code else patient,
                appointment_id=aid, encounter_id=encounter,
                status='CHECKED_IN' if encounter else (appointment_status or 'REGISTERED'),
                awaiting_patient_code=awaiting_code)


def checkin(command):
    from booking.service import request_id, BookingError
    command = request_id(command)
    with connect('oltp', 'writer') as c:
        row = c.execute('SELECT request,patient_id,appointment_id,encounter_id FROM booking_registrations WHERE request_id=%s FOR UPDATE', (command,)).fetchone()
        if not row:
            raise BookingError('Không tìm thấy mã đăng ký.', 404)
        request, patient, aid, encounter = row
        if not encounter:
            now = c.execute('SELECT clock_timestamp()').fetchone()[0]
            scheduled = datetime.fromisoformat(request['scheduled_at'].replace('Z', '+00:00'))
            validate_checkin(now, scheduled)
            if aid:
                current = c.execute('SELECT status FROM appointments WHERE appointment_id=%s FOR UPDATE', (aid,)).fetchone()[0]
                if current != 'SCHEDULED':
                    raise BookingError('Lịch hẹn không còn ở trạng thái chờ tiếp nhận: '+current, 409)
            if patient is None:
                patient = c.execute("SELECT nextval(pg_get_serial_sequence('patients','patient_id'))").fetchone()[0]
                c.execute('''INSERT INTO patients(patient_id,patient_code,date_of_birth,gender,created_at,updated_at)
                    VALUES (%s,%s,NULL,'UNKNOWN',%s,%s)''', (patient, f'BN-{patient:08d}', now, now))
            if request['patient_type'] == 'FIRST':
                c.execute('UPDATE patients SET patient_code=%s WHERE patient_id=%s', (f'BN-{patient:08d}',patient))
            if aid is None:
                aid = appointment(c, request, patient, now)
            c.execute("UPDATE appointments SET status='ATTENDED',status_changed_at=%s,updated_at=%s WHERE appointment_id=%s", (now, now, aid))
            status_event(c, aid, 'SCHEDULED', now)
            encounter = c.execute('''INSERT INTO encounters(patient_id,department_id,doctor_id,appointment_id,
                encounter_type,status,status_changed_at,checked_in_at,created_at,updated_at)
                VALUES (%s,%s,%s,%s,'OUTPATIENT','CHECKED_IN',%s,%s,%s,%s) RETURNING encounter_id''',
                (patient,request['department_id'],request['doctor_id'],aid,now,now,now,now)).fetchone()[0]
            event(c, 'checked_in', encounter, dict(encounter_id=encounter,patient_id=patient,
                department_id=request['department_id'],doctor_id=request['doctor_id'],appointment_id=aid,
                parent_encounter_id=None,encounter_type='OUTPATIENT',checked_in_at=stamp(now)), now)
            c.execute('UPDATE booking_registrations SET patient_id=%s,appointment_id=%s,encounter_id=%s WHERE request_id=%s',
                      (patient,aid,encounter,command))
    return status(command)


def expire_bookings():
    """Migrate pending registrations and expire only form-managed appointments.

    Registration row locks serialize this with check-in; updates/events commit
    together. Seed appointments are not automatically reclassified.
    """
    changed = 0
    with connect('oltp', 'writer') as c:
        pending = c.execute('''SELECT request_id,request,patient_id,created_at FROM booking_registrations
            WHERE appointment_id IS NULL ORDER BY created_at LIMIT 100 FOR UPDATE SKIP LOCKED''').fetchall()
        for command, request, patient, created in pending:
            patient = patient or provisional_patient(c,created)
            aid = appointment(c,request,patient,created)
            c.execute('UPDATE booking_registrations SET patient_id=%s,appointment_id=%s WHERE request_id=%s', (patient,aid,command))
        rows = c.execute('''SELECT r.request_id,r.appointment_id FROM booking_registrations r
            JOIN appointments a USING(appointment_id)
            WHERE r.encounter_id IS NULL AND a.status='SCHEDULED'
              AND a.scheduled_at+interval '30 minutes'<clock_timestamp()
            ORDER BY a.scheduled_at LIMIT 100 FOR UPDATE OF r SKIP LOCKED''').fetchall()
        for command, aid in rows:
            current = c.execute('SELECT status,scheduled_at FROM appointments WHERE appointment_id=%s FOR UPDATE', (aid,)).fetchone()
            now = c.execute('SELECT clock_timestamp()').fetchone()[0]
            if current[0] != 'SCHEDULED' or now <= current[1]+timedelta(minutes=30):
                continue
            if c.execute('SELECT 1 FROM encounters WHERE appointment_id=%s', (aid,)).fetchone():
                continue
            c.execute("UPDATE appointments SET status='NO_SHOW',status_changed_at=%s WHERE appointment_id=%s", (now,aid))
            status_event(c,aid,'SCHEDULED',now)
            changed += 1
    return changed


def publish():
    from confluent_kafka import Producer
    with connect('oltp', 'writer') as c:
        rows = c.execute('SELECT event_id,event_body FROM booking_event_outbox WHERE NOT published ORDER BY event_id LIMIT 100 FOR UPDATE SKIP LOCKED').fetchall()
        if not rows:
            return
        producer = Producer({'bootstrap.servers':'kafka:9092','enable.idempotence':True,'acks':'all','delivery.timeout.ms':10000})
        delivered = {}
        for identifier, value in rows:
            producer.produce(TOPICS[value['event_type']], key=str(value['entity_id']),
                value=json.dumps(value).encode(), on_delivery=lambda error,msg,key=identifier: delivered.__setitem__(key,error))
        producer.flush(12)
        for identifier, error in delivered.items():
            if error is None:
                c.execute('UPDATE booking_event_outbox SET published=TRUE WHERE event_id=%s', (identifier,))
