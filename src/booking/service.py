"""Loopback demo: durable request -> OLTP appointment -> existing Kafka consumer.

The booking journal has its own volume; source IDs still come from PostgreSQL.
No new business tables or visit/lab events are created by a booking.
"""
import json
import os
import threading
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

from operations.foundation import connect
from streaming.common import VALIDATOR, stamp
from streaming.producer import Journal
from booking.errors import BookingError

LOCK = threading.Lock()
PAGE = Path(__file__).with_name('index.html')
ZONE = ZoneInfo('Asia/Ho_Chi_Minh')


def request_id(value):
    try:
        return str(uuid.UUID(value))
    except (ValueError, TypeError, AttributeError):
        raise BookingError('Mã yêu cầu phải là UUID hợp lệ.')


def normalize(data):
    if not isinstance(data, dict) or set(data) != {'request_id', 'patient_code', 'department_id', 'doctor_id', 'scheduled_at'}:
        raise BookingError('Cần đủ mã yêu cầu, mã bệnh nhân, khoa, bác sĩ và giờ khám.')
    command = request_id(data['request_id'])
    code = data['patient_code']
    if not isinstance(code, str) or not 1 <= len(code.strip()) <= 32:
        raise BookingError('Mã bệnh nhân không hợp lệ.')
    for field in ('department_id', 'doctor_id'):
        if type(data[field]) is not int or data[field] <= 0:
            raise BookingError('Khoa và bác sĩ phải là mã hợp lệ.')
    try:
        scheduled = datetime.fromisoformat(data['scheduled_at'].replace('Z', '+00:00'))
        if scheduled.tzinfo is None or scheduled.utcoffset() is None:
            raise ValueError('offset required')
    except (ValueError, TypeError, AttributeError, OverflowError):
        raise BookingError('Giờ khám phải có múi giờ, ví dụ +07:00.')
    return command, {'patient_code': code.strip(), 'department_id': data['department_id'],
                     'doctor_id': data['doctor_id'], 'scheduled_at': stamp(scheduled.astimezone(timezone.utc))}


@contextmanager
def journal():
    # Each SQLite connection is opened and closed in the calling thread.
    with LOCK:
        j = Journal(os.environ.get('BOOKING_JOURNAL_PATH', '/var/lib/hospital-booking/journal.sqlite3'))
        try:
            yield j
        finally:
            j.db.close()
            j.lock.close()


def prepare(j, command, request):
    existing = j.db.execute('SELECT intent FROM commands WHERE id=?', (command,)).fetchone()
    if existing:
        intent = json.loads(existing[0])
        if intent.get('booking_request') != request:
            raise BookingError('Mã yêu cầu đã dùng cho nội dung khác. Hãy tạo lịch hẹn mới.', 409)
        return intent
    with connect('oltp', 'writer') as c:
        patient = c.execute('SELECT patient_id FROM patients WHERE patient_code=%s AND is_active', (request['patient_code'],)).fetchone()
        if not patient:
            raise BookingError('Không tìm thấy bệnh nhân đang hoạt động. Dùng mã bệnh nhân mẫu có sẵn.')
        doctor = c.execute('SELECT d.doctor_id FROM doctors d JOIN departments p USING(department_id) WHERE d.doctor_id=%s AND d.department_id=%s AND d.is_active AND p.is_active',
                           (request['doctor_id'], request['department_id'])).fetchone()
        if not doctor:
            raise BookingError('Bác sĩ không thuộc khoa đã chọn hoặc danh mục đã ngừng hoạt động.')
        now = c.execute('SELECT clock_timestamp()').fetchone()[0]
        scheduled = datetime.fromisoformat(request['scheduled_at'].replace('Z', '+00:00'))
        from booking.reception import validate_slot
        validate_slot(now, scheduled)
        aid = c.execute("SELECT nextval(pg_get_serial_sequence('public.appointments','appointment_id'))").fetchone()[0]
    created = stamp(now)
    row = {'appointment_id': aid, 'patient_id': patient[0], 'department_id': request['department_id'],
           'doctor_id': doctor[0], 'scheduled_at': request['scheduled_at'], 'status': 'SCHEDULED',
           'status_changed_at': created, 'rescheduled_from_appointment_id': None,
           'created_at': created, 'updated_at': created}
    payload = {key: value for key, value in row.items() if key not in ('created_at', 'updated_at')}
    event = {'event_id': str(uuid.uuid4()), 'event_type': 'created', 'entity_id': aid,
             'schema_version': '1.0.0', 'event_time': created, 'published_at': created,
             'source_system': 'hospital_simulator', 'payload': payload}
    VALIDATOR.validate(event)
    intent = {'booking_request': request, 'rows': {'appointments': [row]}, 'events': [event]}
    with j.db:
        j.db.execute('INSERT INTO commands VALUES (?,?,?)', (command, 'PREPARED', json.dumps(intent)))
    return intent


def submit(data):
    command, request = normalize(data)
    with journal() as j:
        prepare(j, command, request)
        j.apply(command)
    return status(command)


def status(command):
    command = request_id(command)
    with journal() as j:
        result = j.db.execute('SELECT state,intent FROM commands WHERE id=?', (command,)).fetchone()
    if not result:
        raise BookingError('Không tìm thấy yêu cầu đặt lịch.', 404)
    state, raw = result
    intent = json.loads(raw)
    row = intent['rows']['appointments'][0]
    event_id = intent['events'][0]['event_id']
    with connect('oltp', 'reader') as c:
        source = c.execute('SELECT status FROM appointments WHERE appointment_id=%s', (row['appointment_id'],)).fetchone()
    answer = {'request_id': command, 'appointment_id': row['appointment_id'], 'event_id': event_id,
              'scheduled_at': row['scheduled_at'], 'status': source[0] if source else 'PENDING',
              'source_saved': bool(source), 'broker_acknowledged': state == 'PUBLISHED',
              'raw_received': None, 'warehouse_received': None,
              'rt_applicable': False, 'rt_processing_status': 'NOT_APPLICABLE',
              'rt_reason': 'Sự kiện tạo lịch hẹn không áp dụng cho KPI RT check-in/xét nghiệm.'}
    # DWH unavailability must not turn an already committed booking into a failure.
    try:
        with connect('dwh', 'consumer') as c:
            answer['raw_received'] = c.execute('SELECT EXISTS(SELECT 1 FROM raw.events WHERE event_id=%s)', (event_id,)).fetchone()[0]
    except Exception as exc:
        answer['analytics_check_error'] = type(exc).__name__
    # Staging is a batch concern; use ETL role, never grant raw/stg to reporter.
    try:
        with connect('dwh', 'etl') as c:
            answer['warehouse_received'] = c.execute('SELECT EXISTS(SELECT 1 FROM dw.fact_appointment WHERE appointment_id=%s)', (row['appointment_id'],)).fetchone()[0]
    except Exception as exc:
        answer['warehouse_check_error'] = type(exc).__name__
    return answer


def catalogs():
    from booking.directory import specialty, doctor_name
    from booking.reception import PURPOSES
    with connect('oltp', 'reader') as c:
        departments = [{'id': r[0], 'name': specialty(r[0])} for r in c.execute('SELECT department_id,department_name FROM departments WHERE is_active ORDER BY department_id')]
        doctors = [{'id': r[0], 'code': r[1], 'department_id': r[2], 'name': doctor_name(r[0])} for r in c.execute('SELECT doctor_id,doctor_code,department_id FROM doctors WHERE is_active ORDER BY doctor_id')]
        example = c.execute('SELECT patient_code FROM patients WHERE is_active ORDER BY patient_id LIMIT 1').fetchone()[0]
        now = c.execute('SELECT clock_timestamp()').fetchone()[0]
    return {'departments': departments, 'doctors': doctors, 'example_patient_code': example,
            'default_slot_local': (now.astimezone(ZONE) + timedelta(days=1)).replace(second=0, microsecond=0).strftime('%Y-%m-%dT%H:%M'),
            'timezone': 'Asia/Ho_Chi_Minh', 'server_now': stamp(now), 'purposes': PURPOSES}


def recover_bookings(j):
    for (command,) in j.db.execute("SELECT id FROM commands WHERE state='PREPARED'").fetchall():
        try:
            j.apply(command)
        except Exception as exc:
            # A source conflict stays durable for manual reconciliation;
            # it must not block publication for unrelated appointments.
            print(json.dumps({'booking_recovery_error': type(exc).__name__}), flush=True)
    j.recover(ack_timeout=3, prepare_pending=False)


def publish_pending(stop):
    while not stop.is_set():
        try:
            from booking.reception import publish, expire_bookings
            expire_bookings()
            publish()
            with journal() as j:
                if j.db.execute("SELECT count(*) FROM commands WHERE state!='PUBLISHED'").fetchone()[0]:
                    recover_bookings(j)
        except Exception as exc:
            print(json.dumps({'booking_worker_error': type(exc).__name__, 'retry': True}), flush=True)
        stop.wait(3)


class Handler(BaseHTTPRequestHandler):
    server_version = 'HospitalBookingDemo/1.0'

    def log_message(self, *args):
        pass  # Request bodies, patient codes and capability IDs stay out of logs.

    def send(self, code, data, content_type='application/json; charset=utf-8'):
        content = data if isinstance(data, bytes) else json.dumps(data, ensure_ascii=False).encode()
        self.send_response(code)
        self.send_header('Content-Type', content_type)
        self.send_header('Content-Length', str(len(content)))
        self.send_header('Cache-Control', 'no-store')
        self.send_header('X-Content-Type-Options', 'nosniff')
        self.send_header('Referrer-Policy', 'no-referrer')
        self.send_header('Content-Security-Policy', "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'")
        self.end_headers()
        self.wfile.write(content)

    def allowed_host(self):
        host = self.headers.get('Host', '').lower()
        # Prevent browser DNS rebinding. The host port is configurable in Compose.
        return host in {f'localhost:{os.environ.get("BOOKING_HOST_PORT", "18081")}',
                        f'127.0.0.1:{os.environ.get("BOOKING_HOST_PORT", "18081")}', 'localhost:8080', '127.0.0.1:8080'}

    def do_GET(self):
        if not self.allowed_host():
            return self.send(403, {'error': 'Host không được phép.'})
        path = urlparse(self.path).path
        try:
            if path == '/':
                return self.send(200, PAGE.read_bytes(), 'text/html; charset=utf-8')
            if path == '/health':
                return self.send(200, {'status': 'running', 'mode': 'local_synthetic_demo'})
            if path == '/api/catalogs':
                return self.send(200, catalogs())
            if path.startswith('/api/registrations/'):
                from booking.reception import status as registration_status
                return self.send(200, registration_status(path.rsplit('/', 1)[-1]))
            if path.startswith('/api/bookings/'):
                return self.send(200, status(path.rsplit('/', 1)[-1]))
            return self.send(404, {'error': 'Không tìm thấy.'})
        except BookingError as exc:
            self.send(exc.status, {'error': str(exc)})
        except Exception as exc:
            self.send(503, {'error': 'Dịch vụ tạm gián đoạn. Giữ mã yêu cầu và thử lại.', 'error_type': type(exc).__name__})

    def do_POST(self):
        origin = self.headers.get('Origin')
        if not self.allowed_host() or (origin is not None and origin != 'http://' + self.headers.get('Host', '')):
            return self.send(403, {'error': 'Chỉ nhận yêu cầu từ form local.'})
        is_checkin = self.path.startswith('/api/registrations/') and self.path.endswith('/check-in')
        if self.path not in ('/api/bookings', '/api/registrations') and not is_checkin:
            return self.send(404, {'error': 'Không tìm thấy.'})
        if self.headers.get('Content-Type', '').split(';')[0].strip() != 'application/json':
            return self.send(415, {'error': 'Yêu cầu phải là JSON.'})
        try:
            length = int(self.headers.get('Content-Length', '0'))
            if not 0 < length <= 8192:
                return self.send(413, {'error': 'Nội dung không hợp lệ hoặc quá lớn.'})
            data = json.loads(self.rfile.read(length))
        except (json.JSONDecodeError, UnicodeDecodeError, ValueError):
            return self.send(400, {'error': 'JSON không hợp lệ.'})
        try:
            if self.path == '/api/registrations':
                from booking.reception import submit as register
                return self.send(201, register(data))
            if is_checkin:
                from booking.reception import checkin
                return self.send(200, checkin(self.path.split('/')[-2]))
            return self.send(201, submit(data))
        except BookingError as exc:
            self.send(exc.status, {'error': str(exc)})
        except Exception as exc:
            self.send(503, {'error': 'Chưa xác nhận được. Thử lại cùng mã yêu cầu để tránh tạo trùng.', 'error_type': type(exc).__name__})


def main():
    stop = threading.Event()
    worker = threading.Thread(target=publish_pending, args=(stop,), daemon=True)
    worker.start()
    server = ThreadingHTTPServer(('0.0.0.0', 8080), Handler)
    print('Booking demo ready on container port 8080; host bind is loopback only.', flush=True)
    try:
        server.serve_forever()
    finally:
        stop.set()
        server.server_close()


if __name__ == '__main__':
    main()
