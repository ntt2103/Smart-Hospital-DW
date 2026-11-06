"""Acceptance against live local API + SQL/Kafka; creates only synthetic bookings."""
import json
import os
import tempfile
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from booking.service import prepare, normalize
from operations.foundation import ROOT, connect
from streaming.producer import Journal

BASE = os.environ.get('BOOKING_URL','http://booking:8080')


def http(path, data=None, headers=None):
    headers = {'Host': f"localhost:{os.environ.get('BOOKING_HOST_PORT','18081')}", **(headers or {})}
    if data is not None:
        headers.setdefault('Content-Type', 'application/json')
    request = Request(BASE + path, data=None if data is None else json.dumps(data).encode(), headers=headers)
    try:
        with urlopen(request, timeout=40) as r:
            content = r.read()
            return r.status, json.loads(content) if r.headers.get_content_type() == 'application/json' else content.decode()
    except HTTPError as exc:
        return exc.code, json.loads(exc.read())


def wait_for(command, field='raw_received', timeout=45):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        code, result = http('/api/bookings/' + command)
        if code == 200 and result[field]:
            return result
        time.sleep(.5)
    raise AssertionError(f'{field} not observed for {command}')


def run(stage):
    out = ROOT / 'artifacts/phase5/booking_demo.json'
    out.parent.mkdir(parents=True,exist_ok=True)
    if stage == 'before':
        code, page = http('/')
        assert code == 200 and '<form' in page
        code, catalogs = http('/api/catalogs')
        assert code == 200 and catalogs['departments'] and catalogs['doctors']
        doctor = catalogs['doctors'][0]
        with connect('oltp', 'reader') as c:
            now = c.execute('SELECT clock_timestamp()').fetchone()[0]
            before = c.execute('SELECT count(*) FROM appointments').fetchone()[0]
        payload = {'request_id': str(uuid.uuid4()), 'patient_code': catalogs['example_patient_code'],
                   'department_id': doctor['department_id'], 'doctor_id': doctor['id'],
                   'scheduled_at': (now + timedelta(days=30)).astimezone(timezone(timedelta(hours=7))).isoformat()}
        code, result = http('/api/bookings', payload)
        assert code == 201 and result['source_saved'] and result['status'] == 'SCHEDULED', (code, result)
        with ThreadPoolExecutor(max_workers=4) as pool:
            retried = list(pool.map(lambda _: http('/api/bookings', payload), range(4)))
        assert all(code == 201 and r['appointment_id'] == result['appointment_id'] for code, r in retried)
        equivalent = datetime.fromisoformat(payload['scheduled_at']).astimezone(timezone.utc).isoformat().replace('+00:00', 'Z')
        code, same_time = http('/api/bookings', {**payload, 'scheduled_at': equivalent})
        assert code == 201 and same_time['appointment_id'] == result['appointment_id']
        assert http('/api/bookings', {**payload, 'doctor_id': 999999})[0] == 409
        def invalid(changes, expected=422):
            code, data = http('/api/bookings', {**payload, 'request_id': str(uuid.uuid4()), **changes})
            assert code == expected, (changes, code, data)
        invalid({'patient_code': '<script>fake</script>'})
        invalid({'doctor_id': 999999})
        wrong_department = next(d['id'] for d in catalogs['departments'] if d['id'] != doctor['department_id'])
        invalid({'department_id': wrong_department})
        invalid({'scheduled_at': (now - timedelta(days=1)).isoformat()})
        invalid({'scheduled_at': (now + timedelta(days=366)).isoformat()})
        invalid({'scheduled_at': '2026-12-01T09:00:00'})
        invalid({'doctor_id': True})
        invalid({'request_id': 'not-a-uuid'})
        assert http('/api/bookings', payload, {'Origin': 'https://untrusted.example'})[0] == 403
        assert http('/api/bookings', payload, {'Content-Type': 'text/plain'})[0] == 415
        assert http('/api/catalogs', headers={'Host': 'evil.example:18081'})[0] == 403
        with connect('oltp', 'reader') as c:
            assert c.execute('SELECT count(*) FROM appointments').fetchone()[0] == before + 1
            assert c.execute('SELECT count(*) FROM encounters WHERE appointment_id=%s', (result['appointment_id'],)).fetchone()[0] == 0
        result = wait_for(payload['request_id'])
        assert result['broker_acknowledged']
        # Exercise the same durable command format, including +07:00 -> UTC recovery.
        recovery_ids = []
        with tempfile.TemporaryDirectory(prefix='booking-acceptance-') as directory:
            j = Journal(Path(directory) / 'journal.sqlite3')
            try:
                request = {**payload, 'request_id': str(uuid.uuid4())}
                command, normalized = normalize(request)
                intent = prepare(j, command, normalized)
                try:
                    j.apply(command, crash_after_commit=True)
                except RuntimeError:
                    pass
                else:
                    raise AssertionError('Crash injection did not fire')
                with connect('oltp', 'reader') as c:
                    count = c.execute('SELECT count(*) FROM appointments').fetchone()[0]
                j.recover(publish=False)
                with connect('oltp', 'reader') as c:
                    assert c.execute('SELECT count(*) FROM appointments').fetchone()[0] == count
                event_id = intent['events'][0]['event_id']
                original = j.db.execute('SELECT body FROM messages WHERE event_id=?', (event_id,)).fetchone()[0]
                try:
                    j.recover(bootstrap='kafka:1', ack_timeout=3)
                except RuntimeError:
                    pass
                else:
                    raise AssertionError('Unavailable broker unexpectedly succeeded')
                assert j.db.execute('SELECT published FROM messages WHERE event_id=?', (event_id,)).fetchone()[0] == 0
                j.recover()
                assert j.db.execute('SELECT body FROM messages WHERE event_id=?', (event_id,)).fetchone()[0] == original
                recovery_ids.append({'appointment_id': intent['rows']['appointments'][0]['appointment_id'], 'event_id': event_id})
                deadline = time.monotonic() + 45
                while time.monotonic() < deadline:
                    with connect('dwh', 'consumer') as c:
                        if c.execute('SELECT EXISTS(SELECT 1 FROM raw.events WHERE event_id=%s)', (event_id,)).fetchone()[0]:
                            break
                    time.sleep(.5)
                else:
                    raise AssertionError('Recovery event not consumed')
            finally:
                j.db.close()
                j.lock.close()
        report = {'local_api_and_html': True, 'booking_saved_in_oltp': True,
                  'four_concurrent_retries_one_appointment': True, 'idempotency_content_conflict_rejected': True,
                  'invalid_patient_doctor_department_datetime_rejected': True, 'csrf_host_and_content_type_rejected': True,
                  'booking_does_not_create_encounter': True, 'created_event_ingested': True,
                  'sql_commit_crash_recovered_without_duplicate': True, 'broker_failure_keeps_immutable_message': True,
                  'main_booking': result, 'recovery_bookings': recovery_ids,
                  'future_scheduled_date': payload['scheduled_at'][:10]}
        out.write_text(json.dumps(report, indent=2), encoding='utf-8')
    else:
        report = json.loads(out.read_text())
        rows = [report['main_booking'], *report['recovery_bookings']]
        with connect('dwh', 'etl') as c:
            for row in rows:
                value = c.execute('SELECT a.status,d.calendar_date,a.is_noshow_eligible FROM dw.fact_appointment a JOIN dw.dim_date d ON d.date_key=a.scheduled_date_key WHERE a.appointment_id=%s', (row['appointment_id'],)).fetchone()
                assert value and value[0] == 'SCHEDULED' and str(value[1]) == report['future_scheduled_date'] and not value[2], value
                assert c.execute('SELECT count(*) FROM mart.event_visibility WHERE event_id=%s', (row['event_id'],)).fetchone()[0] == 0
            assert c.execute("SELECT count(*) FROM dw.fact_bed_snapshot_daily f JOIN dw.dim_date d ON d.date_key=f.snapshot_date_key WHERE d.calendar_date>(clock_timestamp() AT TIME ZONE 'Asia/Ho_Chi_Minh')::date").fetchone()[0] == 0
        result = wait_for(report['main_booking']['request_id'], 'warehouse_received')
        assert result['rt_applicable'] is False and result['rt_processing_status'] == 'NOT_APPLICABLE'
        assert 'stream_fact_received' not in result
        report.update(future_appointment_date_dimension_and_fact_verified=True, booking_created_rt_not_applicable_verified=True,
                      scheduled_excluded_from_no_show=True, no_future_bed_snapshots=True, final_status=result)
        out.write_text(json.dumps(report, indent=2), encoding='utf-8')
    print(json.dumps(report))


if __name__ == '__main__':
    import sys
    run(sys.argv[1] if len(sys.argv) > 1 else 'before')
