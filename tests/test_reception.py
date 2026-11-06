"""Reception contracts and end-to-end SQL workflow in a disposable database."""
import os
import uuid
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb
from operations.foundation import ROOT
from booking import reception
from booking.service import BookingError
from streaming.common import validate


class ReceptionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.database = 'booking_test_' + uuid.uuid4().hex
        cls.options = dict(host='postgres-oltp', user='postgres', password=os.environ['OLTP_ADMIN_PASSWORD'])
        with psycopg.connect(**cls.options, dbname='hospital_oltp', autocommit=True) as c:
            c.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(cls.database)))
        with cls.connection() as c:
            c.execute((ROOT/'sql/design/source_schema_draft.sql').read_text(encoding='utf-8'))
            c.execute((ROOT/'sql/migrations/013_booking_reception.sql').read_text(encoding='utf-8'))
            c.execute("INSERT INTO departments(department_code,department_name) VALUES ('TEST','Test')")
            c.execute("INSERT INTO doctors(doctor_code,department_id) VALUES ('TEST',1)")
            c.execute("INSERT INTO patients(patient_code,date_of_birth,gender) VALUES ('EXISTING','1990-01-01','UNKNOWN')")
        cls.patcher = patch('booking.reception.connect', side_effect=lambda *_: cls.connection())
        cls.patcher.start()

    @classmethod
    def connection(cls):
        return psycopg.connect(**cls.options, dbname=cls.database)

    @classmethod
    def tearDownClass(cls):
        cls.patcher.stop()
        with psycopg.connect(**cls.options, dbname='hospital_oltp', autocommit=True) as c:
            c.execute(sql.SQL('DROP DATABASE {}').format(sql.Identifier(cls.database)))

    def request(self, **changes):
        data = dict(request_id=str(uuid.uuid4()),patient_type='FIRST',patient_name='Nguyễn Minh An',
                    patient_code='',purpose='GENERAL',department_id=1,doctor_id=1,
                    scheduled_at=(datetime.now(timezone.utc)+timedelta(hours=1)).isoformat())
        return data | changes

    def make_due(self, identifier):
        # Fixture-only time setup, never changes the actual hospital database.
        with self.connection() as c:
            now=c.execute('SELECT clock_timestamp()').fetchone()[0]
            self.set_slot(c,identifier,now)

    def set_slot(self,c,identifier,when):
        c.execute("UPDATE booking_registrations SET request=jsonb_set(request,'{scheduled_at}',%s) WHERE request_id=%s",(Jsonb(when.isoformat()),identifier))
        c.execute('UPDATE appointments SET scheduled_at=%s WHERE appointment_id=(SELECT appointment_id FROM booking_registrations WHERE request_id=%s)',(when,identifier))

    def test_first_visit_code_only_at_checkin_and_events_are_valid(self):
        data=self.request()
        result=reception.submit(data)
        self.assertIsNone(result['patient_id'])
        self.assertTrue(result['appointment_id'])
        self.assertEqual(result['status'],'SCHEDULED')
        self.assertEqual(reception.submit(data),result)
        self.make_due(result['request_id'])
        checked=reception.checkin(result['request_id'])
        self.assertTrue(checked['patient_code'].startswith('BN-'))
        self.assertTrue(checked['encounter_id'])
        self.assertEqual(reception.checkin(result['request_id']),checked)
        with self.connection() as c:
            events=c.execute("SELECT event_body FROM booking_event_outbox WHERE event_body->'payload'->>'patient_id'=%s",(str(checked['patient_id']),)).fetchall()
            self.assertEqual(len(events),3)
            for (value,) in events:
                from streaming.common import TOPICS
                validate(value,TOPICS[value['event_type']],str(value['entity_id']),c)

    def test_no_show_retains_appointment_without_encounter_and_emits_once(self):
        data=reception.submit(self.request())
        with self.connection() as c:
            now=c.execute('SELECT clock_timestamp()').fetchone()[0]
            self.set_slot(c,data['request_id'],now-timedelta(minutes=31))
        reception.expire_bookings()
        result=reception.status(data['request_id'])
        self.assertEqual(result['status'],'NO_SHOW')
        self.assertEqual(result['appointment_id'],data['appointment_id'])
        self.assertIsNone(result['encounter_id'])
        with self.assertRaises(BookingError):
            reception.checkin(data['request_id'])
        reception.expire_bookings()
        with self.connection() as c:
            events=c.execute("SELECT event_body FROM booking_event_outbox WHERE event_body->>'event_type'='status_changed' AND event_body->'payload'->>'appointment_id'=%s",(str(result['appointment_id']),)).fetchall()
            self.assertEqual(len(events),1)
            from streaming.common import TOPICS
            validate(events[0][0],TOPICS['status_changed'],str(result['appointment_id']),c)

    def test_expiration_preserves_checked_in_cancelled_and_open_appointments(self):
        attended=reception.submit(self.request())
        self.make_due(attended['request_id'])
        reception.checkin(attended['request_id'])
        cancelled=reception.submit(self.request())
        open_booking=reception.submit(self.request())
        with self.connection() as c:
            now=c.execute('SELECT clock_timestamp()').fetchone()[0]
            self.set_slot(c,attended['request_id'],now-timedelta(hours=1))
            self.set_slot(c,cancelled['request_id'],now-timedelta(hours=1))
            self.set_slot(c,open_booking['request_id'],now-timedelta(minutes=29))
            c.execute("UPDATE appointments SET status='CANCELLED' WHERE appointment_id=%s",(cancelled['appointment_id'],))
        reception.expire_bookings()
        self.assertEqual(reception.status(attended['request_id'])['status'],'CHECKED_IN')
        self.assertEqual(reception.status(cancelled['request_id'])['status'],'CANCELLED')
        self.assertEqual(reception.status(open_booking['request_id'])['status'],'SCHEDULED')

    def test_legacy_pending_registration_gets_appointment(self):
        command,request=reception.normalized(self.request())
        with self.connection() as c:
            c.execute('INSERT INTO booking_registrations(request_id,request) VALUES (%s,%s)',(command,Jsonb(request)))
        reception.expire_bookings()
        self.assertTrue(reception.status(command)['appointment_id'])

    def test_checkin_and_expiration_do_not_overwrite_each_other(self):
        from concurrent.futures import ThreadPoolExecutor
        data=reception.submit(self.request())
        self.make_due(data['request_id'])
        with ThreadPoolExecutor(max_workers=2) as pool:
            checking=pool.submit(reception.checkin,data['request_id'])
            expiring=pool.submit(reception.expire_bookings)
            self.assertEqual(checking.result()['status'],'CHECKED_IN')
            expiring.result()
        self.assertEqual(reception.status(data['request_id'])['status'],'CHECKED_IN')
        with self.connection() as c:
            self.assertEqual(c.execute('SELECT count(*) FROM encounters WHERE appointment_id=%s',(data['appointment_id'],)).fetchone()[0],1)

    def test_noshow_counts_in_production_fact_projection_and_mart(self):
        registration=reception.submit(self.request())
        with self.connection() as c:
            now=c.execute('SELECT clock_timestamp()').fetchone()[0]
            self.set_slot(c,registration['request_id'],now-timedelta(minutes=31))
        reception.expire_bookings()
        with self.connection() as c:
            c.execute((ROOT/'sql/design/warehouse_schema_draft.sql').read_text(encoding='utf-8'))
            c.execute((ROOT/'sql/migrations/014_noshow_30_minutes.sql').read_text(encoding='utf-8'))
            c.execute('CREATE SCHEMA IF NOT EXISTS stg')
            c.execute("CREATE TABLE stg.appointments AS SELECT * FROM appointments WHERE appointment_id=%s",(registration['appointment_id'],))
            c.execute('INSERT INTO dw.dim_patient(patient_id,patient_code) SELECT patient_id,patient_code FROM patients')
            c.execute('INSERT INTO dw.dim_department(department_id,department_name) SELECT department_id,department_name FROM departments')
            c.execute("CREATE FUNCTION dw.local_date_key(t timestamptz) RETURNS integer LANGUAGE sql IMMUTABLE AS $$SELECT to_char(t AT TIME ZONE 'Asia/Ho_Chi_Minh','YYYYMMDD')::integer$$")
            c.execute("INSERT INTO dw.dim_date(date_key,calendar_date) SELECT dw.local_date_key(scheduled_at),(scheduled_at AT TIME ZONE 'Asia/Ho_Chi_Minh')::date FROM stg.appointments")
            c.execute('CREATE TEMP TABLE run_context AS SELECT clock_timestamp() as_of,%s::uuid run_id',(str(uuid.uuid4()),))
            transformation=(ROOT/'sql/warehouse/transformations/batch.sql').read_text(encoding='utf-8')
            projection=transformation.split('CREATE TEMP TABLE next_appointment ON COMMIT DROP AS')[1].split("SELECT dw.merge_current('dw.fact_appointment'")[0]
            c.execute('CREATE TEMP TABLE next_appointment ON COMMIT DROP AS'+projection)
            columns=[r[0] for r in c.execute("SELECT attname FROM pg_attribute WHERE attrelid='next_appointment'::regclass AND attnum>0 ORDER BY attnum")]
            names=sql.SQL(',').join(map(sql.Identifier,columns))
            c.execute(sql.SQL('INSERT INTO dw.fact_appointment ({}) SELECT {} FROM next_appointment').format(names,names))
            row=c.execute('SELECT sum(no_show_count),sum(classified_count) FROM mart.appointment_daily').fetchone()
            self.assertEqual(row,(1,1))

    def test_returning_visit_and_checkin(self):
        result=reception.submit(self.request(patient_type='RETURNING',patient_code='EXISTING',purpose='FOLLOW_UP'))
        self.assertEqual(result['patient_id'],1)
        self.assertTrue(result['appointment_id'])
        self.make_due(result['request_id'])
        self.assertEqual(reception.checkin(result['request_id'])['patient_code'],'EXISTING')

    def test_less_than_30_minutes_is_rejected(self):
        with self.assertRaises(BookingError):
            reception.submit(self.request(scheduled_at=(datetime.now(timezone.utc)+timedelta(minutes=29)).isoformat()))

    def test_exact_cutoff(self):
        now=datetime.now(timezone.utc)
        reception.validate_slot(now,now+timedelta(minutes=30))
        with self.assertRaises(BookingError):
            reception.validate_slot(now,now+timedelta(minutes=30)-timedelta(microseconds=1))

    def test_checkin_window_boundaries(self):
        scheduled=datetime(2026,11,5,3,0,tzinfo=timezone.utc)
        for minutes,accepted in [(-31,False),(-30,True),(-15,True),(0,True),(15,True),(30,True),(31,False)]:
            with self.subTest(minutes_from_appointment=minutes):
                now=scheduled+timedelta(minutes=minutes)
                if accepted:
                    reception.validate_checkin(now,scheduled)
                else:
                    with self.assertRaises(BookingError):
                        reception.validate_checkin(now,scheduled)
        for offset in (-1,1):
            edge=scheduled+timedelta(minutes=30*offset,microseconds=offset)
            with self.assertRaises(BookingError):
                reception.validate_checkin(edge,scheduled)

    def test_unknown_returning_patient(self):
        with self.assertRaises(BookingError):
            reception.submit(self.request(patient_type='RETURNING',patient_code='MISSING',purpose='FOLLOW_UP'))

    def test_conflict(self):
        data=self.request()
        reception.submit(data)
        with self.assertRaises(BookingError) as context:
            reception.submit(data|dict(patient_name='Trần Thanh Bình'))
        self.assertEqual(context.exception.status,409)

    def test_first_cannot_claim_followup(self):
        with self.assertRaises(BookingError):
            reception.submit(self.request(purpose='FOLLOW_UP'))

    def test_early_checkin_rejected(self):
        data=reception.submit(self.request())
        with self.assertRaises(BookingError):
            reception.checkin(data['request_id'])

    def test_concurrent_retries(self):
        from concurrent.futures import ThreadPoolExecutor
        data=self.request()
        with ThreadPoolExecutor(max_workers=4) as pool:
            results=list(pool.map(lambda _: reception.submit(data),range(4)))
        self.assertTrue(all(item==results[0] for item in results))
        with self.connection() as c:
            self.assertEqual(c.execute('SELECT count(*) FROM booking_registrations WHERE request_id=%s',(data['request_id'],)).fetchone()[0],1)


if __name__=='__main__':
    unittest.main()
