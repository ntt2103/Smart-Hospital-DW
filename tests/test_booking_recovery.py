"""Pure unit coverage for booking retry isolation and request normalization.

Run in the Linux foundation image, which supplies the producer's fcntl dependency.
No PostgreSQL, SQLite journal, Kafka connection, or source mutation is performed.
"""
import unittest
import io
import sys
import os
from unittest.mock import Mock, MagicMock, call, patch

if sys.platform == 'win32':
    raise unittest.SkipTest('Booking recovery uses the Linux producer; run through the foundation container.')

from booking.service import BookingError, Handler, normalize, recover_bookings, status


class BookingRecoveryTests(unittest.TestCase):
    def journal(self, commands):
        journal = Mock()
        journal.db.execute.return_value.fetchall.return_value = [(item,) for item in commands]
        return journal

    def test_conflicted_command_does_not_block_later_apply_or_publication(self):
        journal = self.journal(['conflicted-request', 'healthy-request'])
        order = []

        def apply(command):
            order.append(command)
            if command == 'conflicted-request':
                raise ValueError('Canonical source conflicts with the retained intent')

        journal.apply.side_effect = apply
        journal.recover.side_effect = lambda **kwargs: order.append('publish')
        with patch('builtins.print'), patch('booking.service.connect', side_effect=AssertionError('Unexpected database connection')):
            recover_bookings(journal)

        self.assertEqual(journal.apply.call_args_list, [call('conflicted-request'), call('healthy-request')])
        self.assertEqual(order, ['conflicted-request', 'healthy-request', 'publish'])
        journal.recover.assert_called_once_with(ack_timeout=3, prepare_pending=False)

    def test_all_conflicted_commands_still_allow_previously_applied_messages_to_publish(self):
        journal = self.journal(['conflicted-one', 'conflicted-two'])
        journal.apply.side_effect = ValueError('Requires manual reconciliation')

        with patch('builtins.print'), patch('booking.service.connect', side_effect=AssertionError('Unexpected database connection')):
            recover_bookings(journal)

        self.assertEqual(journal.apply.call_count, 2)
        journal.recover.assert_called_once_with(ack_timeout=3, prepare_pending=False)

    def test_messages_can_publish_when_no_commands_need_source_replay(self):
        journal = self.journal([])
        with patch('booking.service.connect', side_effect=AssertionError('Unexpected database connection')):
            recover_bookings(journal)

        journal.apply.assert_not_called()
        journal.recover.assert_called_once_with(ack_timeout=3, prepare_pending=False)


class BookingNormalizationTests(unittest.TestCase):
    def request(self, **changes):
        data = {'request_id': '97d97ab9-4565-44a7-8d1b-226ba33cf17b',
                'patient_code': ' SYN-000001 ', 'department_id': 1, 'doctor_id': 2,
                'scheduled_at': '2026-11-01T10:30:00+07:00'}
        data.update(changes)
        return data

    def test_equivalent_offsets_produce_same_idempotency_payload(self):
        local = normalize(self.request())
        utc = normalize(self.request(scheduled_at='2026-11-01T03:30:00Z'))

        self.assertEqual(local, utc)
        self.assertEqual(local[1]['scheduled_at'], '2026-11-01T03:30:00Z')
        self.assertEqual(local[1]['patient_code'], 'SYN-000001')

    def test_boolean_ids_are_rejected_rather_than_treated_as_integers(self):
        for field in ('department_id', 'doctor_id'):
            for value in (True, False):
                with self.subTest(field=field, value=value):
                    with self.assertRaises(BookingError) as error:
                        normalize(self.request(**{field: value}))
                    self.assertEqual(error.exception.status, 422)

    def test_timestamp_without_timezone_is_rejected(self):
        with self.assertRaises(BookingError) as error:
            normalize(self.request(scheduled_at='2026-11-01T10:30:00'))
        self.assertEqual(error.exception.status, 422)


class BookingTraceTests(unittest.TestCase):
    def test_created_event_rt_is_not_applicable_without_any_fact_event_probe(self):
        journal=MagicMock()
        intent={'rows':{'appointments':[{'appointment_id':42,'scheduled_at':'2026-11-03T03:00:00Z'}]},
                'events':[{'event_id':'97d97ab9-4565-44a7-8d1b-226ba33cf17b'}]}
        import json
        journal.__enter__.return_value.db.execute.return_value.fetchone.return_value=('PUBLISHED',json.dumps(intent))
        source,raw,warehouse=[MagicMock() for _ in range(3)]
        for connection,value in [(source,('SCHEDULED',)),(raw,(True,)),(warehouse,(True,))]:
            connection.__enter__.return_value.execute.return_value.fetchone.return_value=value
        with patch('booking.service.journal',return_value=journal),patch('booking.service.connect',side_effect=[source,raw,warehouse]) as connect:
            result=status('97d97ab9-4565-44a7-8d1b-226ba33cf17b')
        self.assertFalse(result['rt_applicable'])
        self.assertEqual(result['rt_processing_status'],'NOT_APPLICABLE')
        self.assertNotIn('stream_fact_received',result)
        self.assertTrue(result['warehouse_received'])
        self.assertEqual(connect.call_args_list,[call('oltp','reader'),call('dwh','consumer'),call('dwh','etl')])


class BookingHttpFailureTests(unittest.TestCase):
    def test_registration_validation_returns_422(self):
        handler = self.handler(b'{}')
        handler.path = '/api/registrations'
        from booking.errors import BookingError as SharedError
        with patch('booking.reception.submit', side_effect=SharedError('Phải đặt trước giờ hẹn ít nhất 30 phút.')):
            handler.do_POST()
        self.assertEqual(handler.send.call_args.args[0], 422)
        self.assertIn('30 phút', handler.send.call_args.args[1]['error'])

    def handler(self, body):
        handler = object.__new__(Handler)
        handler.path = '/api/bookings'
        handler.headers = {'Host': f"localhost:{os.environ.get('BOOKING_HOST_PORT','18081')}", 'Content-Type': 'application/json',
                           'Content-Length': str(len(body))}
        handler.rfile = io.BytesIO(body)
        handler.send = Mock()
        return handler

    def test_source_recovery_conflict_is_ambiguous_service_failure(self):
        handler = self.handler(b'{}')
        with patch('booking.service.submit', side_effect=ValueError('source conflict')):
            handler.do_POST()
        self.assertEqual(handler.send.call_args.args[0], 503)

    def test_invalid_json_is_rejected_before_submit(self):
        handler = self.handler(b'{not-json}')
        with patch('booking.service.submit') as submit:
            handler.do_POST()
        submit.assert_not_called()
        self.assertEqual(handler.send.call_args.args[0], 400)


if __name__ == '__main__':
    unittest.main()
