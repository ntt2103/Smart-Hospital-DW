"""Clock-pending correctness without changing source events or using a database."""
import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch

from tests.integration_streaming import snapshot_after_actual_clock


class ActualClockEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.at = datetime(2026, 11, 2, tzinfo=timezone.utc)

    def snapshot(self, at):
        return {'run_id': str(at), 'as_of': at.isoformat(), 'canonical_signal_receipts': 2, 'department_rows': 11}

    def test_pending_zero_is_preserved_before_eligible_actual_refresh(self):
        connection = MagicMock()
        connection.__enter__.return_value.execute.return_value.fetchone.side_effect = [
            (self.at, self.at+timedelta(seconds=1), self.at+timedelta(seconds=2)),
            (Decimal(0), Decimal(0)),
            (self.at, self.at+timedelta(seconds=1), self.at+timedelta(seconds=2)),
            (Decimal(2), Decimal(2)),
        ]
        with tempfile.TemporaryDirectory() as directory, \
             patch('tests.integration_streaming.connect', return_value=connection), \
             patch('tests.integration_streaming.refresh', side_effect=[self.snapshot(self.at), self.snapshot(self.at+timedelta(seconds=3))]) as refresh, \
             patch('tests.integration_streaming.time.sleep'):
            result = snapshot_after_actual_clock(Path(directory), timeout_seconds=1)
            evidence = json.loads((Path(directory)/'rt_clock_attempts.json').read_text())
            self.assertEqual(refresh.call_count, 2)
            self.assertEqual(result['as_of'], (self.at+timedelta(seconds=3)).isoformat())
            self.assertEqual([a['status'] for a in evidence['attempts']], ['inputs_pending_real_clock', 'eligible'])
            self.assertEqual(evidence['attempts'][0]['checkins'], 0)
            self.assertFalse(evidence['timestamps_modified'])

    def test_expired_corpus_fails_without_selecting_an_older_window(self):
        connection = MagicMock()
        connection.__enter__.return_value.execute.return_value.fetchone.side_effect = [(self.at, self.at, self.at), (0, 0)]
        with tempfile.TemporaryDirectory() as directory, \
             patch('tests.integration_streaming.connect', return_value=connection), \
             patch('tests.integration_streaming.refresh', return_value=self.snapshot(self.at+timedelta(minutes=16))) as refresh, \
             patch('tests.integration_streaming.time.sleep') as sleep:
            with self.assertRaisesRegex(AssertionError, 'expired'):
                snapshot_after_actual_clock(Path(directory), timeout_seconds=1)
            self.assertEqual(refresh.call_count, 1)
            sleep.assert_not_called()
            evidence = json.loads((Path(directory)/'rt_clock_attempts.json').read_text())
            self.assertEqual(evidence['attempts'][0]['status'], 'expired')

    def test_unrelated_refresh_error_is_recorded_and_not_retried(self):
        with tempfile.TemporaryDirectory() as directory, \
             patch('tests.integration_streaming.refresh', side_effect=ValueError('unrelated')) as refresh, \
             patch('tests.integration_streaming.time.sleep') as sleep:
            with self.assertRaisesRegex(ValueError, 'unrelated'):
                snapshot_after_actual_clock(Path(directory), timeout_seconds=1)
            self.assertEqual(refresh.call_count, 1)
            sleep.assert_not_called()
            evidence = json.loads((Path(directory)/'rt_clock_attempts.json').read_text())
            self.assertFalse(evidence['attempts'][0]['clock_check_failure'])


if __name__ == '__main__':
    unittest.main()
