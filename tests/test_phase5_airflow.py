import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from operations import phase5_airflow


class FakeAirflow:
    def __init__(self, paused=True, fail_run=False):
        self.paused = paused
        self.fail_run = fail_run
        self.patches = []

    def request(self, method, path, body=None):
        if method == 'PATCH':
            self.paused = body['is_paused']
            self.patches.append(self.paused)
            return {'is_paused': self.paused}
        if method == 'POST':
            return {'state': 'queued'}
        if '/dagRuns/' in path:
            if self.fail_run:
                raise RuntimeError('Airflow GET HTTP500')
            if path.endswith('/taskInstances'):
                return {'task_instances': [{'state': 'success'}]}
            return {'state': 'success'}
        return {'is_paused': self.paused}


class Phase5AirflowTests(unittest.TestCase):
    def run_fixture(self, fake, directory):
        with patch.dict(os.environ, {'COMPOSE_PROJECT_NAME': 'smart_hospital_dw_phase5'}), \
             patch.object(phase5_airflow, 'ROOT', Path(directory)), \
             patch.object(phase5_airflow, 'source_counts', return_value={'encounters': 1080004}), \
             patch.object(phase5_airflow, 'Airflow', return_value=fake):
            return phase5_airflow.run('hospital_batch', 5)

    def test_preserves_a_previously_paused_dag(self):
        with tempfile.TemporaryDirectory() as directory:
            fake = FakeAirflow(paused=True)
            result = self.run_fixture(fake, directory)
            self.assertEqual(fake.patches, [False, True])
            self.assertTrue(result['restored_paused'])
            self.assertEqual(result['state'], 'success')

    def test_preserves_a_previously_unpaused_dag(self):
        with tempfile.TemporaryDirectory() as directory:
            fake = FakeAirflow(paused=False)
            self.run_fixture(fake, directory)
            self.assertEqual(fake.patches, [False, False])
            self.assertFalse(fake.paused)

    def test_api_failure_keeps_evidence_and_restores_pause(self):
        with tempfile.TemporaryDirectory() as directory:
            fake = FakeAirflow(paused=True, fail_run=True)
            with self.assertRaisesRegex(RuntimeError, 'HTTP500'):
                self.run_fixture(fake, directory)
            result = json.loads((Path(directory)/'artifacts/phase5/hospital_batch_airflow.json').read_text())
            self.assertTrue(result['restored_paused'])
            self.assertEqual(result['error_type'], 'RuntimeError')
            self.assertNotEqual(result['state'], 'success')

    def test_refuses_the_original_demo_environment(self):
        with patch.dict(os.environ, {'COMPOSE_PROJECT_NAME': 'smart_hospital_dw'}), \
             patch.object(phase5_airflow, 'Airflow') as client:
            with self.assertRaisesRegex(ValueError, 'isolated'):
                phase5_airflow.run('hospital_batch', 5)
            client.assert_not_called()


if __name__ == '__main__':
    unittest.main()
