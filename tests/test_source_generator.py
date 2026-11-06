import copy
import unittest
from data_sources.generator.historical import generate
from data_quality.source_checks import validate


class SourceGeneratorTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.snapshot = generate()

    def test_clean_snapshot_and_scale(self):
        self.assertEqual(validate(self.snapshot), [])
        counts = self.snapshot['manifest']['counts']
        self.assertEqual(counts['appointments'], 500)
        self.assertEqual(counts['encounters'], 540)

    def test_deterministic_and_seed_sensitive(self):
        self.assertEqual(self.snapshot['manifest']['business_sha256'], generate()['manifest']['business_sha256'])
        self.assertNotEqual(self.snapshot['manifest']['business_sha256'], generate(seed=2)['manifest']['business_sha256'])

    def test_reject_cross_patient_transfer(self):
        s = copy.deepcopy(self.snapshot)
        e = next(r for r in s['tables']['encounters'] if r['parent_encounter_id'])
        e['patient_id'] = e['patient_id'] % 100+1
        self.assertIn('OP_TO_IP', {r['rule'] for r in validate(s)})

    def test_reject_bed_and_encounter_overlap(self):
        s = copy.deepcopy(self.snapshot)
        a = dict(s['tables']['bed_assignments'][0], bed_assignment_id=999999)
        s['tables']['bed_assignments'].append(a)
        rules = {r['rule'] for r in validate(s)}
        self.assertTrue({'OVERLAP_bed_id', 'OVERLAP_encounter_id'} <= rules)

    def test_reject_assignment_during_maintenance(self):
        s = copy.deepcopy(self.snapshot)
        a = s['tables']['bed_assignments'][0]
        h = next(h for h in s['tables']['bed_status_history'] if h['bed_id'] == a['bed_id'])
        h['status'] = 'MAINTENANCE'
        self.assertIn('ASSIGNMENT_ACTIVE_COVERAGE', {r['rule'] for r in validate(s)})

    def test_reject_orphan_and_negative_tat(self):
        s = copy.deepcopy(self.snapshot)
        s['tables']['lab_orders'][0]['encounter_id'] = 999999
        self.assertIn('FK', {r['rule'] for r in validate(s)})
        s = copy.deepcopy(self.snapshot)
        l = next(l for l in s['tables']['lab_orders'] if l['result_completed_at'])
        l['result_completed_at'] = '1900-01-01T00:00:00+00:00'
        self.assertIn('LAB_TIMELINE', {r['rule'] for r in validate(s)})


if __name__ == '__main__':
    unittest.main()
