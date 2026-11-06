"""Credential and database recovery files must stay out of code deliveries."""
import unittest

from operations.package_delivery import ROOT, allowed


class PackageDeliveryTests(unittest.TestCase):
    def test_credentials_and_recovery_archives_are_excluded(self):
        for name in (
            '.env', '.env.rotation.part', 'credentials/old.env', 'credentials/NEW.ENV',
            'artifacts/generated/phase5_secret_rotation/new.env',
            'artifacts/phase5/backups/roles.sql', 'artifacts/phase5/backups/checks.json',
            'artifacts/phase5/oltp.tar.gz', 'artifacts/phase5/dwh.sql.gz',
            'artifacts/phase5/dwh.dump', 'artifacts/phase5/metadata.backup',
        ):
            with self.subTest(name=name):
                self.assertFalse(allowed(ROOT/name))

    def test_templates_source_and_safe_evidence_are_included(self):
        for name in (
            '.env.example', 'sql/migrations/005_source_relations.sql',
            'artifacts/phase5/dataset_manifest.json',
            'artifacts/phase5/benchmark/plans/before/Q01_visits_180d.json',
        ):
            with self.subTest(name=name):
                self.assertTrue(allowed(ROOT/name))


if __name__ == '__main__':
    unittest.main()
