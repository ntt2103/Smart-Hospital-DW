import unittest
from analytics.report_dates import report_dates


class ReportDateTests(unittest.TestCase):
    def test_exclusive_asof_uses_last_local_day_in_horizon(self):
        self.assertEqual(report_dates({'history_start':'2026-05-05T17:00:00Z',
                                       'as_of':'2026-11-01T17:00:00Z'}),
                         ('2026-05-06','2026-11-01'))

    def test_explicit_manifest_bounds_are_used(self):
        self.assertEqual(report_dates({'report_start_date':'2026-05-06',
                                       'report_end_date':'2026-11-01'}),
                         ('2026-05-06','2026-11-01'))

    def test_invalid_bounds_fail_instead_of_silently_using_old_demo_dates(self):
        with self.assertRaises(ValueError):
            report_dates({'report_start_date':'2026-11-02','report_end_date':'2026-11-01'})


if __name__=='__main__':unittest.main()
