import contextlib
import csv
import io
import tempfile
import unittest
from pathlib import Path

from data_sources.generator.phase5 import Config, Workload, write_dataset
from data_quality.source_checks import validate


def read_fixture(directory, manifest):
    tables={}
    for table,definition in manifest['tables'].items():
        rows=[]
        for chunk in definition['chunks']:
            with (directory/chunk['path']).open(newline='',encoding='utf-8') as source:
                for values in csv.reader(source):
                    row={}
                    for key,value in zip(definition['columns'],values):
                        row[key]=None if value=='\\N' else value=='True' if value in ('True','False') else int(value) if key.endswith('_id') or key in ('target_tat_minutes_at_order','default_tat_minutes') else value
                    rows.append(row)
        tables[table]=rows
    return {'manifest':manifest,'tables':tables}


class Phase5GeneratorTests(unittest.TestCase):
    def test_count_is_driven_by_daily_visits(self):
        counts=Config().counts()
        self.assertEqual(counts['encounters'],1080000)
        self.assertEqual(counts['inpatient_encounters']+counts['outpatient_encounters'],1080000)
        self.assertEqual(counts['lab_orders'],3024000)

    def test_clean_streamed_fixture_and_chunk_independence(self):
        config=Config(encounters_per_day=30,horizon_days=30)
        with tempfile.TemporaryDirectory() as a,tempfile.TemporaryDirectory() as b,contextlib.redirect_stdout(io.StringIO()):
            first=write_dataset(a,config,37)
            second=write_dataset(b,config,113)
            self.assertEqual(first['business_sha256'],second['business_sha256'])
            self.assertEqual(first['counts']['encounters'],900)
            self.assertEqual(validate(read_fixture(Path(a),first)),[])
            self.assertEqual(len(first['tables']),13)
            self.assertNotEqual(first['core_source_rows'],5000000)

    def test_capacity_failure_never_trims(self):
        config=Config(encounters_per_day=40,horizon_days=30,target_bed_utilization=1)
        with self.assertRaises(ValueError): config.validate()

    def test_maintenance_does_not_close_an_entire_department(self):
        with tempfile.TemporaryDirectory() as directory:
            workload=Workload(Config(),Path(directory)/'contexts.sqlite3')
            try:
                for dept in range(1,11):
                    self.assertTrue(any(d==dept and bed not in workload.maintenance_beds for bed,d in workload.bed_depts.items()))
            finally:
                workload.db.close()


if __name__=='__main__': unittest.main()
