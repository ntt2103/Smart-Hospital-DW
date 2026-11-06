"""One reversible index-control experiment, only in the isolated Phase 5 stack."""
import argparse
import json
import os
from operations.foundation import ROOT,connect,migration
from operations.phase5_optimize import INDEXES


def main(action):
    if os.environ.get('COMPOSE_PROJECT_NAME')!='smart_hospital_dw_phase5':
        raise ValueError('Only the isolated Phase 5 stack is supported')
    path=ROOT/'artifacts/phase5/benchmark/comparison.json'
    if not json.loads(path.read_text())['same_workload']:
        raise ValueError('Complete the first comparable pair before the control experiment')
    with connect('dwh','admin') as connection:
        count=connection.execute("SELECT count(*) FROM pg_indexes WHERE schemaname='dw' AND indexname=ANY(%s)",(INDEXES,)).fetchone()[0]
        if action=='remove':
            if count!=3: raise ValueError('Expected all three optional lookup indexes')
            migration(connection,'011_phase5_lookup_control_remove',ROOT/'sql/migrations/011_phase5_lookup_control_remove.sql')
            expected=0
        else:
            if count not in (0,3): raise ValueError('Unexpected partial lookup index state')
            migration(connection,'012_phase5_lookup_control_restore',ROOT/'sql/migrations/012_phase5_lookup_control_restore.sql')
            expected=3
        actual=connection.execute("SELECT count(*) FROM pg_indexes WHERE schemaname='dw' AND indexname=ANY(%s)",(INDEXES,)).fetchone()[0]
        if actual!=expected: raise ValueError('Index-control migration did not reach the expected state')
    print(json.dumps({'action':action,'lookup_indexes':actual,'business_rows_modified':False}))


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action',choices=['remove','restore'])
    main(parser.parse_args().action)
