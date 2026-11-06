"""Apply optional lookup indexes only after a complete comparable baseline."""
import json
import os
import time
from operations.foundation import ROOT, connect, migration
from operations.benchmark import digest, environment

INDEXES=['phase5_doctor_participation_lookup','phase5_diagnosis_lookup','phase5_patient_history_lookup']


def main():
    if os.environ.get('COMPOSE_PROJECT_NAME')!='smart_hospital_dw_phase5':
        raise ValueError('Use only the isolated Phase 5 environment')
    folder=ROOT/'artifacts/phase5'
    before=json.loads((folder/'benchmark/benchmark_before.json').read_text(encoding='utf-8'))
    if len(before['summary'])!=32 or not all(s['complete'] for s in before['summary'].values()):
        raise ValueError('A complete 32-variant baseline is required')
    with connect('dwh','etl') as connection:
        current=environment(connection)
    for key in ('settings','business_row_counts','event_corpus_sha256'):
        if digest(current[key])!=digest(before['environment'][key]):
            raise ValueError('Frozen workload changed: '+key)
    hypothesis={
        'group':'Three selective lookup indexes',
        'evidence':{
            'Q10':'Sequential bridge scan read40330pages to find2103 participations',
            'Q11_Q12':'Diagnosis bridge sequential scan filters roughly1.5M rows for10800 matches',
            'Q15':'Fact sequential scan read49902pages to return8 patient encounters'},
        'expected_effect':'Fewer scanned/read pages and lower median/p95 for Q10/Q11/Q12/Q15',
        'tradeoff':'Additional index bytes and maintenance on encounter/association writes',
        'unchanged':'Dataset, SQL text/parameters, eligibility, as_of, PG settings and cache protocol',
        'baseline_median_ms':{key:before['summary'][key]['median_ms'] for key in (
            'Q10_participation_180d','Q11_diagnoses_180d','Q12_two_bridges_180d','Q15_patient_history_180d')},
        'schema_baseline_note':'The staging permission and shared-memory prerequisite repairs occurred before both measurements'}
    (folder/'optimization_hypothesis.json').write_text(json.dumps(hypothesis,indent=2),encoding='utf-8')
    begun=time.monotonic()
    with connect('dwh','admin') as connection:
        already=connection.execute("SELECT EXISTS(SELECT 1 FROM _deploy.migrations WHERE name='010_phase5_lookup_indexes')").fetchone()[0]
        migration(connection,'010_phase5_lookup_indexes',ROOT/'sql/migrations/010_phase5_lookup_indexes.sql')
        sizes=connection.execute("SELECT c.relname,pg_relation_size(c.oid) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='dw' AND c.relname=ANY(%s) ORDER BY c.relname",(INDEXES,)).fetchall()
    report={'migration':'010_phase5_lookup_indexes','already_applied':already,
            'elapsed_seconds':time.monotonic()-begun,'indexes_bytes':dict(sizes),'total_index_bytes':sum(row[1] for row in sizes),
            'baseline_query_sha256':before['query_sha256'],'correctness_acceptance':'Pending exact result digests in paired after benchmark'}
    (folder/'optimization_applied.json').write_text(json.dumps(report,indent=2),encoding='utf-8')
    print(json.dumps(report))


if __name__=='__main__': main()
