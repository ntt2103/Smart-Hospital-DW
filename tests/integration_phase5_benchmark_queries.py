"""Execute and decode every benchmark variant on a nonempty isolated fixture.

This is a SQL/client compatibility test, not a performance measurement. In
particular its RT corpus can be empty; the benchmark runner rejects empty RT.
"""
import json

from operations.benchmark import digest, workload
from operations.foundation import ROOT
from tests.integration_phase5_etl_permissions import connect_fixture, TARGET_DB


def run():
    manifest = json.loads((ROOT/'artifacts/generated/phase5_source_copy_smoke/manifest.json').read_text())
    checks = []
    with connect_fixture('dwh','etl') as connection:
        connection.execute('SET TRANSACTION READ ONLY')
        assert connection.execute('SELECT count(*) FROM dw.fact_encounter').fetchone()[0] > 0
        as_of = connection.execute('SELECT clock_timestamp()').fetchone()[0].isoformat()
        queries = workload(manifest,connection,as_of)
        for query in queries:
            try:
                with connection.transaction():
                    rows = connection.execute(query['sql'],query['params']).fetchall()
                checks.append({'query':query['id'],'passed':True,'rows':len(rows),'result_sha256':digest(rows)})
            except Exception as exc:
                checks.append({'query':query['id'],'passed':False,'error_type':type(exc).__name__})
    result = {'scope':'actual SQL execution and psycopg fetch/decode; not a benchmark result',
              'warehouse_database':TARGET_DB,'main_databases_modified':False,'variant_count':len(queries),
              'query_sha256':digest(queries),'checks':checks,'passed':all(check['passed'] for check in checks)}
    output = ROOT/'artifacts/phase5/benchmark_query_fetch.json'
    output.write_text(json.dumps(result,indent=2),encoding='utf-8')
    assert result['passed'],[check for check in checks if not check['passed']]
    print(json.dumps({'benchmark_query_fetch_passed':len(checks),'warehouse_database':TARGET_DB}))


if __name__ == '__main__':
    run()
