"""Reproducible Phase 5 read workload. Run inside the isolated tool service."""
import argparse
import csv
import hashlib
import json
import math
import random
import statistics
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path
from psycopg import sql

from operations.foundation import ROOT, connect
from analytics.report_dates import report_dates


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, default=str, separators=(',', ':')).encode()).hexdigest()


def workload(manifest, warehouse, rt_as_of):
    start_date,end_date = report_dates(manifest)
    end = datetime.fromisoformat(end_date).date()
    common = {'start': datetime.fromisoformat(start_date).date(), 'end': end,
              'start_key': int(start_date.replace('-','')), 'end_key': int(end.strftime('%Y%m%d')),
              'as_of': rt_as_of, 'doctor': 1, 'diagnosis': 1, 'patient': 1, 'bed': 1}
    # Fix actual representatives once and persist every chosen parameter in results.
    departments = warehouse.execute("SELECT department_sk,count(*) n FROM dw.fact_encounter GROUP BY department_sk ORDER BY n DESC,department_sk").fetchall()
    common['department'] = None
    busy_department = departments[0][0] if departments else 1
    common['quiet_department'] = departments[-1][0] if departments else 1
    queries = []
    for path in sorted((ROOT/'sql/benchmarks').glob('Q*.sql')):
        sql_text = path.read_text(encoding='utf-8')
        periods = (7, 30, 180) if path.stem[:3] in ('Q01','Q02','Q03','Q04','Q05','Q07','Q08') else (180,)
        for days in periods:
            params = dict(common, start=end-timedelta(days=days-1), start_key=int((end-timedelta(days=days-1)).strftime('%Y%m%d')))
            if path.stem[:3] == 'Q08': params['department'] = busy_department
            queries.append({'id': f'{path.stem}_{days}d', 'sql': sql_text, 'params': params})
        if path.stem[:3] == 'Q01':
            for label, department in [('busy', busy_department), ('quiet',common['quiet_department'])]:
                queries.append({'id':f'{path.stem}_{label}', 'sql':sql_text, 'params':dict(common,department=department)})
    return queries


def environment(connection):
    settings = connection.execute("SELECT name,setting,unit FROM pg_settings WHERE name IN ('server_version','shared_buffers','work_mem','maintenance_work_mem','max_parallel_workers_per_gather','max_parallel_workers','max_worker_processes','effective_cache_size','random_page_cost','effective_io_concurrency','enable_seqscan','enable_indexscan','enable_bitmapscan','jit','TimeZone') ORDER BY name").fetchall()
    indexes = connection.execute("SELECT schemaname,tablename,indexname,indexdef FROM pg_indexes WHERE schemaname IN ('dw','mart','raw','stg') ORDER BY 1,2,3").fetchall()
    sizes = connection.execute("SELECT n.nspname,c.relname,pg_table_size(c.oid),pg_indexes_size(c.oid),pg_total_relation_size(c.oid) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname IN ('dw','mart','raw','stg','ops') AND c.relkind IN ('r','m') ORDER BY 1,2").fetchall()
    counts = {}
    for schema, table, *_ in sizes:
        counts[f'{schema}.{table}'] = connection.execute(sql.SQL('SELECT count(*) FROM {}.{}').format(sql.Identifier(schema), sql.Identifier(table))).fetchone()[0]
    business_counts = {key: count for key, count in counts.items()
                       if key.startswith(('dw.', 'stg.')) or key in ('raw.events', 'raw.event_deliveries')}
    # Canonical ingestion timestamps are part of the paired RT workload, not just
    # event IDs or result counts. Hash with a bounded cursor for larger corpora.
    corpus = hashlib.sha256()
    with connection.cursor(name='benchmark_event_corpus') as cursor:
        cursor.itersize = 2000
        cursor.execute('SELECT event_id,body_sha256,topic,event_type,entity_id,event_time,published_at,ingested_at FROM raw.events ORDER BY event_id')
        for row in cursor:
            corpus.update(json.dumps(row, default=str, separators=(',', ':')).encode())
            corpus.update(b'\n')
    return {'settings': settings, 'indexes': indexes, 'relations_bytes': sizes,
            'relation_row_counts': counts, 'business_row_counts': business_counts,
            'event_corpus_sha256': corpus.hexdigest(),
            'database_bytes': connection.execute('SELECT pg_database_size(current_database())').fetchone()[0],
            'clock': connection.execute('SELECT clock_timestamp()').fetchone()[0]}


def measure(query, timeout_ms):
    with connect('dwh','etl') as connection:
        connection.execute('SET TRANSACTION READ ONLY')
        connection.execute("SELECT set_config('statement_timeout',%s,true)",(str(timeout_ms),))
        started = time.perf_counter()
        try:
            rows = connection.execute(query['sql'], query['params']).fetchall()
            elapsed = (time.perf_counter()-started)*1000
            return {'status':'ok', 'elapsed_ms':elapsed, 'rows':len(rows), 'result_sha256':digest(rows)}
        except Exception as exc:
            # Do not select only successful runs when computing statistics.
            return {'status':type(exc).__name__, 'elapsed_ms':(time.perf_counter()-started)*1000}


def run(args):
    manifest = json.loads(args.manifest.read_text(encoding='utf-8-sig'))
    artifact_root = (ROOT/'artifacts/phase5').resolve()
    subdir = Path(args.output_subdir)
    output = (artifact_root/subdir).resolve()
    if subdir.is_absolute() or not output.is_relative_to(artifact_root) or output == artifact_root:
        raise ValueError('output-subdir must name a directory under artifacts/phase5')
    output.mkdir(parents=True,exist_ok=True)
    for kind in ('oltp','dwh'):
        with connect(kind,'admin') as connection:
            connection.autocommit = True
            # The same maintenance on both sides removes aborted/restated row
            # versions and makes visibility maps/statistics comparable.
            connection.execute('VACUUM (ANALYZE, PARALLEL 0)')
    with connect('dwh','etl') as connection:
        if args.phase == 'before':
            snapshot_as_of = connection.execute('SELECT max(as_of) FROM mart.realtime_department_snapshot').fetchone()[0]
            if snapshot_as_of is None:
                raise ValueError('Prepare and refresh a nonempty RT corpus before benchmarking')
            fixed_as_of = snapshot_as_of.isoformat()
        else:
            fixed_as_of = json.loads((output/'benchmark_before.json').read_text())['rt_as_of']
        env = environment(connection)
        queries = workload(manifest,connection,fixed_as_of)
        signals = connection.execute('SELECT sum(a.value+b.value) FROM mart.rt01_checkins_last_15m(%s) a JOIN mart.rt02_lab_results_last_15m(%s) b USING(department_id)',(fixed_as_of,fixed_as_of)).fetchone()[0]
        if not signals:
            raise ValueError('RT workload window is empty; prepare a valid corpus and freeze it')
    if args.query:
        prefixes = tuple(prefix.strip() for prefix in args.query.split(',') if prefix.strip())
        queries = [q for q in queries if prefixes and q['id'].startswith(prefixes)]
    if not queries:
        raise ValueError('No benchmark variants match the query filter')
    report = {'phase':args.phase, 'manifest_sha256':digest(manifest), 'rt_as_of':fixed_as_of,
              'cache_mode':'warm', 'preparation':'VACUUM (ANALYZE, PARALLEL 0) on source and warehouse',
              'mode':args.mode, 'clients':args.clients, 'timeout_ms':args.timeout_ms,
              'warmups':args.warmups,'requested_samples':args.samples,
              'query_sha256':digest(queries), 'environment':env, 'queries':queries, 'runs':[], 'summary':{}}
    def save():
        (output/f'benchmark_{args.phase}.json').write_text(json.dumps(report,indent=2,default=str),encoding='utf-8')
    failed = {q['id']:0 for q in queries}
    for stage, count in [('first',1),('warmup',args.warmups),('measured',args.samples)]:
        for repetition in range(count):
            ordered = list(queries)
            random.Random(20261102+repetition).shuffle(ordered)
            for query in ordered:
                if failed[query['id']]>=3:
                    continue
                with ThreadPoolExecutor(max_workers=args.clients) as pool:
                    results = list(pool.map(lambda _:measure(query,args.timeout_ms),range(args.clients)))
                for client,result in enumerate(results):
                    report['runs'].append({'query':query['id'],'stage':stage,'repetition':repetition,'client':client,**result})
                    failed[query['id']] = failed[query['id']]+1 if result['status']!='ok' else 0
                save()
    for query in queries:
        runs=[r for r in report['runs'] if r['query']==query['id'] and r['stage']=='measured']
        values=sorted(r['elapsed_ms'] for r in runs if r['status']=='ok')
        complete=len(values)==args.samples*args.clients and len({r.get('result_sha256') for r in runs})==1
        report['summary'][query['id']]={'complete':complete,'successful_samples':len(values),'errors':len(runs)-len(values)}
        if complete:
            report['summary'][query['id']].update(median_ms=statistics.median(values),p95_ms=values[math.ceil(.95*len(values))-1],min_ms=min(values),max_ms=max(values),result_sha256=runs[0]['result_sha256'])
        if args.plans:
            plan_path=output/'plans'/args.phase
            plan_path.mkdir(parents=True,exist_ok=True)
            with connect('dwh','etl') as connection:
                connection.execute('SET TRANSACTION READ ONLY')
                connection.execute("SELECT set_config('statement_timeout',%s,true)",(str(args.timeout_ms),))
                try:
                    plan=connection.execute('EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) '+query['sql'],query['params']).fetchone()[0]
                except Exception as exc:
                    plan={'error':type(exc).__name__}
                (plan_path/(query['id']+'.json')).write_text(json.dumps(plan,indent=2,default=str),encoding='utf-8')
    save()
    with (output/f'benchmark_{args.phase}.csv').open('w',newline='',encoding='utf-8') as handle:
        writer=csv.DictWriter(handle,fieldnames=['query','stage','repetition','client','status','elapsed_ms','rows','result_sha256'])
        writer.writeheader();writer.writerows(report['runs'])
    if args.phase == 'after':
        before=json.loads((output/'benchmark_before.json').read_text())
        equivalent=all(report[k]==before[k] for k in ('manifest_sha256','query_sha256','rt_as_of','mode','clients','warmups','requested_samples','timeout_ms','preparation'))
        # JSON normalization makes tuple/list representation immaterial after reload.
        environment_matches = {
            key: digest(report['environment'][key]) == digest(before['environment'].get(key))
            for key in ('settings','business_row_counts','event_corpus_sha256')
        }
        equivalent = equivalent and all(environment_matches.values())
        comparisons={}
        for key,after in report['summary'].items():
            prior=before['summary'].get(key,{})
            if equivalent and prior.get('complete') and after.get('complete') and prior['result_sha256']==after['result_sha256']:
                comparisons[key]={'speedup':prior['median_ms']/after['median_ms'],'improvement_percent':100*(prior['median_ms']-after['median_ms'])/prior['median_ms']}
            else:
                comparisons[key]={'comparable':False}
        (output/'comparison.json').write_text(json.dumps({'same_workload':equivalent,'environment_matches':environment_matches,'queries':comparisons},indent=2),encoding='utf-8')
    print(json.dumps({'phase':args.phase,'variants':len(queries),'complete':sum(s['complete'] for s in report['summary'].values()),'artifact':str(output)}))


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--manifest',type=Path,required=True)
    parser.add_argument('--phase',choices=['before','after'],required=True)
    parser.add_argument('--samples',type=int,default=20)
    parser.add_argument('--warmups',type=int,default=3)
    parser.add_argument('--clients',type=int,choices=[1,5],default=1)
    parser.add_argument('--timeout-ms',type=int,default=120000)
    parser.add_argument('--mode',choices=['isolated','coexistence'],default='isolated')
    parser.add_argument('--plans',action='store_true')
    parser.add_argument('--query',help='Comma-separated query ID prefixes, e.g. Q10,Q12,Q15')
    parser.add_argument('--output-subdir',default='benchmark',help='Relative directory under artifacts/phase5')
    args=parser.parse_args()
    if args.samples<1 or args.warmups<0 or args.timeout_ms<1: parser.error('Invalid measurement bounds')
    run(args)
