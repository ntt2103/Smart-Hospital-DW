"""Atomic incremental batch with bounded cursors and exact full reconciliation.

All source rows are checked by SQL DQ. Extraction memory is bounded by a server
cursor; unchanged historical payloads are compared in ordered hash streams.
Facts are restated in SQL to preserve corrections and time dependent eligibility.
"""
import argparse
import hashlib
import json
import os
import time
import uuid
from datetime import datetime, timedelta, timezone
from itertools import zip_longest
from psycopg import sql
from psycopg.types.json import Jsonb
from operations.foundation import ROOT, connect, migration
from data_sources.generator.historical import TABLE_KEYS

TRANSITIONS = {
    'appointments': {'SCHEDULED': {'ATTENDED', 'NO_SHOW', 'CANCELLED', 'RESCHEDULED'}},
    'encounters': {'REGISTERED': {'CHECKED_IN', 'IN_CONSULTATION', 'COMPLETED', 'CANCELLED'},
                   'CHECKED_IN': {'IN_CONSULTATION', 'COMPLETED', 'CANCELLED'},
                   'IN_CONSULTATION': {'COMPLETED'}, 'ADMITTED': {'DISCHARGED'}},
    'lab_orders': {'ORDERED': {'COLLECTED', 'PROCESSING', 'COMPLETED', 'CANCELLED'},
                   'COLLECTED': {'PROCESSING', 'COMPLETED', 'CANCELLED'},
                   'PROCESSING': {'COMPLETED', 'CANCELLED'}},
}
FACTS = ['fact_appointment', 'fact_encounter', 'fact_bed_assignment',
         'fact_bed_snapshot_daily', 'fact_lab_order']
BRIDGES = {'bridge_encounter_doctor': 'encounter_doctors',
           'bridge_encounter_diagnosis': 'encounter_diagnoses'}


def setup():
    with connect('oltp', 'reader') as source, connect('dwh', 'admin') as c:
        c.execute(sql.SQL('GRANT TEMPORARY ON DATABASE {} TO hospital_etl').format(sql.Identifier(c.info.dbname)))
        migration(c, '003_warehouse', ROOT/'sql/design/warehouse_schema_draft.sql')
        migration(c, '004_kpis', ROOT/'sql/design/kpi_queries.sql')
        migration(c, '005_batch_helpers', ROOT/'sql/warehouse/ddl/batch_helpers.sql')
        migration(c, '007_phase5_warehouse', ROOT/'sql/migrations/007_phase5_warehouse.sql')
        # Source type catalog is authoritative; this also creates extension staging.
        for name, pk in TABLE_KEYS.items():
            columns = source.execute("SELECT attname,format_type(atttypid,atttypmod) FROM pg_attribute WHERE attrelid=%s::regclass AND attnum>0 AND NOT attisdropped ORDER BY attnum", ('public.'+name,)).fetchall()
            definition = sql.SQL(',').join(sql.SQL('{} {}').format(sql.Identifier(field), sql.SQL(kind)) for field, kind in columns)
            c.execute(sql.SQL('CREATE TABLE IF NOT EXISTS stg.{} ({},PRIMARY KEY({}))').format(sql.Identifier(name), definition, sql.Identifier(pk)))
        migration(c, '008_phase5_staging_indexes', ROOT/'sql/migrations/008_phase5_staging_indexes.sql')
        migration(c, '014_noshow_30_minutes', ROOT/'sql/migrations/014_noshow_30_minutes.sql')
        for schema in ['dw', 'mart', 'stg', 'ops', 'raw']:
            c.execute(sql.SQL('GRANT ALL ON ALL TABLES IN SCHEMA {} TO hospital_etl').format(sql.Identifier(schema)))
            c.execute(sql.SQL('GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA {} TO hospital_etl').format(sql.Identifier(schema)))
        c.execute('GRANT SELECT ON ALL TABLES IN SCHEMA mart TO hospital_reporter')


def _stream(connection, statement, params=None):
    """Use a server side cursor, never psycopg's default fully buffered result."""
    with connection.cursor(name='batch_'+uuid.uuid4().hex) as cursor:
        cursor.itersize = 2000
        cursor.execute(statement, params)
        yield from cursor


def _reconcile(source, target, name, pk):
    query = sql.SQL('SELECT {},md5(to_jsonb(t)::text) FROM {}.{} t ORDER BY {}')
    left = _stream(source, query.format(sql.Identifier(pk), sql.Identifier('public'), sql.Identifier(name), sql.Identifier(pk)))
    right = _stream(target, query.format(sql.Identifier(pk), sql.Identifier('stg'), sql.Identifier(name), sql.Identifier(pk)))
    count = 0
    digest = hashlib.sha256()
    for expected, actual in zip_longest(left, right):
        if expected != actual:
            raise ValueError(f'Payload/key reconciliation failed: {name}; run backfill')
        count += 1
        digest.update(f'{expected[0]}:{expected[1]}\n'.encode())
    return count, digest.hexdigest()


def _check_transitions(target, name, pk, run_id):
    # No array of millions of selected business keys is allocated in Python.
    query = sql.SQL("SELECT s.{},s.status,r.payload->>'status' FROM raw.source_rows r JOIN stg.{} s ON s.{}=r.source_pk WHERE r.batch_id=%s AND r.source_table=%s AND s.status IS DISTINCT FROM r.payload->>'status'")
    for key, previous, current in _stream(target, query.format(sql.Identifier(pk), sql.Identifier(name), sql.Identifier(pk)), (run_id, name)):
        if current not in TRANSITIONS[name].get(previous, set()):
            raise ValueError(f'Illegal observed transition {name}:{key} {previous}->{current}; audited backfill required for correction')


def execute(mode='incremental', fail_after_stage=False, bounds=None):
    from data_quality.source_checks import validate_database
    if mode not in ('initial', 'incremental', 'backfill'):
        raise ValueError('Invalid batch mode')
    bounds = bounds or {}
    date_bounds = [datetime.fromisoformat(bounds[k]).date() if bounds.get(k) else None for k in ('start', 'end')]
    if all(date_bounds) and date_bounds[1] < date_bounds[0]:
        raise ValueError('Invalid backfill bounds')
    run_id = uuid.uuid4()
    monotonic_started = time.perf_counter()
    started = None
    source_observed_at = None
    warehouse_started_at = None
    extracted = 0
    committed = False
    rejected = {}
    try:
        with connect('oltp', 'reader') as src, connect('dwh', 'etl') as dst:
            src.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
            source_observed_at = src.execute('SELECT clock_timestamp()').fetchone()[0]
            warehouse_started_at = dst.execute('SELECT clock_timestamp()').fetchone()[0]
            # Audit start and finish share the DWH clock. Source time defines
            # business eligibility and observed SCD2 boundaries independently.
            started = warehouse_started_at
            cutoff = source_observed_at-timedelta(seconds=120)
            if not dst.execute('SELECT pg_try_advisory_xact_lock(12447803)').fetchone()[0]:
                raise RuntimeError('Another batch is running')
            dst.execute("INSERT INTO ops.pipeline_runs(run_id,pipeline_name,run_kind,status,started_at,input_bounds) VALUES (%s,'hospital_batch',%s,'RUNNING',%s,%s)",
                        (run_id, 'BACKFILL' if mode == 'backfill' else 'BATCH', started,
                         Jsonb({'mode': mode, 'business_cutoff': cutoff.isoformat(), 'requested_bounds': bounds,
                                'source_observed_at': source_observed_at.isoformat(),
                                'warehouse_started_at': warehouse_started_at.isoformat(),
                                'source_minus_warehouse_seconds': (source_observed_at-warehouse_started_at).total_seconds()})))
            issues = validate_database(src, source_observed_at+timedelta(seconds=5))
            if issues:
                keys = []
                for issue in issues[:100]:
                    key = str(issue.get('key', '')).split(':')[-1]
                    if key.isdigit():
                        keys.append(int(key))
                records = {}
                for name, pk in TABLE_KEYS.items():
                    records[name] = [r[0] for r in src.execute(sql.SQL('SELECT to_jsonb(t) FROM public.{} t WHERE {}=ANY(%s) LIMIT 100').format(sql.Identifier(name), sql.Identifier(pk)), (keys,))]
                rejected = {'issues': issues, 'records': records}
                raise ValueError(json.dumps(issues[:10]))
            source_counts, reconciliation = {}, {}
            for name, pk in TABLE_KEYS.items():
                upper = src.execute(sql.SQL('SELECT updated_at,{} FROM public.{} ORDER BY updated_at DESC,{} DESC LIMIT 1').format(sql.Identifier(pk), sql.Identifier(name), sql.Identifier(pk))).fetchone()
                watermark = dst.execute("SELECT high_water_updated_at,high_water_pk FROM ops.watermarks WHERE pipeline_name='hospital_batch' AND source_name=%s", (name,)).fetchone()
                lower = watermark[0]-timedelta(minutes=120) if watermark and mode == 'incremental' else datetime.min.replace(tzinfo=timezone.utc)
                selected = 0
                if upper:
                    statement = sql.SQL('SELECT to_jsonb(t) FROM public.{} t WHERE updated_at >= %s AND (updated_at,{}) <= (%s,%s) ORDER BY updated_at,{}').format(sql.Identifier(name), sql.Identifier(pk), sql.Identifier(pk))
                    with dst.cursor().copy('COPY raw.source_rows(batch_id,source_table,source_pk,source_updated_at,extracted_at,payload) FROM STDIN') as copy:
                        for row, in _stream(src, statement, (lower, *upper)):
                            copy.write_row((run_id, name, row[pk], row['updated_at'], started, Jsonb(row)))
                            selected += 1
                extracted += selected
                if name in TRANSITIONS and selected and mode != 'backfill':
                    _check_transitions(dst, name, pk, run_id)
                fields = [r[0] for r in src.execute("SELECT attname FROM pg_attribute WHERE attrelid=%s::regclass AND attnum>0 AND NOT attisdropped ORDER BY attnum", ('public.'+name,))]
                field_sql = sql.SQL(',').join(map(sql.Identifier, fields))
                updates = sql.SQL(',').join(sql.SQL('{}=EXCLUDED.{}').format(sql.Identifier(field), sql.Identifier(field)) for field in fields if field != pk)
                dst.execute(sql.SQL('INSERT INTO stg.{} AS current ({}) SELECT (jsonb_populate_record(NULL::stg.{},payload)).* FROM raw.source_rows WHERE batch_id=%s AND source_table=%s ON CONFLICT ({}) DO UPDATE SET {} WHERE EXCLUDED.updated_at>=current.updated_at').format(sql.Identifier(name), field_sql, sql.Identifier(name), sql.Identifier(pk), updates), (run_id, name))
                if upper:
                    dst.execute("INSERT INTO ops.watermarks(pipeline_name,source_name,high_water_updated_at,high_water_pk,last_run_id,version) VALUES ('hospital_batch',%s,%s,%s,%s,1) ON CONFLICT (pipeline_name,source_name) DO UPDATE SET high_water_updated_at=EXCLUDED.high_water_updated_at,high_water_pk=EXCLUDED.high_water_pk,last_run_id=EXCLUDED.last_run_id,version=ops.watermarks.version+1,recorded_at=clock_timestamp()", (name, *upper, run_id))
                count, digest = _reconcile(src, dst, name, pk)
                source_counts[name] = count
                reconciliation[name] = {'rows': count, 'ordered_payload_digest': digest}
                dst.execute("INSERT INTO ops.reconciliation_results(run_id,scope_key,source_name,target_name,extraction_cut,source_count,target_count,result) VALUES (%s,'full_payload_stream',%s,%s,%s,%s,%s,'PASS')", (run_id, name, 'stg.'+name, source_observed_at, count, count))
            if fail_after_stage:
                raise RuntimeError('Injected failure before warehouse/watermark commit')
            dst.execute('CREATE TEMP TABLE run_context(run_id UUID,as_of TIMESTAMPTZ,observed_at TIMESTAMPTZ,snapshot_start DATE,snapshot_end DATE) ON COMMIT DROP')
            dst.execute('INSERT INTO run_context VALUES (%s,%s,%s,%s,%s)', (run_id, cutoff, source_observed_at, *date_bounds))
            dst.execute((ROOT/'sql/warehouse/transformations/batch.sql').read_text(encoding='utf-8'))
            fact_counts = {name: dst.execute(sql.SQL('SELECT count(*) FROM dw.{}').format(sql.Identifier(name))).fetchone()[0] for name in FACTS}
            expected_counts = {'fact_appointment': source_counts['appointments'], 'fact_encounter': source_counts['encounters'],
                               'fact_bed_assignment': source_counts['bed_assignments'], 'fact_lab_order': source_counts['lab_orders']}
            bridge_counts = {name: dst.execute(sql.SQL('SELECT count(*) FROM dw.{}').format(sql.Identifier(name))).fetchone()[0] for name in BRIDGES}
            for name, count in {**expected_counts, **{b: source_counts[s] for b, s in BRIDGES.items()}}.items():
                actual = fact_counts.get(name, bridge_counts.get(name))
                if actual != count:
                    raise ValueError(f'Fact/bridge reconciliation: {name}')
            # Exact business-key sets in both directions, not count-only equality.
            for target, source, key in [('fact_encounter', 'encounters', 'encounter_id'),
                                        ('fact_appointment', 'appointments', 'appointment_id'),
                                        ('fact_bed_assignment', 'bed_assignments', 'bed_assignment_id'),
                                        ('fact_lab_order', 'lab_orders', 'lab_order_id'),
                                        *[(b, s, 'association_id') for b, s in BRIDGES.items()]]:
                different = dst.execute(sql.SQL('SELECT EXISTS((SELECT {} FROM dw.{} EXCEPT SELECT {} FROM stg.{}) UNION ALL (SELECT {} FROM stg.{} EXCEPT SELECT {} FROM dw.{}))').format(sql.Identifier(key), sql.Identifier(target), sql.Identifier(key), sql.Identifier(source), sql.Identifier(key), sql.Identifier(source), sql.Identifier(key), sql.Identifier(target))).fetchone()[0]
                if different:
                    raise ValueError('Warehouse key set reconciliation: '+target)
            if dst.execute('SELECT count(*) FROM dw.fact_bed_snapshot_daily WHERE NOT is_snapshot_valid').fetchone()[0]:
                raise ValueError('Invalid bed snapshots')
            overlap = dst.execute("SELECT EXISTS(SELECT 1 FROM dw.dim_doctor a JOIN dw.dim_doctor b ON a.doctor_id=b.doctor_id AND a.doctor_sk<b.doctor_sk AND tstzrange(a.effective_from,a.effective_to,'[)') && tstzrange(b.effective_from,b.effective_to,'[)'))").fetchone()[0]
            if overlap:
                raise ValueError('SCD2 overlapping versions')
            for group in range(1, 9):
                dst.execute("INSERT INTO ops.dq_results(run_id,rule_id,scope_key,severity,result,checked_count,failed_count,query_version) VALUES (%s,%s,'source_and_warehouse','ERROR','PASS',%s,0,'phase5-sql-v1')", (run_id, f'DQ0{group}', sum(source_counts.values())))
            dst.execute("UPDATE ops.pipeline_runs SET status='SUCCEEDED',finished_at=clock_timestamp(),extracted_count=%s,raw_count=%s,staged_count=%s,upserted_count=%s WHERE run_id=%s", (extracted, extracted, extracted, sum(fact_counts.values())+sum(bridge_counts.values()), run_id))
        committed = True
        result = {'run_id': str(run_id), 'mode': mode, 'extracted': extracted, 'fact_counts': fact_counts,
                  'bridge_counts': bridge_counts, 'source_counts': source_counts, 'reconciliation': reconciliation,
                  'dq_groups_passed': 8, 'key_reconciliations': len(TABLE_KEYS),
                  'source_clock': source_observed_at.isoformat(), 'warehouse_clock': warehouse_started_at.isoformat(),
                  'source_minus_warehouse_seconds': (source_observed_at-warehouse_started_at).total_seconds(),
                  'elapsed_seconds': time.perf_counter()-monotonic_started}
        if not os.environ.get('BATCH_ATTEMPT'):
            out = ROOT/'artifacts/phase5'
            out.mkdir(parents=True, exist_ok=True)
            (out/'latest_batch.json').write_text(json.dumps(result, indent=2), encoding='utf-8')
        print(json.dumps(result))
        return result
    except Exception as exc:
        if committed:
            raise
        with connect('dwh', 'etl') as c:
            if started is None:
                started = c.execute('SELECT clock_timestamp()').fetchone()[0]
            c.execute("INSERT INTO ops.pipeline_runs(run_id,pipeline_name,run_kind,status,started_at,finished_at,error_code,error_message,input_bounds) VALUES (%s,'hospital_batch','BATCH','FAILED',%s,clock_timestamp(),%s,%s,%s)",
                      (run_id, started, type(exc).__name__, str(exc)[:1000],
                       Jsonb({'mode': mode, 'source_observed_at': source_observed_at.isoformat() if source_observed_at else None,
                              'warehouse_started_at': warehouse_started_at.isoformat() if warehouse_started_at else None,
                              'elapsed_seconds': time.perf_counter()-monotonic_started})))
            c.execute("INSERT INTO ops.quarantined_records(run_id,source_kind,source_name,rule_id,severity,reason,payload) VALUES (%s,'TRANSFORM','hospital_batch','DQ_GATE','ERROR',%s,%s)", (run_id, str(exc)[:1000], Jsonb({'mode': mode, 'failure': type(exc).__name__, **rejected})))
            c.execute("INSERT INTO ops.dq_results(run_id,rule_id,scope_key,severity,result,checked_count,failed_count,query_version) VALUES (%s,'DQ_GATE','batch_transaction','ERROR','FAIL',1,1,'phase5-sql-v1')", (run_id,))
        raise


if __name__ == '__main__':
    p = argparse.ArgumentParser()
    p.add_argument('--setup', action='store_true')
    p.add_argument('--mode', choices=['initial', 'incremental', 'backfill'], default=os.environ.get('BATCH_MODE', 'incremental'))
    p.add_argument('--start-date', default=os.environ.get('BATCH_START_DATE') or None)
    p.add_argument('--end-date', default=os.environ.get('BATCH_END_DATE') or None)
    args = p.parse_args()
    if args.setup:
        setup()
    execute(args.mode, fail_after_stage=os.environ.get('BATCH_FAIL_FIRST') == 'true' and os.environ.get('BATCH_ATTEMPT') == '1', bounds={'start': args.start_date, 'end': args.end_date})
