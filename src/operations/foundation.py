"""Phase 2 migrations, immutable seeding, raw/stg smoke and role checks.

Run through the foundation Compose tool service. Never resets or drops databases.
"""
import argparse
import hashlib
import json
import os
import re
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

from data_sources.generator.historical import LOAD_ORDER, TABLE_KEYS, generate
from data_quality.source_checks import validate

ROOT = Path(__file__).resolve().parents[2]


def connect(kind, role):
    import psycopg
    users = {('oltp', 'admin'): 'postgres', ('dwh', 'admin'): 'postgres',
             ('oltp', 'writer'): 'hospital_writer', ('oltp', 'reader'): 'hospital_reader',
             ('dwh', 'etl'): 'hospital_etl', ('dwh', 'reporter'): 'hospital_reporter', ('dwh','consumer'):'hospital_consumer'}
    return psycopg.connect(host=f'postgres-{kind}', dbname=f'hospital_{kind}',
                           user=users[kind, role], password=os.environ[f'{kind}_{role}_password'.upper()],
                           options='-c timezone=UTC', connect_timeout=15)


def migration(conn, name, path):
    from psycopg import sql
    text = path.read_text(encoding='utf-8')
    digest = hashlib.sha256(text.encode()).hexdigest()
    # Draft BEGIN/COMMIT are removed so checksum receipt and DDL are atomic.
    text = re.sub(r'(?mi)^\s*(BEGIN|COMMIT);\s*$', '', text)
    with conn.transaction():
        conn.execute('SELECT pg_advisory_xact_lock(12447801)')
        conn.execute('CREATE SCHEMA IF NOT EXISTS _deploy')
        conn.execute('CREATE TABLE IF NOT EXISTS _deploy.migrations(name TEXT PRIMARY KEY, sha256 TEXT NOT NULL, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())')
        existing = conn.execute('SELECT sha256 FROM _deploy.migrations WHERE name=%s', (name,)).fetchone()
        if existing:
            if existing[0] != digest:
                raise ValueError(f'Applied migration changed: {name}; create a new migration')
            return
        conn.execute(text)
        conn.execute('INSERT INTO _deploy.migrations(name,sha256) VALUES (%s,%s)', (name, digest))


def setup():
    from psycopg import sql
    for kind, roles in [('oltp', ['writer', 'reader']), ('dwh', ['etl', 'consumer', 'reporter'])]:
        with connect(kind, 'admin') as conn:
            for role in roles:
                username = f'hospital_{role}'
                exists = conn.execute('SELECT 1 FROM pg_roles WHERE rolname=%s', (username,)).fetchone()
                if not exists:
                    conn.execute(sql.SQL('CREATE ROLE {} LOGIN PASSWORD {}').format(
                        sql.Identifier(username), sql.Literal(os.environ[f'{kind}_{role}_password'.upper()])))
            conn.execute(sql.SQL('REVOKE ALL ON DATABASE {} FROM PUBLIC').format(sql.Identifier(f'hospital_{kind}')))
            for role in roles:
                conn.execute(sql.SQL('GRANT CONNECT ON DATABASE {} TO {}').format(
                    sql.Identifier(f'hospital_{kind}'), sql.Identifier(f'hospital_{role}')))
            conn.execute('REVOKE CREATE ON SCHEMA public FROM PUBLIC')
        if kind == 'oltp':
            with connect(kind, 'admin') as conn:
                migration(conn, '001_source', ROOT/'sql/design/source_schema_draft.sql')
                migration(conn, '005_source_relations', ROOT/'sql/migrations/005_source_relations.sql')
                migration(conn, '013_booking_reception', ROOT/'sql/migrations/013_booking_reception.sql')
                conn.execute('GRANT USAGE ON SCHEMA public TO hospital_reader,hospital_writer')
                conn.execute('GRANT SELECT ON ALL TABLES IN SCHEMA public TO hospital_reader')
                conn.execute('GRANT SELECT,INSERT,UPDATE ON ALL TABLES IN SCHEMA public TO hospital_writer')
                conn.execute('GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA public TO hospital_writer')
        else:
            with connect(kind, 'admin') as conn:
                migration(conn, '001_ops', ROOT/'sql/design/ops_schema_draft.sql')
                migration(conn, '002_raw', ROOT/'sql/design/ingestion_schema_draft.sql')
                for schema in ['raw', 'stg', 'dw', 'mart', 'ops']:
                    conn.execute(sql.SQL('CREATE SCHEMA IF NOT EXISTS {}').format(sql.Identifier(schema)))
                    conn.execute(sql.SQL('GRANT USAGE,CREATE ON SCHEMA {} TO hospital_etl').format(sql.Identifier(schema)))
                    conn.execute(sql.SQL('GRANT ALL ON ALL TABLES IN SCHEMA {} TO hospital_etl').format(sql.Identifier(schema)))
                    conn.execute(sql.SQL('GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA {} TO hospital_etl').format(sql.Identifier(schema)))
                    if schema == 'mart':
                        conn.execute('GRANT USAGE ON SCHEMA mart TO hospital_reporter')
                        conn.execute('GRANT SELECT ON ALL TABLES IN SCHEMA mart TO hospital_reporter')
                        conn.execute('ALTER DEFAULT PRIVILEGES FOR ROLE hospital_etl IN SCHEMA mart GRANT SELECT ON TABLES TO hospital_reporter')
                conn.execute('GRANT USAGE ON SCHEMA raw,ops TO hospital_consumer')
                conn.execute('GRANT SELECT,INSERT ON raw.events,raw.event_deliveries,ops.quarantined_records TO hospital_consumer')
                conn.execute('GRANT USAGE,SELECT ON ALL SEQUENCES IN SCHEMA raw,ops TO hospital_consumer')
    return {'migrations': 'applied_or_checksum_verified', 'runtime_ddl': True}


def seed(snapshot):
    from psycopg import sql
    problems = validate(snapshot)
    if problems:
        raise ValueError(f'Source validation failed: {problems[:5]}')
    counts = {}
    with connect('oltp', 'admin') as conn:
        conn.execute('SELECT pg_advisory_xact_lock(12447802)')
        for name in LOAD_ORDER:
            rows = snapshot['tables'][name]
            columns = list(rows[0])
            table = sql.Identifier('public', name)
            temp = sql.Identifier(f'seed_{name}')
            conn.execute(sql.SQL('CREATE TEMP TABLE {} (LIKE {} INCLUDING DEFAULTS) ON COMMIT DROP').format(temp, table))
            fields = sql.SQL(',').join(map(sql.Identifier, columns))
            with conn.cursor().copy(sql.SQL('COPY {} ({}) FROM STDIN').format(temp, fields)) as copy:
                for row in rows:
                    copy.write_row([row[c] for c in columns])
            key = sql.Identifier(TABLE_KEYS[name])
            conflict = conn.execute(sql.SQL('SELECT p.{} FROM {} p JOIN {} t USING ({}) WHERE to_jsonb(p) IS DISTINCT FROM to_jsonb(t) LIMIT 1').format(key, table, temp, key)).fetchone()
            if conflict:
                raise ValueError(f'Seed conflicts with existing {name}:{conflict[0]}; source preserved')
            result = conn.execute(sql.SQL('INSERT INTO {} ({}) SELECT {} FROM {} ORDER BY {} ON CONFLICT ({}) DO NOTHING').format(table, fields, fields, temp, key, key))
            counts[name] = result.rowcount
            seq = conn.execute('SELECT pg_get_serial_sequence(%s,%s)', (f'public.{name}', TABLE_KEYS[name])).fetchone()[0]
            conn.execute(sql.SQL('SELECT setval(%s,GREATEST((SELECT COALESCE(MAX({}),1) FROM {}), (SELECT last_value FROM {})),true)').format(key, table, sql.Identifier(*seq.split('.'))), (seq,))
    return {'inserted': counts, 'manifest': snapshot['manifest']}


def source_snapshot(manifest):
    from psycopg import sql
    with connect('oltp', 'reader') as conn:
        conn.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
        tables = {name: [r[0] for r in conn.execute(sql.SQL('SELECT to_jsonb(t) FROM public.{} t ORDER BY {}').format(
            sql.Identifier(name), sql.Identifier(pk)))] for name, pk in TABLE_KEYS.items()}
    return {'manifest': manifest, 'tables': tables}


def smoke(manifest):
    from psycopg import sql
    from psycopg.types.json import Jsonb
    snapshot = source_snapshot(manifest)
    errors = validate(snapshot)
    if errors:
        raise ValueError(f'Runtime source validation failed: {errors[:5]}')
    run_id = uuid.uuid4()
    now = datetime.now(timezone.utc)
    counts = {}
    # One transaction commits raw, typed staging and the audit together.
    with connect('oltp', 'reader') as source, connect('dwh', 'etl') as target:
        source.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
        target.execute('SELECT pg_advisory_xact_lock(12447803)')
        target.execute("INSERT INTO ops.pipeline_runs(run_id,pipeline_name,run_kind,status,started_at,input_bounds) VALUES (%s,'foundation_smoke','BATCH','RUNNING',%s,%s)",
                       (run_id, now, Jsonb({'manifest_sha256': manifest['business_sha256'], 'as_of': manifest['as_of']})))
        for name, pk in TABLE_KEYS.items():
            columns = source.execute("SELECT a.attname,pg_catalog.format_type(a.atttypid,a.atttypmod) FROM pg_attribute a WHERE a.attrelid=%s::regclass AND a.attnum>0 AND NOT a.attisdropped ORDER BY a.attnum", (f'public.{name}',)).fetchall()
            definition = sql.SQL(',').join(sql.SQL('{} {}').format(sql.Identifier(c), sql.SQL(t)) for c,t in columns)
            target.execute(sql.SQL('CREATE TABLE IF NOT EXISTS stg.{} ({}, PRIMARY KEY ({}))').format(sql.Identifier(name), definition, sql.Identifier(pk)))
            # Use exactly the frozen source snapshot that passed validation.
            rows = snapshot['tables'][name]
            with target.cursor().copy('COPY raw.source_rows (batch_id,source_table,source_pk,source_updated_at,extracted_at,payload) FROM STDIN') as copy:
                for row in rows:
                    copy.write_row((run_id, name, row[pk], row['updated_at'], now, Jsonb(row)))
            fields = sql.SQL(',').join(sql.Identifier(c) for c,t in columns)
            updates = sql.SQL(',').join(sql.SQL('{}=EXCLUDED.{}').format(sql.Identifier(c), sql.Identifier(c)) for c,t in columns if c != pk)
            target.execute(sql.SQL('INSERT INTO stg.{} ({}) SELECT (jsonb_populate_record(NULL::stg.{},payload)).* FROM raw.source_rows WHERE batch_id=%s AND source_table=%s ON CONFLICT ({}) DO UPDATE SET {}').format(
                sql.Identifier(name), fields, sql.Identifier(name), sql.Identifier(pk), updates), (run_id, name))
            n = target.execute(sql.SQL('SELECT count(*) FROM stg.{}').format(sql.Identifier(name))).fetchone()[0]
            if n != len(rows):
                raise ValueError(f'Staging key-count mismatch: {name}')
            counts[name] = n
        total = sum(counts.values())
        target.execute("UPDATE ops.pipeline_runs SET status='SUCCEEDED',finished_at=clock_timestamp(),extracted_count=%s,raw_count=%s,staged_count=%s WHERE run_id=%s", (total,total,total,run_id))
    return {'run_id': str(run_id), 'raw_stg_counts': counts, 'source_dq_errors': 0}


def permissions():
    from psycopg import errors
    checks = {}
    # EXPLAIN performs privilege checking without mutating business records.
    for kind, role, statement in [('oltp', 'reader', 'EXPLAIN UPDATE public.patients SET is_active=false'),
                                   ('dwh', 'reporter', 'SELECT count(*) FROM raw.source_rows')]:
        try:
            with connect(kind, role) as conn:
                conn.execute(statement)
        except errors.InsufficientPrivilege:
            checks[f'{kind}_{role}_denied'] = True
        else:
            raise ValueError(f'Unexpected privilege for {kind}/{role}')
    with connect('dwh', 'etl') as conn:
        conn.execute('CREATE OR REPLACE VIEW mart.foundation_health AS SELECT 1 AS ready')
    with connect('dwh', 'reporter') as conn:
        checks['reporter_select_mart'] = conn.execute('SELECT ready FROM mart.foundation_health').fetchone()[0] == 1
    return checks


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('command', choices=['setup', 'seed', 'smoke', 'verify', 'all'])
    parser.add_argument('--scale', choices=['small', 'baseline'], default='small')
    args = parser.parse_args()
    report = {'scope': 'Phase 2 foundation runtime', 'checks': {}, 'completed': False}
    output = ROOT/'artifacts/phase2/runtime.json'
    output.parent.mkdir(parents=True, exist_ok=True)
    try:
        snapshot = generate(args.scale, int(os.environ.get('SYNTHETIC_RANDOM_SEED', 20261022)),
                            os.environ.get('SYNTHETIC_AS_OF', '2026-10-22T00:00:00+07:00'))
        if args.command in ('setup', 'all'):
            report['checks']['setup'] = setup()
        if args.command in ('seed', 'all'):
            report['checks']['seed'] = seed(snapshot)
        if args.command in ('smoke', 'all'):
            report['checks']['smoke'] = smoke(snapshot['manifest'])
        if args.command in ('verify', 'all'):
            report['checks']['permissions'] = permissions()
        report['completed'] = True
    except Exception as exc:
        # Avoid DSNs, passwords or server exception detail in routine output.
        report['error_type'] = type(exc).__name__
        report['completed'] = False
        print(f"Foundation failed: {type(exc).__name__}; inspect the isolated database/server logs.", file=sys.stderr)
    finally:
        output.write_text(json.dumps(report, indent=2, default=str), encoding='utf-8')
        print(json.dumps({'completed': report['completed'], 'evidence': str(output)}))
    if not report['completed']:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
