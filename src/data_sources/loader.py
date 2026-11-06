"""Receipt-based COPY of Phase 5 chunks into an explicitly isolated empty database.

No truncation, FK bypass, trigger bypass or automatic Kafka publication.
"""
import argparse
import hashlib
import json
import time
from pathlib import Path

from data_sources.generator.historical import LOAD_ORDER, TABLE_KEYS
from data_quality.source_checks import validate_database


def load_dataset(conn, directory, *, isolated_bench=False, validate=True):
    from psycopg import sql
    if not isolated_bench:
        raise ValueError('Loader requires an explicitly isolated benchmark database')
    started=time.monotonic()
    directory=Path(directory)
    manifest=json.loads((directory/'manifest.json').read_text(encoding='utf-8'))
    digest=manifest['business_sha256']
    with conn.transaction():
        conn.execute('SELECT pg_advisory_xact_lock(12447855)')
        conn.execute('CREATE SCHEMA IF NOT EXISTS _seed')
        conn.execute('CREATE TABLE IF NOT EXISTS _seed.datasets(dataset_sha256 TEXT PRIMARY KEY,config_sha256 TEXT NOT NULL,created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp())')
        conn.execute('CREATE TABLE IF NOT EXISTS _seed.chunks(dataset_sha256 TEXT NOT NULL REFERENCES _seed.datasets,source_table TEXT NOT NULL,path TEXT NOT NULL,sha256 TEXT NOT NULL,row_count BIGINT NOT NULL,committed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),PRIMARY KEY(dataset_sha256,path))')
        known=conn.execute('SELECT dataset_sha256 FROM _seed.datasets').fetchall()
        if known and any(row[0]!=digest for row in known):
            raise ValueError('Different dataset already owns this benchmark database')
        if not known:
            for table in LOAD_ORDER:
                if conn.execute(sql.SQL('SELECT EXISTS(SELECT 1 FROM public.{})').format(sql.Identifier(table))).fetchone()[0]:
                    raise ValueError(f'Nonempty source {table}; existing data is preserved')
            conn.execute('INSERT INTO _seed.datasets(dataset_sha256,config_sha256) VALUES(%s,%s)',(digest,manifest['config_sha256']))
    loaded=skipped=0
    checksum_seconds=copy_seconds=0
    # Serialize concurrent/resumed seed processes across individual transactions.
    conn.execute('SELECT pg_advisory_lock(12447856)')
    conn.commit()
    try:
        for table in LOAD_ORDER:
            definition=manifest['tables'][table]
            fields=sql.SQL(',').join(map(sql.Identifier,definition['columns']))
            for chunk in definition['chunks']:
                path=(directory/chunk['path']).resolve()
                if not path.is_relative_to(directory.resolve()):
                    raise ValueError('Chunk path escapes dataset directory')
                hasher=hashlib.sha256()
                checksum_started=time.monotonic()
                with path.open('rb') as source:
                    for block in iter(lambda:source.read(1024*1024),b''): hasher.update(block)
                if hasher.hexdigest()!=chunk['sha256']:
                    raise ValueError(f'Chunk hash mismatch: {chunk["path"]}')
                checksum_seconds+=time.monotonic()-checksum_started
                with conn.transaction():
                    receipt=conn.execute('SELECT sha256,row_count FROM _seed.chunks WHERE dataset_sha256=%s AND path=%s',(digest,chunk['path'])).fetchone()
                    if receipt:
                        if receipt!=(chunk['sha256'],chunk['rows']):
                            raise ValueError('Existing chunk receipt differs')
                        skipped+=chunk['rows']
                        continue
                    statement=sql.SQL("COPY public.{} ({}) FROM STDIN WITH(FORMAT CSV,NULL '\\N')").format(sql.Identifier(table),fields)
                    copy_started=time.monotonic()
                    with path.open('rb') as source,conn.cursor().copy(statement) as copy:
                        for block in iter(lambda:source.read(1024*1024),b''): copy.write(block)
                    copy_seconds+=time.monotonic()-copy_started
                    conn.execute('INSERT INTO _seed.chunks(dataset_sha256,source_table,path,sha256,row_count) VALUES(%s,%s,%s,%s,%s)',(digest,table,chunk['path'],chunk['sha256'],chunk['rows']))
                    loaded+=chunk['rows']
                print(json.dumps({'committed_chunk':chunk['path'],'rows':chunk['rows']}),flush=True)
            with conn.transaction():
                actual=conn.execute(sql.SQL('SELECT count(*) FROM public.{}').format(sql.Identifier(table))).fetchone()[0]
                if actual!=definition['rows']: raise ValueError(f'Loaded count mismatch: {table}')
                seq=conn.execute('SELECT pg_get_serial_sequence(%s,%s)',(f'public.{table}',TABLE_KEYS[table])).fetchone()[0]
                if definition['rows']:
                    conn.execute(sql.SQL('SELECT setval(%s,(SELECT max({}) FROM public.{}),true)').format(sql.Identifier(TABLE_KEYS[table]),sql.Identifier(table)),(seq,))
        with conn.transaction():
            invalid=conn.execute("SELECT conname FROM pg_constraint WHERE connamespace='public'::regnamespace AND NOT convalidated").fetchall()
            if invalid: raise ValueError('Source constraints have not been validated')
            constraint_count=conn.execute("SELECT count(*) FROM pg_constraint WHERE connamespace='public'::regnamespace AND convalidated").fetchone()[0]
            dq_evidence={}
            dq_started=time.monotonic()
            issues=validate_database(conn,manifest['as_of'],evidence=dq_evidence) if validate else []
            dq_seconds=time.monotonic()-dq_started
            if issues: raise ValueError(f'Source DQ failed: {issues[:10]}')
            storage={}
            for table in LOAD_ORDER:
                values=conn.execute('SELECT pg_table_size(%s::regclass),pg_indexes_size(%s::regclass),pg_total_relation_size(%s::regclass)',(f'public.{table}',)*3).fetchone()
                storage[table]=dict(zip(('table_bytes','index_bytes','total_bytes'),values))
            database_bytes=conn.execute('SELECT pg_database_size(current_database())').fetchone()[0]
        try:
            import resource
            peak_rss=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024
        except ImportError:
            peak_rss=None
        return {'dataset_sha256':digest,'loaded_rows':loaded,'resumed_rows':skipped,'source_counts':manifest['counts'],
                'core_source_rows':manifest['core_source_rows'],'oltp_total_rows':manifest['oltp_total_rows'],
                'source_dq_errors':len(issues),'full_dq_executed':validate,'constraints_validated':True,
                'validated_constraint_count':constraint_count,'dq':dq_evidence,'table_storage':storage,
                'database_bytes':database_bytes,'elapsed_seconds':round(time.monotonic()-started,3),
                'copy_seconds':round(copy_seconds,3),'checksum_seconds':round(checksum_seconds,3),
                'dq_seconds':round(dq_seconds,3),'peak_process_rss_bytes':peak_rss}
    finally:
        conn.execute('SELECT pg_advisory_unlock(12447856)')
        conn.commit()


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('directory',type=Path)
    parser.add_argument('--isolated-bench',action='store_true')
    parser.add_argument('--evidence',type=Path,default=Path('artifacts/phase5/source_load.json'))
    args=parser.parse_args()
    from operations.foundation import connect
    with connect('oltp','admin') as conn:
        result=load_dataset(conn,args.directory,isolated_bench=args.isolated_bench)
    args.evidence.parent.mkdir(parents=True,exist_ok=True)
    args.evidence.write_text(json.dumps(result,indent=2),encoding='utf-8')
    print(json.dumps(result))


if __name__=='__main__': main()
