"""Compose startup jobs. Existing source data is never replaced or reseeded."""
import argparse
import json
import os
import time
from pathlib import Path

from operations.foundation import ROOT, connect, setup

RUNTIME = ROOT / 'artifacts/runtime'


def prepare_source():
    from data_sources.generator.historical import LOAD_ORDER
    from data_sources.generator.phase5 import Config, write_dataset
    from data_sources.loader import load_dataset
    from psycopg import sql

    directory = RUNTIME / 'dataset'
    manifest_path = directory / 'manifest.json'
    with connect('oltp', 'admin') as source:
        populated = any(source.execute(sql.SQL('SELECT EXISTS(SELECT 1 FROM public.{})').format(
            sql.Identifier(table))).fetchone()[0] for table in LOAD_ORDER)
        receipts = source.execute("SELECT to_regclass('_seed.datasets') IS NOT NULL").fetchone()[0]
        known = source.execute('SELECT dataset_sha256 FROM _seed.datasets').fetchall() if receipts else []
        manifest = None
        resume = False
        if manifest_path.exists() and known:
            saved = json.loads(manifest_path.read_text(encoding='utf-8'))
            if any(row[0] == saved['business_sha256'] for row in known):
                loaded = source.execute('SELECT coalesce(sum(row_count),0) FROM _seed.chunks WHERE dataset_sha256=%s',
                                        (saved['business_sha256'],)).fetchone()[0]
                resume = loaded < saved['oltp_total_rows']
                if not resume:
                    manifest = saved
        if not populated or resume:
            config = Config(
                encounters_per_day=int(os.environ.get('BOOTSTRAP_ENCOUNTERS_PER_DAY', '6000')),
                horizon_days=int(os.environ.get('BOOTSTRAP_HORIZON_DAYS', '180')),
                as_of=os.environ.get('SYNTHETIC_AS_OF', Config.as_of),
                seed=int(os.environ.get('SYNTHETIC_RANDOM_SEED', str(Config.seed))))
            print('Preparing/resuming synthetic dataset on an empty or receipt-owned source.', flush=True)
            manifest = write_dataset(directory, config)
            load_dataset(source, directory, isolated_bench=True)
        else:
            print('Existing source detected; skipping generation and loading.', flush=True)
            # Derive the reporting range from actual data, independent of an old snapshot path.
            start, end = source.execute("""SELECT min((created_at AT TIME ZONE 'Asia/Ho_Chi_Minh')::date),
                max((created_at AT TIME ZONE 'Asia/Ho_Chi_Minh')::date) FROM encounters""").fetchone()
            manifest = manifest or {'report_start_date': str(start) if start else '2026-05-06',
                                    'report_end_date': str(end) if end else '2026-11-01'}
            recorded = ROOT / 'artifacts/phase5/dataset_manifest.json'
            if recorded.exists():
                frozen = json.loads(recorded.read_text(encoding='utf-8'))
                if any(row[0] == frozen.get('business_sha256') for row in known):
                    manifest = frozen
    (RUNTIME / 'manifest.json').write_text(json.dumps(manifest, indent=2), encoding='utf-8')


def database():
    from batch_pipeline.jobs import batch, near_rt
    RUNTIME.mkdir(parents=True, exist_ok=True)
    # One bootstrap process per project; release automatically on process/connection failure.
    with connect('dwh', 'admin') as lock:
        lock.execute('SELECT pg_advisory_lock(12447000)')
        lock.commit()
        setup()
        prepare_source()
        batch.setup()
        near_rt.setup()
        with connect('dwh', 'etl') as warehouse:
            ready = warehouse.execute("SELECT EXISTS(SELECT 1 FROM ops.pipeline_runs WHERE pipeline_name='hospital_batch' AND status='SUCCEEDED')").fetchone()[0]
        if not ready:
            print('Running initial warehouse load; large datasets may take several minutes.', flush=True)
            batch.execute('initial')
        else:
            print('Warehouse already initialized; keeping existing facts.', flush=True)
        near_rt.refresh()
    print('Database initialization complete.', flush=True)


def dashboards():
    from analytics.provision import provision
    os.environ.setdefault('ANALYTICS_OUTPUT_DIR', str(RUNTIME))
    provision(5, Path(os.environ.get('SYNTHETIC_MANIFEST', str(RUNTIME / 'manifest.json'))))
    print('Metabase dashboards ready.', flush=True)


def topics():
    from streaming.producer import topics as create_topics
    for attempt in range(12):
        try:
            print(json.dumps({'topics': create_topics()}), flush=True)
            return
        except Exception:
            if attempt == 11:
                raise
            print('Kafka metadata not ready; retrying topic initialization.', flush=True)
            time.sleep(5)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=['database', 'dashboards', 'topics'])
    args = parser.parse_args()
    {'database': database, 'dashboards': dashboards, 'topics': topics}[args.command]()
