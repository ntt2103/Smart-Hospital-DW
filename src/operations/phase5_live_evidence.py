"""Record the live tail separately from the immutable historical source dataset.

This command reads business databases and writes evidence only. It does not
publish events, create bookings, refresh snapshots or run a batch.
"""
import argparse
import hashlib
import json
from pathlib import Path

from psycopg import sql
from data_sources.generator.historical import TABLE_KEYS
from operations.foundation import ROOT, connect


def capture():
    with connect('oltp', 'reader') as source:
        source.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
        source_clock = source.execute('SELECT clock_timestamp()').fetchone()[0]
        counts = {table: source.execute(sql.SQL('SELECT count(*) FROM public.{}').format(sql.Identifier(table))).fetchone()[0]
                  for table in TABLE_KEYS}
    with connect('dwh', 'etl') as warehouse:
        warehouse.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
        clock = warehouse.execute('SELECT clock_timestamp()').fetchone()[0]
        raw = warehouse.execute('SELECT count(*) FROM raw.events').fetchone()[0]
        deliveries = warehouse.execute('SELECT count(*) FROM raw.event_deliveries').fetchone()[0]
        quarantines = warehouse.execute("SELECT count(*) FROM ops.quarantined_records WHERE source_kind='KAFKA'").fetchone()[0]
        digest = hashlib.sha256()
        with warehouse.cursor(name='phase5_live_corpus') as cursor:
            cursor.itersize = 2000
            cursor.execute('SELECT event_id,body_sha256,topic,event_type,entity_id,event_time,published_at,ingested_at FROM raw.events ORDER BY event_id')
            for row in cursor:
                digest.update(json.dumps(row, default=str, separators=(',', ':')).encode())
                digest.update(b'\n')
        snapshot = warehouse.execute('SELECT max(as_of) FROM mart.realtime_department_snapshot').fetchone()[0]
        window = None
        if snapshot:
            signals = warehouse.execute('SELECT a.department_id,a.value,b.value FROM mart.rt01_checkins_last_15m(%s) a JOIN mart.rt02_lab_results_last_15m(%s) b USING(department_id) ORDER BY a.department_id', (snapshot, snapshot)).fetchall()
            rows = warehouse.execute('SELECT d.department_id,s.checkins_last_15m,s.lab_results_last_15m FROM mart.realtime_department_snapshot s JOIN dw.dim_department d USING(department_sk) WHERE s.as_of=%s ORDER BY coalesce(d.department_id,0)', (snapshot,)).fetchall()
            normalized = [(0 if key is None else key, checkins, labs) for key, checkins, labs in rows]
            expected = [(key, int(checkins), int(labs)) for key, checkins, labs in signals]
            assert normalized == expected, 'Snapshot/canonical raw RT totals differ'
            window = {'as_of': snapshot.isoformat(), 'department_rows': len(rows),
                      'checkins': sum(int(r[1]) for r in signals),
                      'lab_results': sum(int(r[2]) for r in signals),
                      'idle_departments': sum(r[1] == 0 and r[2] == 0 for r in signals),
                      'canonical_snapshot_values_equal': True}
        types = warehouse.execute('SELECT topic,event_type,count(*) FROM raw.events GROUP BY 1,2 ORDER BY 1,2').fetchall()
    return {'source_clock': source_clock.isoformat(), 'warehouse_clock': clock.isoformat(),
            'source_counts': counts, 'source_total_rows': sum(counts.values()),
            'canonical_events': raw, 'deliveries': deliveries, 'kafka_quarantines': quarantines,
            'event_corpus_sha256': digest.hexdigest(), 'event_types': types,
            'latest_snapshot': window}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('stage', choices=['baseline', 'freeze'])
    parser.add_argument('--manifest', type=Path, required=True)
    args = parser.parse_args()
    manifest = json.loads(args.manifest.read_text(encoding='utf-8'))
    out = ROOT/'artifacts/phase5'
    out.mkdir(parents=True, exist_ok=True)
    result = capture()
    result['base_dataset_sha256'] = manifest['business_sha256']
    result['base_encounters'] = manifest['counts']['encounters']
    if args.stage == 'baseline':
        assert result['source_counts'] == manifest['counts'], 'Base source counts differ from the generated manifest'
        assert result['canonical_events'] == 0 and result['deliveries'] == 0, 'Historical load must not create Kafka events'
        target = out/'live_baseline.json'
    else:
        baseline = json.loads((out/'live_baseline.json').read_text(encoding='utf-8'))
        assert baseline['base_dataset_sha256'] == manifest['business_sha256']
        result['source_delta_counts'] = {table: count-baseline['source_counts'][table] for table, count in result['source_counts'].items()}
        assert all(count >= 0 for count in result['source_delta_counts'].values()), 'Live verification must not remove historical rows'
        result['source_delta_rows'] = sum(result['source_delta_counts'].values())
        result['canonical_events_delta'] = result['canonical_events']-baseline['canonical_events']
        result['deliveries_delta'] = result['deliveries']-baseline['deliveries']
        assert result['latest_snapshot'] and result['latest_snapshot']['checkins'] > 0 and result['latest_snapshot']['lab_results'] > 0, 'Freeze a nonempty actual RT snapshot'
        assert result['latest_snapshot']['idle_departments'] > 0, 'Corpus must include idle departments'
        target = out/'live_corpus_manifest.json'
    target.write_text(json.dumps(result, indent=2, default=str), encoding='utf-8')
    print(json.dumps({'stage': args.stage, 'source_total_rows': result['source_total_rows'],
                      'canonical_events': result['canonical_events'], 'deliveries': result['deliveries'],
                      'snapshot': result['latest_snapshot'], 'evidence': str(target)}))


if __name__ == '__main__':
    main()
