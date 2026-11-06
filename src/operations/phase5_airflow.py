"""Trigger one isolated Phase 5 DAG and restore its original pause state."""
import argparse
import json
import os
import time
import uuid
from datetime import datetime, timezone

from operations.airflow_control import Airflow
from operations.foundation import ROOT, connect
from data_sources.generator.historical import TABLE_KEYS


def source_counts():
    from psycopg import sql
    with connect('oltp', 'reader') as connection:
        connection.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
        return {table: connection.execute(sql.SQL('SELECT count(*) FROM public.{}').format(
            sql.Identifier(table))).fetchone()[0] for table in TABLE_KEYS}


def run(dag_id, timeout_seconds):
    if os.environ.get('COMPOSE_PROJECT_NAME') != 'smart_hospital_dw_phase5':
        raise ValueError('DAG acceptance is restricted to the isolated Phase 5 stack')
    client = None
    original_paused = None
    result = {'dag_id': dag_id, 'timeout_seconds': timeout_seconds,
              'state': 'not_triggered', 'observations': []}
    started = time.monotonic()
    error = None

    def request(method, path, body=None):
        nonlocal client
        try:
            return client.request(method, path, body)
        except RuntimeError as exc:
            if 'HTTP401' not in str(exc):
                raise
            # Long acceptance runs may outlive a short API access token.
            client = Airflow()
            return client.request(method, path, body)

    try:
        result['source_counts_before'] = source_counts()
        client = Airflow()
        # A new processor may need a few cycles to serialize these real DAGs.
        discovery_deadline = time.monotonic() + min(120, timeout_seconds)
        while True:
            try:
                before = request('GET', f'/api/v2/dags/{dag_id}')
                break
            except RuntimeError as exc:
                if 'HTTP404' not in str(exc) or time.monotonic() >= discovery_deadline:
                    raise
                time.sleep(2)
        original_paused = before['is_paused']
        result['original_paused'] = original_paused
        # Snapshot consumers must be quiet while the paired SQL benchmark runs.
        # The caller schedules this acceptance before freeze, or after pairing.
        request('PATCH', f'/api/v2/dags/{dag_id}', {'is_paused': False})
        run_id = 'phase5_acceptance_' + uuid.uuid4().hex
        request('POST', f'/api/v2/dags/{dag_id}/dagRuns', {
            'dag_run_id': run_id, 'logical_date': datetime.now(timezone.utc).isoformat(),
            'conf': {'mode': 'incremental'},
        })
        result['run_id'] = run_id
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            observed = request('GET', f'/api/v2/dags/{dag_id}/dagRuns/{run_id}')
            result['state'] = observed['state']
            if not result['observations'] or result['observations'][-1]['state'] != observed['state']:
                observation = {'state': observed['state'], 'elapsed_seconds': time.monotonic()-started}
                result['observations'].append(observation)
                print(json.dumps({'dag_id': dag_id, **observation}), flush=True)
            if observed['state'] in ('success', 'failed'):
                result['tasks'] = request('GET', f'/api/v2/dags/{dag_id}/dagRuns/{run_id}/taskInstances')
                break
            time.sleep(2)
        else:
            result['censored_timeout'] = True
        result['source_counts_after'] = source_counts()
        result['source_counts_unchanged'] = result['source_counts_before'] == result['source_counts_after']
    except Exception as exc:
        error = exc
        result['error_type'] = type(exc).__name__
    finally:
        result['elapsed_seconds'] = time.monotonic() - started
        if client is not None and original_paused is not None:
            try:
                request('PATCH', f'/api/v2/dags/{dag_id}', {'is_paused': original_paused})
                result['restored_paused'] = request('GET', f'/api/v2/dags/{dag_id}')['is_paused']
            except Exception as exc:
                result['pause_restore_error_type'] = type(exc).__name__
                if error is None:
                    error = exc
        folder = ROOT/'artifacts/phase5'
        folder.mkdir(parents=True, exist_ok=True)
        path = folder/(dag_id+'_airflow.json')
        temporary = path.with_suffix('.json.part')
        temporary.write_text(json.dumps(result, indent=2, default=str), encoding='utf-8')
        temporary.replace(path)
    if error is not None:
        raise error
    assert result['state'] == 'success', result
    assert result['restored_paused'] == original_paused
    assert result['source_counts_unchanged'], result
    print(json.dumps({'dag_id': dag_id, 'state': result['state'], 'elapsed_seconds': result['elapsed_seconds'],
                      'pause_state_restored': True}))
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('dag', choices=['hospital_batch', 'hospital_near_rt'])
    parser.add_argument('--timeout-seconds', type=int, default=2700)
    args = parser.parse_args()
    if args.timeout_seconds < 1:
        parser.error('timeout must be positive')
    run(args.dag, args.timeout_seconds)
