"""Prove full ownership/ACL restore on the small dedicated ETL fixture."""
import argparse
import json
import os
from pathlib import Path

import psycopg
from psycopg import sql

from operations.phase5_recovery import (ROOT,atomic_json,capture,clone_database_policy,
    connect,database_policy,guard,target_name)

SOURCE='hospital_phase5_etl_permissions'


def run(stage, run_id):
    guard()
    target=target_name('dwh',run_id)
    folder=ROOT/'artifacts/generated/phase5_recovery_acl_probe'/run_id
    folder.mkdir(parents=True,exist_ok=True)
    manifest=json.loads((ROOT/'artifacts/generated/phase5_6000_180d/manifest.json').read_text())
    if stage=='create':
        baseline=capture('dwh',SOURCE,manifest)
        with connect('dwh','postgres') as connection:
            connection.autocommit=True
            if connection.execute('SELECT 1 FROM pg_database WHERE datname=%s',(target,)).fetchone():
                raise ValueError('Probe target already exists; use a new run ID')
            policy=database_policy(connection,SOURCE)
            connection.execute(sql.SQL('CREATE DATABASE {} OWNER {} CONNECTION LIMIT {}').format(
                sql.Identifier(target),sql.Identifier(policy['owner']),sql.Literal(policy['connection_limit'])))
            with connection.transaction(): clone_database_policy(connection,target,policy)
        atomic_json(folder/'baseline.json',{'target':target,'capture':baseline})
        print(json.dumps({'stage':stage,'target':target,'completed':True}))
        return
    before=json.loads((folder/'baseline.json').read_text())
    actual=capture('dwh',target,manifest)
    comparisons={key:json.dumps(actual[key],sort_keys=True,default=str)==json.dumps(before['capture'][key],sort_keys=True,default=str)
        for key in ('tables','schema','sequences','database_policy','kpis')}
    with psycopg.connect(host='postgres-dwh',dbname=target,user='hospital_etl',
        password=os.environ['DWH_ETL_PASSWORD'],options='-c timezone=UTC') as connection:
        rows=connection.execute('SELECT count(*) FROM stg.encounters').fetchone()[0]
        assert rows==actual['tables']['stg.encounters']['rows']
    report={'scope':'small dedicated fixture only; full original ownership and ACL restored',
        'original_fixture':SOURCE,'target_database':target,'comparisons':comparisons,
        'real_hospital_etl_login_and_read':True,'rows':sum(t['rows'] for t in actual['tables'].values()),
        'database_level_commands_in_archive':0,'passed':all(comparisons.values())}
    atomic_json(ROOT/'artifacts/phase5/recovery_acl_probe.json',report)
    assert report['passed'],comparisons
    print(json.dumps(report))


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('stage',choices=['create','verify'])
    parser.add_argument('--run-id',required=True)
    args=parser.parse_args();run(args.stage,args.run_id)
