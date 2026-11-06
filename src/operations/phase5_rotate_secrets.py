"""Rotate credentials only in the isolated Phase 5 stack during maintenance.

Run prepare/roles/airflow-ui/promote in that order. The generated old/new env
files stay under ignored artifacts/generated for recovery from partial rotation.
No credential value is printed or copied into evidence.
"""
import argparse
import json
import os
import re
import secrets
from datetime import datetime, timezone
from pathlib import Path

import psycopg
from psycopg import sql
from operations.foundation import ROOT

KEYS = (
    'OLTP_ADMIN_PASSWORD', 'DWH_ADMIN_PASSWORD', 'META_ADMIN_PASSWORD',
    'OLTP_WRITER_PASSWORD', 'OLTP_READER_PASSWORD', 'DWH_ETL_PASSWORD',
    'DWH_CONSUMER_PASSWORD', 'DWH_REPORTER_PASSWORD', 'AIRFLOW_META_PASSWORD',
    'METABASE_META_PASSWORD', 'AIRFLOW_API_SECRET', 'AIRFLOW_JWT_SECRET',
    'AIRFLOW_UI_PASSWORD', 'METABASE_UI_PASSWORD',
)
ROLES = {
    'oltp': [('postgres','OLTP_ADMIN_PASSWORD','hospital_oltp'),
             ('hospital_writer','OLTP_WRITER_PASSWORD','hospital_oltp'),
             ('hospital_reader','OLTP_READER_PASSWORD','hospital_oltp')],
    'dwh': [('postgres','DWH_ADMIN_PASSWORD','hospital_dwh'),
            ('hospital_etl','DWH_ETL_PASSWORD','hospital_dwh'),
            ('hospital_consumer','DWH_CONSUMER_PASSWORD','hospital_dwh'),
            ('hospital_reporter','DWH_REPORTER_PASSWORD','hospital_dwh')],
    'meta': [('postgres','META_ADMIN_PASSWORD','postgres'),
             ('airflow_meta_owner','AIRFLOW_META_PASSWORD','airflow_meta'),
             ('metabase_meta_owner','METABASE_META_PASSWORD','metabase_app')],
}


def read_env(path):
    values = {}
    for line in path.read_text(encoding='utf-8-sig').splitlines():
        if line and not line.lstrip().startswith('#') and '=' in line:
            key, value = line.split('=',1)
            values[key] = value.strip().strip('"').strip("'")
    if values.get('COMPOSE_PROJECT_NAME') != 'smart_hospital_dw_phase5':
        raise ValueError('Credential rotation is restricted to the isolated Phase 5 project')
    if any(key not in values for key in KEYS):
        raise ValueError('Required isolated secret keys are missing')
    return values


def write_state(directory, state):
    path = directory/'state.json'
    temp = path.with_suffix('.json.part')
    temp.write_text(json.dumps(state,indent=2),encoding='utf-8')
    temp.replace(path)
    evidence = ROOT/'artifacts/phase5/secret_rotation.json'
    evidence.parent.mkdir(parents=True,exist_ok=True)
    evidence.write_text(json.dumps(state,indent=2),encoding='utf-8')


def prepare(directory, runtime_env):
    directory.mkdir(parents=True,exist_ok=True)
    if (directory/'new.env').exists():
        read_env(directory/'new.env')
        return
    current = read_env(runtime_env)
    text = runtime_env.read_text(encoding='utf-8-sig')
    (directory/'old.env').write_text(text,encoding='utf-8')
    for key in KEYS:
        replacement = key+'='+secrets.token_hex(32)
        text, count = re.subn(r'^'+re.escape(key)+r'=.*$',replacement,text,flags=re.MULTILINE)
        if count != 1:
            raise ValueError('Secret configuration keys must appear exactly once')
    (directory/'new.env').write_text(text,encoding='utf-8')
    generated = read_env(directory/'new.env')
    assert all(re.fullmatch('[0-9a-f]{64}',generated[key]) for key in KEYS)
    assert len({generated[key] for key in KEYS}) == len(KEYS)
    assert all(generated[key] != current[key] for key in KEYS)
    write_state(directory,{'project':'smart_hospital_dw_phase5','prepared_at':datetime.now(timezone.utc).isoformat(),
                           'secret_keys':list(KEYS),'rotated_clusters':[],'airflow_ui_rotated':False,
                           'env_promoted':False,'original_demo_env_modified':False,
                           'credential_values_recorded':False})


def connection(kind, values, user='postgres', key=None, database=None):
    key = key or kind.upper()+'_ADMIN_PASSWORD'
    database = database or ('postgres' if kind=='meta' else 'hospital_'+kind)
    return psycopg.connect(host='postgres-'+kind,dbname=database,user=user,password=values[key],
                           connect_timeout=15,options='-c timezone=UTC')


def rotate_roles(directory):
    old,new = read_env(directory/'old.env'),read_env(directory/'new.env')
    state = json.loads((directory/'state.json').read_text(encoding='utf-8'))
    for kind,roles in ROLES.items():
        # A completed cluster can be resumed with its new admin credential.
        values = new if kind in state['rotated_clusters'] else old
        try:
            conn = connection(kind,values)
        except psycopg.OperationalError:
            conn = connection(kind,new)
        with conn:
            for username,key,database in roles:
                conn.execute(sql.SQL('ALTER ROLE {} PASSWORD {}').format(sql.Identifier(username),sql.Literal(new[key])))
        for username,key,database in roles:
            with connection(kind,new,user=username,key=key,database=database) as verified:
                assert verified.execute('SELECT 1').fetchone()[0] == 1
        if kind not in state['rotated_clusters']:
            state['rotated_clusters'].append(kind)
        state['role_authentication_verified'] = True
        write_state(directory,state)


def rotate_airflow_ui(directory):
    # The Airflow image provides its own installed password hashing library.
    from werkzeug.security import generate_password_hash, check_password_hash
    new = read_env(directory/'new.env')
    password_hash = generate_password_hash(new['AIRFLOW_UI_PASSWORD'])
    assert check_password_hash(password_hash,new['AIRFLOW_UI_PASSWORD'])
    with connection('meta',new,database='airflow_meta') as conn:
        changed = conn.execute('UPDATE ab_user SET password=%s WHERE username=%s',(password_hash,'admin')).rowcount
        if changed != 1:
            raise ValueError('Expected one isolated Airflow admin account')
    state = json.loads((directory/'state.json').read_text(encoding='utf-8'))
    state['airflow_ui_rotated'] = True
    write_state(directory,state)


def promote(directory,runtime_env):
    state = json.loads((directory/'state.json').read_text(encoding='utf-8'))
    if set(state['rotated_clusters']) != set(ROLES) or not state['airflow_ui_rotated']:
        raise ValueError('Verify all roles and the Airflow account before promoting credentials')
    new = read_env(directory/'new.env')
    for kind,roles in ROLES.items():
        for username,key,database in roles:
            with connection(kind,new,user=username,key=key,database=database) as verified:
                assert verified.execute('SELECT 1').fetchone()[0] == 1
    temp = runtime_env.with_suffix('.env.rotation.part')
    temp.write_text((directory/'new.env').read_text(encoding='utf-8'),encoding='utf-8')
    os.replace(temp,runtime_env)
    state['env_promoted'] = True
    state['completed_at'] = datetime.now(timezone.utc).isoformat()
    write_state(directory,state)


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action',choices=['prepare','roles','airflow-ui','promote'])
    parser.add_argument('--runtime-env',type=Path,default=ROOT/'.env')
    parser.add_argument('--directory',type=Path,default=ROOT/'artifacts/generated/phase5_secret_rotation')
    args=parser.parse_args()
    try:
        if args.action=='prepare': prepare(args.directory,args.runtime_env)
        elif args.action=='roles': rotate_roles(args.directory)
        elif args.action=='airflow-ui': rotate_airflow_ui(args.directory)
        else: promote(args.directory,args.runtime_env)
    except Exception as exc:
        # Database exceptions can contain connection details; report only type.
        print(json.dumps({'action':args.action,'completed':False,'error_type':type(exc).__name__}))
        raise SystemExit(1) from None
    print(json.dumps({'action':args.action,'completed':True,'credential_values_recorded':False}))
