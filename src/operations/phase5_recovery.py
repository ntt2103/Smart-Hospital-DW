"""Full-row recovery verification for the isolated Phase 5 PostgreSQL stack.

This helper never drops, truncates or replaces a database. Restore targets have
unique names and are created only after an explicit isolation guard. Credentials
stay in the local environment; evidence contains counts, fingerprints and KPI
values, never source rows or authentication values.
"""
import argparse
import hashlib
import json
import os
import re
import time
import urllib.request
import uuid
from pathlib import Path

import psycopg
from psycopg import sql

from analytics.report_dates import report_dates
from operations.foundation import ROOT


DATABASES = {
    'oltp': ('oltp', 'hospital_oltp'),
    'dwh': ('dwh', 'hospital_dwh'),
    'airflow': ('meta', 'airflow_meta'),
    'metabase': ('meta', 'metabase_app'),
}


def connect(kind, database):
    return psycopg.connect(host=f'postgres-{kind}', dbname=database, user='postgres',
                           password=os.environ[f'{kind.upper()}_ADMIN_PASSWORD'],
                           options='-c timezone=UTC', connect_timeout=15)


def guard():
    if os.environ.get('COMPOSE_PROJECT_NAME') != 'smart_hospital_dw_phase5':
        raise ValueError('Recovery is restricted to the isolated Phase 5 project')


def target_name(label, run_id):
    if label not in DATABASES or not re.fullmatch(r'[a-z0-9]{10,24}', run_id):
        raise ValueError('Invalid isolated recovery identifier')
    return f'phase5_restore_{label}_{run_id}'


def atomic_json(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix+'.part')
    temp.write_text(json.dumps(value, indent=2, default=str), encoding='utf-8')
    temp.replace(path)


def row_digest(connection, schema, table):
    keys = connection.execute("""SELECT a.attname FROM pg_index i
        JOIN pg_attribute a ON a.attrelid=i.indrelid AND a.attnum=ANY(i.indkey)
        WHERE i.indrelid=%s::regclass AND i.indisprimary
        ORDER BY array_position(i.indkey::smallint[],a.attnum)""", (f'{schema}.{table}',)).fetchall()
    order = sql.SQL(',').join(sql.Identifier(key) for key, in keys) if keys else sql.SQL('md5(to_jsonb(t)::text)')
    query = sql.SQL('SELECT md5(to_jsonb(t)::text) FROM {}.{} t ORDER BY {}').format(
        sql.Identifier(schema), sql.Identifier(table), order)
    digest = hashlib.sha256()
    count = 0
    with connection.cursor(name='recovery_'+uuid.uuid4().hex) as cursor:
        cursor.itersize = 2000
        cursor.execute(query)
        for (value,) in cursor:
            digest.update(value.encode('ascii')+b'\n')
            count += 1
    return {'rows': count, 'ordered_row_md5_sha256': digest.hexdigest(),
            'order': [key for key, in keys] or ['row_md5']}


def schema_fingerprint(connection):
    # Hash definitions rather than copying possible application literals to logs.
    statements = {
        'columns': """SELECT n.nspname,c.relname,a.attnum,a.attname,
            format_type(a.atttypid,a.atttypmod),a.attnotnull,a.attidentity,a.attgenerated,
            md5(coalesce(pg_get_expr(d.adbin,d.adrelid),''))
            FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid
            JOIN pg_namespace n ON n.oid=c.relnamespace
            LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
            WHERE n.nspname !~ '^pg_' AND n.nspname<>'information_schema'
            AND c.relkind IN('r','p','v','m') AND a.attnum>0 AND NOT a.attisdropped
            ORDER BY 1,2,3""",
        'constraints': """SELECT n.nspname,c.relname,x.conname,x.contype,x.convalidated,
            md5(pg_get_constraintdef(x.oid,true))
            FROM pg_constraint x JOIN pg_class c ON c.oid=x.conrelid
            JOIN pg_namespace n ON n.oid=c.relnamespace
            WHERE n.nspname !~ '^pg_' AND n.nspname<>'information_schema' ORDER BY 1,2,3""",
        'indexes': """SELECT schemaname,tablename,indexname,md5(indexdef) FROM pg_indexes
            WHERE schemaname !~ '^pg_' AND schemaname<>'information_schema' ORDER BY 1,2,3""",
        'views': """SELECT n.nspname,c.relname,c.relkind,md5(pg_get_viewdef(c.oid,true))
            FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
            WHERE c.relkind IN('v','m') AND n.nspname !~ '^pg_'
            AND n.nspname<>'information_schema' ORDER BY 1,2""",
        'functions': """SELECT n.nspname,p.proname,pg_get_function_identity_arguments(p.oid),
            md5(pg_get_functiondef(p.oid)) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
            WHERE n.nspname !~ '^pg_' AND n.nspname<>'information_schema'
            AND p.prokind IN('f','p') AND NOT EXISTS(SELECT 1 FROM pg_depend d
              WHERE d.classid='pg_proc'::regclass AND d.objid=p.oid AND d.deptype='e')
            ORDER BY 1,2,3""",
        'triggers': """SELECT n.nspname,c.relname,t.tgname,md5(pg_get_triggerdef(t.oid,true))
            FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid
            JOIN pg_namespace n ON n.oid=c.relnamespace
            WHERE NOT t.tgisinternal AND n.nspname !~ '^pg_'
            AND n.nspname<>'information_schema' ORDER BY 1,2,3""",
        'extensions': 'SELECT extname,extversion FROM pg_extension ORDER BY extname',
        'owners': """SELECT 'schema',n.nspname,'',pg_get_userbyid(n.nspowner)
            FROM pg_namespace n WHERE n.nspname !~ '^pg_' AND n.nspname<>'information_schema'
            UNION ALL SELECT 'relation',n.nspname,c.relname,pg_get_userbyid(c.relowner)
            FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
            WHERE n.nspname !~ '^pg_' AND n.nspname<>'information_schema'
            UNION ALL SELECT 'function',n.nspname,p.proname||'('||pg_get_function_identity_arguments(p.oid)||')',pg_get_userbyid(p.proowner)
            FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
            WHERE n.nspname !~ '^pg_' AND n.nspname<>'information_schema'
            AND NOT EXISTS(SELECT 1 FROM pg_depend d WHERE d.classid='pg_proc'::regclass AND d.objid=p.oid AND d.deptype='e')
            UNION ALL SELECT 'type',n.nspname,t.typname,pg_get_userbyid(t.typowner)
            FROM pg_type t JOIN pg_namespace n ON n.oid=t.typnamespace
            WHERE t.typtype IN('e','d') AND n.nspname !~ '^pg_' AND n.nspname<>'information_schema'
            ORDER BY 1,2,3,4""",
        'schema_acl': """SELECT n.nspname,pg_get_userbyid(a.grantor),
            CASE WHEN a.grantee=0 THEN 'PUBLIC' ELSE pg_get_userbyid(a.grantee) END,
            a.privilege_type,a.is_grantable FROM pg_namespace n
            CROSS JOIN LATERAL aclexplode(coalesce(n.nspacl,acldefault('n',n.nspowner))) a
            WHERE n.nspname !~ '^pg_' AND n.nspname<>'information_schema' ORDER BY 1,2,3,4,5""",
        'relation_acl': """SELECT n.nspname,c.relname,pg_get_userbyid(a.grantor),
            CASE WHEN a.grantee=0 THEN 'PUBLIC' ELSE pg_get_userbyid(a.grantee) END,
            a.privilege_type,a.is_grantable FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
            CROSS JOIN LATERAL aclexplode(coalesce(c.relacl,acldefault(CASE WHEN c.relkind='S' THEN 'S'::"char" ELSE 'r'::"char" END,c.relowner))) a
            WHERE c.relkind IN('r','p','m','v','S','f') AND n.nspname !~ '^pg_' AND n.nspname<>'information_schema'
            ORDER BY 1,2,3,4,5,6""",
        'function_acl': """SELECT n.nspname,p.proname,pg_get_function_identity_arguments(p.oid),
            pg_get_userbyid(a.grantor),CASE WHEN a.grantee=0 THEN 'PUBLIC' ELSE pg_get_userbyid(a.grantee) END,
            a.privilege_type,a.is_grantable FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
            CROSS JOIN LATERAL aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) a
            WHERE n.nspname !~ '^pg_' AND n.nspname<>'information_schema'
            AND NOT EXISTS(SELECT 1 FROM pg_depend d WHERE d.classid='pg_proc'::regclass AND d.objid=p.oid AND d.deptype='e')
            ORDER BY 1,2,3,4,5,6,7""",
        'default_acl': """SELECT pg_get_userbyid(d.defaclrole),coalesce(n.nspname,''),d.defaclobjtype,
            pg_get_userbyid(a.grantor),CASE WHEN a.grantee=0 THEN 'PUBLIC' ELSE pg_get_userbyid(a.grantee) END,
            a.privilege_type,a.is_grantable FROM pg_default_acl d LEFT JOIN pg_namespace n ON n.oid=d.defaclnamespace
            CROSS JOIN LATERAL aclexplode(d.defaclacl) a ORDER BY 1,2,3,4,5,6,7""",
    }
    result = {}
    for name, query in statements.items():
        values = connection.execute(query).fetchall()
        result[name] = {'count':len(values), 'sha256':hashlib.sha256(
            json.dumps(values,default=str,separators=(',',':')).encode()).hexdigest()}
    return result


def compare_schema(expected, actual, archive_reference=None):
    """Check logical restore identity without treating deparser text as identity.

    A logical restore compacts dropped-column attnum holes and PostgreSQL may
    reparse an ARRAY cast into equivalent per-element casts. Never normalize SQL
    with string replacement: compare against a fresh schema-only restore of the
    exact same archive instead. Object counts, owners and every ACL must still
    match the original captured fingerprint directly.
    """
    differences = [key for key in expected if expected[key] != actual.get(key)]
    raw_matches = expected == actual
    reference_matches = archive_reference == actual if archive_reference is not None else None
    safe_categories = set(differences) <= {'columns', 'constraints', 'views'}
    counts_match = expected.keys() == actual.keys() and all(
        expected[key]['count'] == actual[key]['count'] for key in expected)
    reference_accepted = reference_matches is True and safe_categories and counts_match
    return {'matches': raw_matches or reference_accepted,
            'raw_captured_schema_matches': raw_matches,
            'archive_schema_reference_matches': reference_matches,
            'differing_raw_categories': differences,
            'original_object_counts_match': counts_match,
            'method': 'captured schema fingerprint' if raw_matches else
                ('complete schema-only restore from the same hashed archive; original owners/ACL/object counts checked directly'
                 if reference_accepted else 'unresolved schema difference')}


def schema_references(folder, run_id, reference_run_id):
    """Bind optional deparser references to the retained immutable dump hashes."""
    target_name('oltp', reference_run_id)
    provenance = json.loads((folder/'schema_references.json').read_text(encoding='utf-8-sig'))
    if provenance['source_run_id'] != run_id or provenance['reference_run_id'] != reference_run_id:
        raise ValueError('Schema reference provenance does not match this recovery run')
    result = {}
    for label,(kind,_) in DATABASES.items():
        archive = folder/f'{label}.dump'
        with archive.open('rb') as stream:
            digest = hashlib.file_digest(stream, 'sha256').hexdigest()
        if digest != provenance['archives'][label]['sha256']:
            raise ValueError('Schema reference archive hash changed')
        with connect(kind,target_name(label,reference_run_id)) as connection:
            result[label] = schema_fingerprint(connection)
    return result, provenance


def database_policy(connection, database):
    row=connection.execute("""SELECT pg_get_userbyid(datdba),datconnlimit,datallowconn,
        pg_encoding_to_char(encoding),datcollate,datctype,datlocprovider,daticulocale
        FROM pg_database WHERE datname=%s""",(database,)).fetchone()
    if row is None: raise ValueError('Expected recovery source database')
    acl=connection.execute("""SELECT pg_get_userbyid(a.grantor),
        CASE WHEN a.grantee=0 THEN 'PUBLIC' ELSE pg_get_userbyid(a.grantee) END,
        a.privilege_type,a.is_grantable FROM pg_database d
        CROSS JOIN LATERAL aclexplode(coalesce(d.datacl,acldefault('d',d.datdba))) a
        WHERE d.datname=%s ORDER BY 1,2,3,4""",(database,)).fetchall()
    return {'owner':row[0],'connection_limit':row[1],'allow_connections':row[2],
            'encoding':row[3],'collation':row[4],'ctype':row[5],
            'locale_provider':row[6],'icu_locale':row[7],'acl':[list(item) for item in acl]}


def clone_database_policy(connection, target, policy):
    """Apply captured database grants ONLY to a fresh, guarded trial target."""
    if not re.fullmatch(r'phase5_restore_(?:oltp|dwh|airflow|metabase)_[a-z0-9]{10,24}',target):
        raise ValueError('Database privilege target is outside the private restore namespace')
    owner=policy['owner']
    connection.execute(sql.SQL('REVOKE ALL PRIVILEGES ON DATABASE {} FROM PUBLIC,{}').format(
        sql.Identifier(target),sql.Identifier(owner)))
    grants=sorted(policy['acl'],key=lambda item:(item[0]!=owner,item[0],item[1],item[2]))
    for grantor,grantee,privilege,grantable in grants:
        if privilege not in {'CREATE','CONNECT','TEMPORARY'}:
            raise ValueError('Unexpected database privilege')
        connection.execute(sql.SQL('SET ROLE {}').format(sql.Identifier(grantor)))
        try:
            recipient=sql.SQL('PUBLIC') if grantee=='PUBLIC' else sql.Identifier(grantee)
            connection.execute(sql.SQL('GRANT {} ON DATABASE {} TO {}{}').format(
                sql.SQL(privilege),sql.Identifier(target),recipient,
                sql.SQL(' WITH GRANT OPTION') if grantable else sql.SQL('')))
        finally:
            connection.execute('RESET ROLE')


def kpis(connection, manifest):
    start, end = report_dates(manifest)
    expressions = {
        'OP01': ('outpatient_daily','sum(visit_count)'),
        'OP02': ('outpatient_daily','sum(waiting_minutes_sum)/nullif(sum(waiting_valid_count),0)'),
        'OP03': ('appointment_daily','100.0*sum(no_show_count)/nullif(sum(classified_count),0)'),
        'IP01': ('inpatient_admission_daily','sum(admission_count)'),
        'IP03': ('inpatient_discharge_daily','sum(los_days_sum)/nullif(sum(los_valid_count),0)'),
        'LAB01': ('lab_daily','sum(completed_orders)'),
        'LAB02': ('lab_daily','sum(tat_minutes_sum)/nullif(sum(tat_valid_count),0)'),
        'LAB03': ('lab_daily','100.0*sum(ontime_orders)/nullif(sum(ontime_valid_count),0)'),
    }
    result = {}
    for key,(view,expression) in expressions.items():
        value = connection.execute(sql.SQL('SELECT {} FROM mart.{} WHERE report_date BETWEEN %s AND %s').format(
            sql.SQL(expression),sql.Identifier(view)),(start,end)).fetchone()[0]
        result[key] = None if value is None else str(value)
    occupancy = connection.execute("""SELECT CASE WHEN sum(invalid_snapshot_count)=0 THEN
        100.0*sum(occupied_beds)/nullif(sum(active_capacity),0) END
        FROM mart.bed_occupancy_daily WHERE report_date=%s""",(end,)).fetchone()[0]
    result['IP02'] = None if occupancy is None else str(occupancy)
    as_of = connection.execute('SELECT max(as_of) FROM mart.realtime_department_snapshot').fetchone()[0]
    for key,function in [('RT01','rt01_checkins_last_15m'),('RT02','rt02_lab_results_last_15m')]:
        if as_of is None:
            result[key] = None
        else:
            values = connection.execute(sql.SQL('SELECT department_id,value FROM mart.{}(%s) ORDER BY department_id').format(
                sql.Identifier(function)),(as_of,)).fetchall()
            result[key] = [(department, str(value)) for department,value in values]
    result['rt_as_of'] = None if as_of is None else as_of.isoformat()
    result['historical_start'] = start
    result['historical_end'] = end
    return result


def capture(label, database, manifest):
    kind,_ = DATABASES[label]
    started = time.monotonic()
    with connect(kind,database) as connection:
        connection.execute('SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY')
        relations = connection.execute("""SELECT n.nspname,c.relname FROM pg_class c
            JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.relkind IN('r','m')
            AND n.nspname !~ '^pg_' AND n.nspname<>'information_schema' ORDER BY 1,2""").fetchall()
        tables = {}
        for schema,table in relations:
            tables[f'{schema}.{table}'] = row_digest(connection,schema,table)
        sequences = {}
        for schema,name in connection.execute("""SELECT n.nspname,c.relname FROM pg_class c
            JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.relkind='S'
            AND n.nspname !~ '^pg_' AND n.nspname<>'information_schema' ORDER BY 1,2"""):
            state = connection.execute(sql.SQL('SELECT last_value,is_called FROM {}.{}').format(
                sql.Identifier(schema),sql.Identifier(name))).fetchone()
            sequences[f'{schema}.{name}'] = list(state)
        result = {'database': database, 'tables': tables, 'sequences':sequences, 'schema':schema_fingerprint(connection),
                  'database_policy':database_policy(connection,database),
                  'database_bytes':connection.execute('SELECT pg_database_size(current_database())').fetchone()[0]}
        if label == 'dwh':
            result['kpis'] = kpis(connection,manifest)
        result['elapsed_seconds'] = time.monotonic()-started
        return result


def create_targets(run_id):
    result = {}
    for label,(kind,source_database) in DATABASES.items():
        database = target_name(label,run_id)
        with connect(kind,'postgres') as connection:
            connection.autocommit = True
            if connection.execute('SELECT 1 FROM pg_database WHERE datname=%s',(database,)).fetchone():
                raise ValueError('Recovery target already exists; choose a fresh run ID; no database was dropped')
            policy=database_policy(connection,source_database)
            if not policy['allow_connections']: raise ValueError('Source database disallows connections')
            connection.execute(sql.SQL('CREATE DATABASE {} OWNER {} CONNECTION LIMIT {}').format(
                sql.Identifier(database),sql.Identifier(policy['owner']),sql.Literal(policy['connection_limit'])))
            with connection.transaction():
                clone_database_policy(connection,database,policy)
            if database_policy(connection,database)!=policy:
                raise ValueError('New restore target database owner/ACL/locale differs from source')
        result[label] = database
    return result


def check_quiet():
    with connect('meta','airflow_meta') as connection:
        active = connection.execute("SELECT count(*) FROM dag_run WHERE state IN('queued','running')").fetchone()[0]
    if active:
        raise ValueError('Drain active/queued DAGs before recovery capture')
    return {'active_or_queued_dag_runs':active}


def restored_permissions(run_id, before):
    """Authenticate real app roles against restored targets; never write rows."""
    checks={}
    def role_connection(kind,label,user,key):
        return psycopg.connect(host=f'postgres-{kind}',dbname=target_name(label,run_id),user=user,
            password=os.environ[key],options='-c timezone=UTC',connect_timeout=15)
    with role_connection('oltp','oltp','hospital_reader','OLTP_READER_PASSWORD') as connection:
        count=connection.execute('SELECT count(*) FROM public.encounters').fetchone()[0]
        expected=before['databases']['oltp']['tables']['public.encounters']['rows']
        assert count==expected
        try:
            with connection.transaction(force_rollback=True):
                connection.execute('EXPLAIN UPDATE public.patients SET is_active=false')
        except psycopg.errors.InsufficientPrivilege:
            denied=True
        else: raise ValueError('Restored source reader unexpectedly has UPDATE privilege')
        checks['hospital_reader']={'authenticated':True,'encounter_rows':count,'update_denied':denied}
    with role_connection('oltp','oltp','hospital_writer','OLTP_WRITER_PASSWORD') as connection:
        privileges=connection.execute("SELECT has_table_privilege('public.appointments','INSERT'),has_table_privilege('public.appointments','UPDATE'),has_sequence_privilege(pg_get_serial_sequence('public.appointments','appointment_id'),'USAGE')").fetchone()
        assert all(privileges)
        checks['hospital_writer']={'authenticated':True,'insert_update_sequence_permissions':True,'rows_written':0}
    with role_connection('dwh','dwh','hospital_reporter','DWH_REPORTER_PASSWORD') as connection:
        departments=connection.execute('SELECT count(*) FROM mart.departments').fetchone()[0]
        visits=connection.execute('SELECT sum(visit_count) FROM mart.outpatient_daily WHERE report_date BETWEEN %s AND %s',
            (before['databases']['dwh']['kpis']['historical_start'],before['databases']['dwh']['kpis']['historical_end'])).fetchone()[0]
        assert str(visits)==before['databases']['dwh']['kpis']['OP01']
        try:
            with connection.transaction(force_rollback=True):
                connection.execute('SELECT count(*) FROM raw.source_rows')
        except psycopg.errors.InsufficientPrivilege:
            raw_denied=True
        else: raise ValueError('Restored reporter unexpectedly has broad raw access')
        checks['hospital_reporter']={'authenticated':True,'mart_departments':departments,'historical_op01':str(visits),'raw_denied':raw_denied}
    with role_connection('dwh','dwh','hospital_etl','DWH_ETL_PASSWORD') as connection:
        privileges=connection.execute("SELECT has_table_privilege('stg.encounters','INSERT'),has_table_privilege('dw.fact_encounter','UPDATE'),has_database_privilege(current_database(),'TEMPORARY')").fetchone()
        assert all(privileges)
        checks['hospital_etl']={'authenticated':True,'staging_fact_temporary_permissions':True,'rows_written':0}
    with role_connection('dwh','dwh','hospital_consumer','DWH_CONSUMER_PASSWORD') as connection:
        privileges=connection.execute("SELECT has_table_privilege('raw.events','SELECT'),has_table_privilege('raw.events','INSERT'),has_table_privilege('raw.event_deliveries','INSERT')").fetchone()
        assert all(privileges)
        checks['hospital_consumer']={'authenticated':True,'canonical_delivery_permissions':True,'rows_written':0}
    for label,user,key,table in (
        ('airflow','airflow_meta_owner','AIRFLOW_META_PASSWORD','ab_user'),
        ('metabase','metabase_meta_owner','METABASE_META_PASSWORD','report_dashboard')):
        with role_connection('meta',label,user,key) as connection:
            count=connection.execute(sql.SQL('SELECT count(*) FROM public.{}').format(sql.Identifier(table))).fetchone()[0]
            assert count==before['databases'][label]['tables'][f'public.{table}']['rows']
            checks[user]={'authenticated':True,'application_metadata_read':True,'rows':count}
    return {'passed':True,'roles':checks,'proof_scope':'Original object ownership/ACL and database grants restored; real app-role authentication/read/denial checks on new databases only'}


def http_health(service, status, payload, requested):
    """HTTP 200 alone is insufficient when an Airflow worker is unhealthy."""
    if service != 'airflow-api':
        expected = 'running' if service == 'booking' else 'ok'
        return {service: {'ready': status == 200 and payload.get('status') == expected,
                          'scope': 'HTTP health endpoint status and JSON status'}}
    components = [('airflow-api', 'metadatabase')]
    components += [(worker, component) for worker, component in (
        ('airflow-scheduler', 'scheduler'), ('airflow-dag-processor', 'dag_processor'))
        if worker in requested]
    return {name: {'ready': status == 200 and (payload.get(component) or {}).get('status') == 'healthy',
                   'component': component,
                   'component_status': (payload.get(component) or {}).get('status'),
                   'scope': 'Airflow monitor health component; undeployed triggerer is not required'}
            for name, component in components}


def health_request(service, url):
    # The booking app intentionally accepts only local Host values. Connect by
    # Docker DNS while using its allowed local Host; keep the app guard intact.
    headers = {'Host':'localhost:8080'} if service == 'booking' else {}
    return urllib.request.Request(url,headers=headers)


def health(services):
    requested = set(services.split(',')) if services else set()
    if not requested <= {'airflow-scheduler','airflow-dag-processor','consumer','booking','airflow-api','metabase','kafka'}:
        raise ValueError('Invalid isolated service health scope')
    endpoints = {'booking':'http://booking:8080/health',
                 'metabase':'http://metabase:3000/api/health',
                 'airflow-api':'http://airflow-api:8080/api/v2/monitor/health'}
    report = {'services':{},'databases':{},'passed':False}
    if 'airflow-api' not in requested:
        for worker in ('airflow-scheduler','airflow-dag-processor'):
            if worker in requested:
                report['services'][worker]={'ready':False,
                    'scope':'Airflow API was not running before maintenance; worker health could not be verified'}
    if 'consumer' in requested and 'kafka' not in requested:
        report['services']['consumer']={'ready':False,'scope':'Kafka was not running before maintenance; offset lag cannot be verified'}
    deadline = time.monotonic()+120
    while time.monotonic()<deadline:
        complete = True
        for service,url in endpoints.items():
            if service not in requested:
                continue
            try:
                with urllib.request.urlopen(health_request(service,url),timeout=3) as response:
                    states = http_health(service,response.status,json.load(response),requested)
                report['services'].update(states)
                complete = complete and all(state['ready'] for state in states.values())
            except Exception as error:
                report['services'][service]={'ready':False,'error_type':type(error).__name__}
                complete = False
        if 'kafka' in requested:
            try:
                from confluent_kafka import Consumer,TopicPartition
                consumer=Consumer({'bootstrap.servers':'kafka:9092','group.id':'hospital-raw-v1',
                                   'enable.auto.commit':False,'enable.auto.offset.store':False})
                partitions=[]
                try:
                    for topic in ('appointments.events','encounters.events','laboratory.events'):
                        for partition in range(3):
                            tp=TopicPartition(topic,partition)
                            _,end=consumer.get_watermark_offsets(tp,timeout=3)
                            committed=consumer.committed([tp],timeout=3)[0].offset
                            partitions.append({'topic':topic,'partition':partition,
                                'high_watermark':end,'committed_offset':committed,
                                'lag':end-max(committed,0)})
                finally:
                    consumer.close()
                lag=sum(p['lag'] for p in partitions)
                report['services']['kafka']={'ready':True,'partitions':partitions}
                if 'consumer' in requested:
                    report['services']['consumer']={'ready':lag==0,'total_lag':lag,
                        'scope':'read-only watermark/committed-offset comparison; no group subscription or offset writes'}
                    complete=complete and lag==0
            except Exception as error:
                report['services']['kafka']={'ready':False,'error_type':type(error).__name__}
                complete=False
        if complete:
            break
        time.sleep(2)
    for label,(kind,database) in DATABASES.items():
        with connect(kind,database) as connection:
            report['databases'][label]={'ready':connection.execute('SELECT 1').fetchone()[0]==1}
    report['passed']=all(value.get('ready') for value in report['services'].values()) and all(value['ready'] for value in report['databases'].values())
    atomic_json(ROOT/'artifacts/phase5/recovery_health.json',report)
    if not report['passed']:
        raise ValueError('Restored application running states require health/lag attention')
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action',choices=['check-quiet','capture','create-targets','verify','health'])
    parser.add_argument('--run-id',required=True)
    parser.add_argument('--manifest',type=Path,default=ROOT/'artifacts/generated/phase5_6000_180d/manifest.json')
    parser.add_argument('--services',default='')
    parser.add_argument('--schema-reference-run-id',default='')
    args = parser.parse_args()
    guard()
    target_name('oltp',args.run_id)
    folder = ROOT/'artifacts/generated/phase5_recovery'/args.run_id
    folder.mkdir(parents=True,exist_ok=True)
    begun = time.monotonic()
    if args.action == 'health':
        report=health(args.services)
    elif args.action == 'check-quiet':
        report = check_quiet()
    elif args.action == 'create-targets':
        report = {'targets':create_targets(args.run_id)}
        atomic_json(folder/'targets.json',report)
    else:
        manifest = json.loads(args.manifest.read_text(encoding='utf-8'))
        if args.action == 'capture':
            report = {'base_dataset_sha256':manifest['business_sha256'], 'databases':{
                label:capture(label,database,manifest) for label,(_,database) in DATABASES.items()}}
            atomic_json(folder/'baseline.json',report)
        else:
            before = json.loads((folder/'baseline.json').read_text(encoding='utf-8'))
            references,provenance = schema_references(folder,args.run_id,args.schema_reference_run_id) if args.schema_reference_run_id else ({},None)
            report = {'base_dataset_sha256':manifest['business_sha256'], 'passed':True,
                      'proof_scope':'full logical archives restored into new isolated databases; full-row digests and schema/KPI parity',
                      'test_restore_policy':'Full original object ownership/ACL; database owner/grants cloned only to guarded new target names; no --create',
                      'databases':{}}
            if provenance is not None:
                report['schema_reference_provenance'] = provenance
            for label in DATABASES:
                actual = capture(label,target_name(label,args.run_id),manifest)
                expected = before['databases'][label]
                comparisons = {key:json.dumps(actual[key],sort_keys=True,default=str)==json.dumps(expected[key],sort_keys=True,default=str)
                               for key in ('tables','schema','sequences','database_policy')}
                schema_check = compare_schema(expected['schema'],actual['schema'],references.get(label))
                comparisons['schema'] = schema_check['matches']
                if label == 'dwh':
                    comparisons['all_11_kpis'] = json.dumps(actual['kpis'],sort_keys=True,default=str)==json.dumps(expected['kpis'],sort_keys=True,default=str)
                report['databases'][label] = {'source_database':expected['database'], 'restored_database':actual['database'],
                    'comparisons':comparisons,'table_count':len(actual['tables']),
                    'restored_row_count':sum(t['rows'] for t in actual['tables'].values()),
                    'source_database_bytes':expected['database_bytes'], 'restored_database_bytes':actual['database_bytes'],
                    'capture_seconds':actual['elapsed_seconds'], 'tables':actual['tables'],
                    'schema_verification':schema_check}
                if label=='dwh': report['databases'][label]['kpis']=actual['kpis']
                report['passed'] = report['passed'] and all(comparisons.values())
            if report['passed']:
                report['application_permissions']=restored_permissions(args.run_id,before)
            report['elapsed_seconds'] = time.monotonic()-begun
            atomic_json(ROOT/'artifacts/phase5/recovery_verification.json',report)
            if not report['passed']:
                raise ValueError('Recovery parity failed; retain archives and inspect comparison evidence')
    print(json.dumps({'action':args.action,'completed':True,'elapsed_seconds':round(time.monotonic()-begun,3),
                      'passed':report.get('passed'),'run_id':args.run_id}))


if __name__=='__main__':
    try:
        main()
    except Exception as error:
        print(json.dumps({'completed':False,'error_type':type(error).__name__,
                          'scope':'isolated recovery; existing source databases preserved'}))
        raise SystemExit(1) from None
