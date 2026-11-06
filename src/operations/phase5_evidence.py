"""Capture restoration/migration evidence without exporting credentials or rows."""
import argparse
import hashlib
import json
from operations.foundation import ROOT, connect


def capture():
    report={}
    for kind in ('oltp','dwh'):
        with connect(kind,'admin') as connection:
            schema='public' if kind=='oltp' else 'dw'
            tables=connection.execute("SELECT tablename FROM pg_tables WHERE schemaname=%s ORDER BY tablename",(schema,)).fetchall()
            from psycopg import sql
            counts={name:connection.execute(sql.SQL('SELECT count(*) FROM {}.{}').format(sql.Identifier(schema),sql.Identifier(name))).fetchone()[0] for name, in tables}
            report[kind]={'clock':str(connection.execute('SELECT clock_timestamp()').fetchone()[0]),'counts':counts,
                          'database_bytes':connection.execute('SELECT pg_database_size(current_database())').fetchone()[0],
                          'migrations':connection.execute('SELECT name,sha256 FROM _deploy.migrations ORDER BY name').fetchall()}
            if kind=='dwh':
                values={}
                for name, in connection.execute("SELECT table_name FROM information_schema.views WHERE table_schema='mart' AND table_name LIKE '%daily' ORDER BY table_name"):
                    rows=connection.execute(sql.SQL('SELECT to_jsonb(t) FROM mart.{} t ORDER BY to_jsonb(t)::text').format(sql.Identifier(name))).fetchall()
                    values[name]={'rows':len(rows),'sha256':hashlib.sha256(json.dumps(rows,sort_keys=True,default=str).encode()).hexdigest()}
                report['historical_mart_digests']=values
                report['physical_inventory']=connection.execute("SELECT CASE WHEN relname LIKE 'fact_%' THEN 'facts' WHEN relname LIKE 'dim_%' THEN 'dimensions' ELSE 'bridges' END,count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='dw' AND c.relkind='r' AND (relname LIKE 'fact_%' OR relname LIKE 'dim_%' OR relname LIKE 'bridge_%') GROUP BY 1 ORDER BY 1").fetchall()
    return report


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',required=True)
    parser.add_argument('--compare')
    args=parser.parse_args()
    result=capture()
    if args.compare:
        prior=json.loads((ROOT/args.compare).read_text(encoding='utf-8'))
        result['historical_mart_parity']=result['historical_mart_digests']==prior['historical_mart_digests']
        assert result['historical_mart_parity'],'Historical mart values changed during migration'
    output=ROOT/args.output
    output.parent.mkdir(parents=True,exist_ok=True)
    output.write_text(json.dumps(result,indent=2,default=str),encoding='utf-8')
    print(json.dumps({'evidence':str(output),'inventory':result['physical_inventory'],'parity':result.get('historical_mart_parity')}))
