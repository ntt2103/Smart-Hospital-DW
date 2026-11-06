"""Repeatable local Metabase setup; no credentials or session tokens in artifacts."""
import json
import os
import urllib.error
import urllib.request
import time
from pathlib import Path


class Metabase:
    def __init__(self):
        self.url = os.environ.get('METABASE_URL', 'http://metabase:3000')
        self.session = None

    def request(self, method, path, body=None):
        headers = {'Content-Type': 'application/json'}
        if self.session:
            headers['X-Metabase-Session'] = self.session
        req = urllib.request.Request(self.url+'/api/'+path, data=json.dumps(body).encode() if body is not None else None,
                                     headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=120) as response:
                text = response.read()
                return json.loads(text) if text else None
        except urllib.error.HTTPError as exc:
            # Endpoint/schema errors are useful; never include request credentials.
            detail = exc.read().decode()[:1500]
            raise RuntimeError(f'Metabase {method} {path} HTTP{exc.code}: {detail}') from None

    def login(self):
        email = os.environ.get('METABASE_UI_EMAIL', 'admin@hospital.local')
        password = os.environ['METABASE_UI_PASSWORD']
        props = self.request('GET', 'session/properties')
        if props.get('setup-token'):
            self.request('POST', 'setup', {
                'token': props['setup-token'], 'prefs': {'site_name': 'Smart Hospital DW', 'allow_tracking': False},
                'user': {'first_name':'Hospital','last_name':'Admin','email':email,'password':password},
                'database': None,
            })
        self.session = self.request('POST', 'session', {'username':email,'password':password})['id']
        databases = self.request('GET', 'database').get('data', [])
        database = next((d for d in databases if d['name']=='Hospital Warehouse'), None)
        if database is None:
            database = self.request('POST', 'database', {'name':'Hospital Warehouse', 'engine':'postgres',
                'details':{'host':'postgres-dwh','port':5432,'dbname':'hospital_dwh',
                           'user':'hospital_reporter','password':os.environ['DWH_REPORTER_PASSWORD'],
                           'ssl':False, 'schema-filters-type':'inclusion','schema-filters-patterns':'mart'}})
        self.database_id = database['id']
        return self.database_id

    def query(self, query):
        return self.request('POST','dataset',{'database':self.database_id,'type':'native','native':{'query':query},'parameters':[]})


if __name__ == '__main__':
    client = Metabase()
    for attempt in range(90):
        try:
            if client.request('GET','health').get('status') == 'ok':
                break
        except (RuntimeError, urllib.error.URLError):
            pass
        time.sleep(2)
    else:
        raise RuntimeError('Metabase did not become ready within 180 seconds')
    db = client.login()
    result = client.query('SELECT ready FROM mart.foundation_health')
    assert result['data']['rows'] == [[1]], result.get('error')
    output = Path('/app/artifacts/phase2/metabase_health.json')
    output.write_text(json.dumps({'database_id':db,'read_only_query_passed':True,'rows':result['data']['rows']}, indent=2), encoding='utf-8')
    print('Metabase application DB and reporter datasource verified.')
