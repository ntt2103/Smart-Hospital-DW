"""Trigger and observe a real LocalExecutor DAG run through Airflow3 API."""
import argparse
import json
import os
import time
import uuid
import urllib.request
import urllib.error
from datetime import datetime,timezone
from operations.foundation import ROOT


class Airflow:
    def __init__(self):
        self.url='http://airflow-api:8080'
        self.token=None
        self.token=self.request('POST','/auth/token',{'username':'admin','password':os.environ['AIRFLOW_UI_PASSWORD']})['access_token']

    def request(self,method,path,body=None):
        headers={'Content-Type':'application/json'}
        if self.token: headers['Authorization']='Bearer '+self.token
        req=urllib.request.Request(self.url+path,method=method,headers=headers,data=json.dumps(body).encode() if body is not None else None)
        try:
            with urllib.request.urlopen(req,timeout=60) as res: return json.loads(res.read())
        except urllib.error.HTTPError as exc:
            raise RuntimeError(f'Airflow {method} {path}: HTTP{exc.code} {exc.read().decode()[:1000]}') from None

    def trigger(self,dag_id,conf=None):
        for attempt in range(45):
            try:
                self.request('PATCH',f'/api/v2/dags/{dag_id}',{'is_paused':False});break
            except RuntimeError as exc:
                if 'HTTP404' not in str(exc):raise
                time.sleep(2)
        else:raise RuntimeError(f'DAG not discovered: {dag_id}')
        run_id='acceptance_'+uuid.uuid4().hex
        data=self.request('POST',f'/api/v2/dags/{dag_id}/dagRuns',{'dag_run_id':run_id,'logical_date':datetime.now(timezone.utc).isoformat(),'conf':conf or {}})
        for _ in range(120):
            data=self.request('GET',f'/api/v2/dags/{dag_id}/dagRuns/{run_id}')
            if data['state'] in ['success','failed']:
                tasks=self.request('GET',f'/api/v2/dags/{dag_id}/dagRuns/{run_id}/taskInstances')
                result={'dag_id':dag_id,'run_id':run_id,'state':data['state'],'conf':conf,'tasks':tasks}
                phase=4 if dag_id=='hospital_near_rt' else 3
                (ROOT/f'artifacts/phase{phase}').mkdir(parents=True,exist_ok=True)
                (ROOT/f'artifacts/phase{phase}/airflow_run.json').write_text(json.dumps(result,indent=2),encoding='utf-8')
                assert data['state']=='success',result
                print(json.dumps({'dag_id':dag_id,'run_id':run_id,'state':data['state']}))
                return result
            time.sleep(2)
        raise TimeoutError(f'Airflow run did not finish: {run_id}')


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('dag',default='hospital_batch',nargs='?');p.add_argument('--retry-test',action='store_true');a=p.parse_args()
    Airflow().trigger(a.dag,{'inject_failure_once':True} if a.retry_test else {})
