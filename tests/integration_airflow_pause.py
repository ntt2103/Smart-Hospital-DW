import json
import time
from operations.airflow_control import Airflow
from operations.foundation import ROOT
from streaming.producer import Journal
from tests.integration_streaming import wait_for,sqlval


def run():
    client=Airflow()
    client.request('PATCH','/api/v2/dags/hospital_near_rt',{'is_paused':True})
    journal=Journal();journal.recover()
    command=journal.prepare();journal.apply(command);journal.recover()
    ids=[r[0] for r in journal.db.execute('SELECT event_id FROM messages WHERE command_id=?',(command,))]
    for _ in range(90):
        if sqlval('SELECT count(*) FROM raw.events WHERE event_id=ANY(%s::uuid[])',(ids,))==len(ids):break
        journal.reconcile();time.sleep(2)
    else:raise AssertionError('Consumer failed to ingest while DAG paused')
    result=client.trigger('hospital_near_rt')
    assert result['state']=='success'
    output={'consumer_ingested_while_mart_dag_paused':True,'dag_unpaused_and_caught_up':True,'event_ids':ids}
    (ROOT/'artifacts/phase4/pause_recovery.json').write_text(json.dumps(output,indent=2),encoding='utf-8')
    print(json.dumps(output))


if __name__=='__main__':run()
