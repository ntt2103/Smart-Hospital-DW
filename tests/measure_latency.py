"""Clock-real live signals observed through Metabase, using scheduled 5m refresh.

No manual mart refresh is performed during this measurement.
"""
import argparse
import csv
import json
import math
import time
from analytics.metabase_api import Metabase
from operations.foundation import ROOT,connect
from streaming.producer import Journal
from streaming.common import utc,parse


def run(count=50,phase=5):
    client=Metabase();client.login();journal=Journal();journal.recover()
    samples={}
    command=journal.prepare_batch(count);journal.apply(command)
    for _ in range(1):
        rows=journal.db.execute('SELECT event_id,body FROM messages WHERE command_id=?',(command,)).fetchall()
        for event_id,value in rows:
            event=json.loads(value)
            if event['event_type'] not in ['checked_in','result_completed']:continue
            samples[event_id]={'event_id':event_id,'event_type':event['event_type'],'published_at':event['published_at'],
                               'publish_monotonic':journal.publish_references[event_id]}
        journal.recover()
    assert len(samples)>=2*count
    folder=ROOT/f'artifacts/phase{phase}';folder.mkdir(parents=True,exist_ok=True)
    (folder/'latency_pending.json').write_text(json.dumps({'sample_count':len(samples),'first_published_at':min(r['published_at'] for r in samples.values()),'mode':'scheduled 5 minute DAG; polling Metabase every 5 seconds'},indent=2),encoding='utf-8')
    query="SELECT event_id,published_at,visible_as_of FROM mart.event_visibility WHERE event_id IN ("+','.join("'"+key+"'::uuid" for key in samples)+")"
    stop=time.monotonic()+600
    next_reconciliation=time.monotonic()+15
    for probe in range(120):
        if time.monotonic()>=stop or all('first_visible_at' in r for r in samples.values()):break
        data=client.query(query)
        if data.get('error'):raise RuntimeError(data['error'])
        with connect('oltp','reader') as c:now=c.execute('SELECT clock_timestamp()').fetchone()[0]
        mono=time.monotonic()
        for row in data['data']['rows']:
            sample=samples[row[0]]
            if 'first_visible_at' in sample:continue
            elapsed=mono-sample['publish_monotonic']
            sample.update(first_visible_at=now.isoformat(),visible_as_of=row[2],latency_seconds=elapsed,
                          wall_clock_latency_seconds=(now-parse(sample['published_at'])).total_seconds(),monotonic_latency_seconds=elapsed)
        if time.monotonic()>=next_reconciliation:
            journal.reconcile();next_reconciliation=time.monotonic()+20
        time.sleep(5)
    rows=list(samples.values());visible=[r for r in rows if 'first_visible_at' in r]
    values=sorted(r['latency_seconds'] for r in visible)
    pct=lambda p:values[max(0,math.ceil(len(values)*p)-1)] if values else None
    skew=max((abs(r['wall_clock_latency_seconds']-r['monotonic_latency_seconds']) for r in visible),default=None)
    result={'sample_count':len(rows),'visible_count':len(visible),'timeouts':len(rows)-len(visible),
            'p50_seconds':pct(.5),'p95_seconds':pct(.95),'max_clock_delta_seconds':skew,
            'target_seconds':600,'target_met':len(visible)==len(rows) and pct(.95)<=600,
            'wall_clock_subtraction_reliable':skew is not None and skew<5,
            'measurement':'monotonic elapsed from immutable publication anchor to first Metabase API observation of a committed snapshot processing receipt; wall-clock deltas retained separately; browser render not measured',
            'settings':{'dag_schedule':'*/5 * * * *','manual_refresh_during_measurement':False,'probe_seconds':5,'window_minutes':15,'load':f'one burst of {count} valid OP/lab journeys; {len(samples)} measured KPI signals; source deltas reported separately','clock':'monotonic same-process publication anchor; UTC fields from source PostgreSQL; no accelerated clock'},
            'events':[{k:v for k,v in r.items() if k!='publish_monotonic'} for r in rows]}
    (folder/'latency_measurements.json').write_text(json.dumps(result,indent=2),encoding='utf-8')
    with (folder/'latency_measurements.csv').open('w',newline='',encoding='utf-8') as file:
        writer=csv.DictWriter(file,fieldnames=['event_id','event_type','published_at','first_visible_at','visible_as_of','latency_seconds','wall_clock_latency_seconds','monotonic_latency_seconds']);writer.writeheader();writer.writerows(result['events'])
    print(json.dumps({k:v for k,v in result.items() if k!='events'}))
    assert result['target_met'], 'Latency target/clock integrity not met; see recorded evidence'


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--count',type=int,default=50);parser.add_argument('--phase',type=int,default=5)
    args=parser.parse_args();run(args.count,args.phase)
