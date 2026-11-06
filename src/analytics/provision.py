"""Idempotent native SQL questions and dashboard layouts with filter bindings."""
import argparse
import json
import os
import time
import urllib.error
from pathlib import Path
from analytics.metabase_api import Metabase
from operations.foundation import ROOT
from analytics.report_dates import report_dates

FILTER="report_date BETWEEN {{start}} AND {{end}} [[AND department_sk IN (SELECT department_sk FROM mart.departments WHERE department_id={{department}})]]"
SNAP="report_date={{snapshot}} [[AND department_sk IN (SELECT department_sk FROM mart.departments WHERE department_id={{department}})]]"


def query_card(client, path, body):
    """Retry transient empty streaming responses during Metabase startup."""
    for attempt in range(5):
        try:
            result = client.request('POST', path, body)
            if isinstance(result, dict) and ('data' in result or result.get('error')):
                return result
        except (urllib.error.URLError, TimeoutError):
            if attempt == 4:
                raise
        if attempt < 4:
            time.sleep(2)
    raise RuntimeError('No completed Metabase query result: ' + path)


def specs(phase):
    period=lambda view,expression:f'SELECT {expression} AS value FROM mart.{view} WHERE {FILTER}'
    occupancy=f'SELECT CASE WHEN sum(invalid_snapshot_count)=0 THEN 100.0*sum(occupied_beds)/nullif(sum(active_capacity),0) END AS value FROM mart.bed_occupancy_daily WHERE {SNAP}'
    series=lambda view,expr:f'SELECT report_date,{expr} AS value FROM mart.{view} WHERE {FILTER} GROUP BY report_date ORDER BY report_date'
    op1=period('outpatient_daily','sum(visit_count)')
    op2=period('outpatient_daily','sum(waiting_minutes_sum)/nullif(sum(waiting_valid_count),0)')
    op3=period('appointment_daily','100.0*sum(no_show_count)/nullif(sum(classified_count),0)')
    ip1=period('inpatient_admission_daily','sum(admission_count)')
    ip3=period('inpatient_discharge_daily','sum(los_days_sum)/nullif(sum(los_valid_count),0)')
    lab1=period('lab_daily','sum(completed_orders)')
    lab2=period('lab_daily','sum(tat_minutes_sum)/nullif(sum(tat_valid_count),0)')
    lab3=period('lab_daily','100.0*sum(ontime_orders)/nullif(sum(ontime_valid_count),0)')
    dashboards={
      'Executive Overview':[
       ('OP01 — Lượt khám ngoại trú',op1,'scalar'),('IP01 — Lượt nhập viện',ip1,'scalar'),
       ('IP02 — Occupancy 00:00 Việt Nam (%)',occupancy,'scalar'),('LAB01 — Xét nghiệm hoàn thành',lab1,'scalar'),
       ('Ngoại trú theo ngày',series('outpatient_daily','sum(visit_count)'),'line'),
       ('Nhập viện theo ngày',series('inpatient_admission_daily','sum(admission_count)'),'line'),
       ('Xét nghiệm theo ngày',series('lab_daily','sum(completed_orders)'),'line')],
      'Outpatient Operations':[
       ('OP01 — Lượt khám ngoại trú',op1,'scalar'),('OP02 — Chờ khám (phút)',op2,'scalar'),('OP03 — No-show (%)',op3,'scalar'),
       ('Ngoại trú theo ngày',series('outpatient_daily','sum(visit_count)'),'line'),
       ('Chờ khám theo ngày',series('outpatient_daily','sum(waiting_minutes_sum)/nullif(sum(waiting_valid_count),0)'),'line'),
       ('No-show theo ngày',series('appointment_daily','100.0*sum(no_show_count)/nullif(sum(classified_count),0)'),'line')],
      'Inpatient & Bed Management':[
       ('IP01 — Lượt nhập viện',ip1,'scalar'),('IP02 — Occupancy 00:00 Việt Nam (%)',occupancy,'scalar'),('IP03 — LOS (ngày)',ip3,'scalar'),
       ('Nhập viện theo ngày',series('inpatient_admission_daily','sum(admission_count)'),'line'),
       ('LOS theo ngày xuất viện',series('inpatient_discharge_daily','sum(los_days_sum)/nullif(sum(los_valid_count),0)'),'line'),
       ('Giường theo khoa tại snapshot',f'SELECT department_sk,active_capacity,occupied_beds,invalid_snapshot_count FROM mart.bed_occupancy_daily WHERE {SNAP} ORDER BY department_sk','table')],
    }
    if phase in (4,5):
        rtbase="FROM mart.realtime_department_snapshot WHERE as_of=(SELECT max(as_of) FROM mart.realtime_department_snapshot) [[AND department_sk IN (SELECT department_sk FROM mart.departments WHERE department_id={{department}})]]"
        laboratory={'Laboratory & Real-Time':[
          ('LAB01 — Xét nghiệm hoàn thành',lab1,'scalar'),('LAB02 — TAT (phút)',lab2,'scalar'),('LAB03 — Đúng hạn (%)',lab3,'scalar'),
          ('RT01 — Check-in / 15 phút',f'SELECT sum(checkins_last_15m) AS value {rtbase}','scalar'),
          ('RT02 — Lab hoàn thành / 15 phút',f'SELECT sum(lab_results_last_15m) AS value {rtbase}','scalar'),
          ('Cửa sổ event-time và lần cập nhật',f"SELECT as_of-interval '15 minutes' AS window_start,as_of AS window_end,max(refreshed_at) AS last_updated_at {rtbase} GROUP BY as_of",'table'),
          ('Xét nghiệm theo ngày',series('lab_daily','sum(completed_orders)'),'line'),
          ('Realtime theo khoa',f'SELECT department_sk,as_of,checkins_last_15m,lab_results_last_15m,refreshed_at {rtbase} ORDER BY department_sk','table')]}
        if phase==4:dashboards=laboratory
        else:dashboards.update(laboratory)
    for name,cards in dashboards.items():
        cards.append(('Freshness và các lần chạy lỗi','SELECT * FROM mart.pipeline_health ORDER BY pipeline_name','table'))
    return dashboards


def provision(phase,manifest=None):
    start,end=report_dates(manifest)
    client=Metabase()
    db=client.login()
    existing_cards={r['name']:r for r in client.request('GET','card')}
    existing_dashboards={r['name']:r for r in client.request('GET','dashboard')}
    output=[]
    for name,questions in specs(phase).items():
        definitions=[]
        for i,(label,query,display) in enumerate(questions):
            card_name=f'{name} / {label}'
            defaults={'start':start,'end':end,'snapshot':end}
            tags={key:{'id':key,'name':key,'display-name':key,'type':'number' if key=='department' else 'date',
                       **({'required':True,'default':defaults[key]} if key!='department' else {})}
                  for key in ['start','end','snapshot','department'] if '{{'+key+'}}' in query}
            payload={'name':card_name,'display':display,'dataset_query':{'database':db,'type':'native','native':{'query':query,'template-tags':tags}},
                     'visualization_settings':{},'description':'Synthetic hospital analytics; UTC storage, Vietnam reporting dates. N/A when denominator is empty.'}
            if card_name in existing_cards:
                card=client.request('PUT',f'card/{existing_cards[card_name]["id"]}',payload)
            else: card=client.request('POST','card',payload)
            result=query_card(client,f'card/{card["id"]}/query',{'parameters':[]})
            assert not result.get('error'),(card_name,result.get('error'))
            # Execute an actual department filter as well as the default range.
            if 'department' in tags:
                filtered=query_card(client,f'card/{card["id"]}/query',{'parameters':[{'id':'department','type':'number','target':['variable',['template-tag','department']],'value':1}]})
                assert not filtered.get('error'),(card_name,filtered.get('error'))
            definitions.append({'id':card['id'],'name':label,'display':display,'query':query,'tags':list(tags),'rows':result.get('data',{}).get('rows',[])[:3]})
        parameters=[{'id':key,'name':label,'slug':key,'type':'number/=' if key=='department' else 'date/single',
                     **({'default':defaults[key]} if key!='department' else {})} for key,label in [('start','Từ ngày'),('end','Đến ngày'),('snapshot','Ngày snapshot 00:00'),('department','Mã khoa')]]
        payload={'name':name,'description':'Smart Hospital DW — synthetic data. Counts are per business entity; ratios use weighted components. RT uses latest single as_of.', 'parameters':parameters}
        if name in existing_dashboards: dashboard=client.request('PUT',f'dashboard/{existing_dashboards[name]["id"]}',payload)
        else: dashboard=client.request('POST','dashboard',payload)
        dashcards=[]
        for i,c in enumerate(definitions):
            dashcards.append({'id':-i-1,'card_id':c['id'],'row':(i//3)*5,'col':(i%3)*8,'size_x':8,'size_y':5,
                             'parameter_mappings':[{'parameter_id':key,'card_id':c['id'],'target':['variable',['template-tag',key]]} for key in c['tags']]})
        layout=client.request('PUT',f'dashboard/{dashboard["id"]}/cards',{'cards':dashcards})
        output.append({'dashboard_id':dashboard['id'],'name':name,'url':f'http://localhost:{os.environ.get("METABASE_HOST_PORT","13000")}/dashboard/{dashboard["id"]}','cards':definitions})
    folder=Path(os.environ.get('ANALYTICS_OUTPUT_DIR', str(ROOT/f'artifacts/phase{phase}')))
    folder.mkdir(parents=True,exist_ok=True)
    (folder/'dashboards.json').write_text(json.dumps({'report_start_date':start,'report_end_date':end,'dashboards':output,'queries_executed':sum(len(d['cards']) for d in output),'department_filter_executed':True},indent=2,ensure_ascii=False),encoding='utf-8')
    print(json.dumps({'phase':phase,'dashboards':[d['url'] for d in output]}))
    return output


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--phase',type=int,choices=[3,4,5],default=3)
    parser.add_argument('--manifest',type=Path)
    args=parser.parse_args();provision(args.phase,args.manifest)
