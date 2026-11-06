import json
import re
from analytics.metabase_api import Metabase
from operations.foundation import ROOT,connect
from analytics.report_dates import report_dates


def run(phase=3):
    client=Metabase();client.login()
    report=json.loads((ROOT/f'artifacts/phase{phase}/dashboards.json').read_text())
    start,end=report_dates({'report_start_date':report['report_start_date'],'report_end_date':report['report_end_date']}) if 'report_start_date' in report else report_dates()
    checks=[]
    for dashboard in report['dashboards']:
        layout=client.request('GET',f'dashboard/{dashboard["dashboard_id"]}')
        assert len(layout['dashcards'])==len(dashboard['cards'])
        for card in dashboard['cards']:
            if card['display']!='scalar':continue
            query=re.sub(r'\[\[.*?\]\]','',card['query'])
            for key,default in [('start',start),('end',end),('snapshot',end)]:
                query=query.replace('{{'+key+'}}',f"DATE '{default}'")
            actual=client.request('POST',f'card/{card["id"]}/query',{'parameters':[]})['data']['rows'][0][0]
            with connect('dwh','reporter') as c: expected=c.execute(query).fetchone()[0]
            if expected is None:assert actual is None
            else:assert abs(float(expected)-float(actual))<1e-6,(card['name'],expected,actual)
            checks.append({'card':card['name'],'expected':float(expected) if expected is not None else None,'actual':actual})
    if phase in (3,5):
        card=report['dashboards'][0]['cards'][0]
        params=[{'id':key,'type':'date/single','target':['variable',['template-tag',key]],'value':start} for key in ['start','end']]
        actual=client.request('POST',f'card/{card["id"]}/query',{'parameters':params})['data']['rows'][0][0]
        with connect('dwh','reporter') as c:expected=c.execute('SELECT sum(visit_count) FROM mart.outpatient_daily WHERE report_date=%s',(start,)).fetchone()[0]
        assert actual is None if expected is None else abs(float(actual)-float(expected))<1e-6
        checks.append({'date_filter':start,'expected':float(expected) if expected is not None else None,'actual':actual})
    (ROOT/f'artifacts/phase{phase}/dashboard_reconciliation.json').write_text(json.dumps({'checks':checks,'passed':len(checks),'layout_verified_by_api':True,'visual_browser_verification':False,'verification_scope':'Metabase API layout and SQL results; browser rendering was not measured.'},indent=2,ensure_ascii=False),encoding='utf-8')
    print(json.dumps({'phase':phase,'dashboard_checks_passed':len(checks)}))


if __name__=='__main__':
    import sys
    run(int(sys.argv[1]) if len(sys.argv)>1 else 3)
