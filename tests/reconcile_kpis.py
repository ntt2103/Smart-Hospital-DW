import json
import argparse
from operations.foundation import ROOT,connect
from analytics.report_dates import report_dates

def source_queries(start,end):
 FILTER=f"BETWEEN DATE '{start}' AND DATE '{end}'"
 snapshot=f"TIMESTAMPTZ '{end} 00:00:00+07'"
 return {
 'OP01':f"SELECT count(*) FROM encounters WHERE encounter_type='OUTPATIENT' AND (consultation_started_at AT TIME ZONE 'Asia/Ho_Chi_Minh')::date {FILTER}",
 'OP02':f"SELECT avg(extract(epoch from consultation_started_at-checked_in_at)/60.0) FROM encounters WHERE encounter_type='OUTPATIENT' AND checked_in_at<=consultation_started_at AND (consultation_started_at AT TIME ZONE 'Asia/Ho_Chi_Minh')::date {FILTER}",
 'OP03':f"SELECT 100.0*count(*) FILTER(WHERE status='NO_SHOW')/nullif(count(*),0) FROM appointments WHERE status IN('ATTENDED','NO_SHOW') AND (scheduled_at AT TIME ZONE 'Asia/Ho_Chi_Minh')::date {FILTER}",
 'IP01':f"SELECT count(*) FROM encounters WHERE encounter_type='INPATIENT' AND (admitted_at AT TIME ZONE 'Asia/Ho_Chi_Minh')::date {FILTER}",
 'IP02':f"SELECT 100.0*count(a.bed_assignment_id)/nullif(count(*),0) FROM beds b JOIN bed_status_history h USING(bed_id) LEFT JOIN bed_assignments a ON a.bed_id=b.bed_id AND a.assigned_at<={snapshot} AND (a.released_at IS NULL OR a.released_at>{snapshot}) WHERE h.status='ACTIVE' AND h.effective_from<={snapshot} AND (h.effective_to IS NULL OR h.effective_to>{snapshot})",
 'IP03':f"SELECT avg(extract(epoch from discharged_at-admitted_at)/86400.0) FROM encounters WHERE encounter_type='INPATIENT' AND status='DISCHARGED' AND (discharged_at AT TIME ZONE 'Asia/Ho_Chi_Minh')::date {FILTER}",
 'LAB01':f"SELECT count(*) FROM lab_orders WHERE status='COMPLETED' AND (result_completed_at AT TIME ZONE 'Asia/Ho_Chi_Minh')::date {FILTER}",
 'LAB02':f"SELECT avg(extract(epoch from result_completed_at-ordered_at)/60.0) FROM lab_orders WHERE status='COMPLETED' AND (result_completed_at AT TIME ZONE 'Asia/Ho_Chi_Minh')::date {FILTER}",
 'LAB03':f"SELECT 100.0*count(*) FILTER(WHERE extract(epoch from result_completed_at-ordered_at)/60.0<=target_tat_minutes_at_order)/nullif(count(*),0) FROM lab_orders WHERE status='COMPLETED' AND (result_completed_at AT TIME ZONE 'Asia/Ho_Chi_Minh')::date {FILTER}",
}


def run(manifest=None,phase=5):
    from tests.integration_warehouse import AGG
    start,end=report_dates(manifest)
    FILTER=f"BETWEEN DATE '{start}' AND DATE '{end}'"
    checks=[]
    with connect('oltp','reader') as source,connect('dwh','etl') as warehouse:
        for kpi,query in source_queries(start,end).items():
            expected=source.execute(query).fetchone()[0]
            if kpi=='IP02':query=f"SELECT CASE WHEN sum(invalid_snapshot_count)=0 THEN 100.0*sum(occupied_beds)/nullif(sum(active_capacity),0) END FROM mart.bed_occupancy_daily WHERE report_date=DATE '{end}'"
            else:
                view,expression=AGG[kpi];query=f'SELECT {expression} FROM {view} WHERE report_date {FILTER}'
            got=warehouse.execute(query).fetchone()[0]
            assert got is None if expected is None else got is not None and abs(float(got)-float(expected))<1e-7,(kpi,got,expected)
            checks.append({'kpi':kpi,'source':float(expected) if expected is not None else None,'warehouse':float(got) if got is not None else None})
    folder=ROOT/f'artifacts/phase{phase}';folder.mkdir(parents=True,exist_ok=True)
    (folder/'historical_regression.json').write_text(json.dumps({'report_start_date':start,'report_end_date':end,'passed':len(checks),'checks':checks},indent=2),encoding='utf-8')
    print('All 9 historical KPIs reconcile to OLTP after streaming.')


if __name__=='__main__':
    parser=argparse.ArgumentParser();parser.add_argument('--manifest');parser.add_argument('--phase',type=int,default=5)
    args=parser.parse_args();run(args.manifest,args.phase)
