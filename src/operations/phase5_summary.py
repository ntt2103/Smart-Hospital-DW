"""Collect recorded Phase 5 evidence; never invent missing acceptance results."""
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
FOLDER = ROOT/'artifacts/phase5'


def read(name):
    path=FOLDER/name
    return json.loads(path.read_text(encoding='utf-8-sig')) if path.exists() else None


def memory_bytes(value):
    match=re.match(r'([\d.]+)\s*([KMGT]?i?B)',value)
    if not match: return None
    factors={'B':1,'kB':1000,'KB':1000,'MB':1000**2,'GB':1000**3,
             'KiB':1024,'MiB':1024**2,'GiB':1024**3,'TiB':1024**4}
    return round(float(match[1])*factors[match[2]])


def main():
    manifest=read('dataset_manifest.json')
    summary={'dataset':{key:manifest[key] for key in ('report_start_date','report_end_date','encounters_per_day',
        'horizon_days','counts','core_source_rows','association_rows','catalog_extension_rows','oltp_total_rows',
        'business_sha256','generation_seconds','peak_process_rss_bytes','csv_bytes')},
        'source_load':read('source_load.json'),'source_resume':read('source_resume.json'),
        'initial_etl':read('initial_etl_success.json'),'latest_batch':read('latest_batch.json'),
        'live_corpus':read('live_corpus_manifest.json'),'gates':read('gates.json'),
        'current_warehouse_after_dags':read('warehouse_after_dags.json'),
        'airflow_acceptance':read('airflow_acceptance.json'),
        'verification':read('unit_tests.json'),
        'recovery':{'archives':read('recovery_archives.json'),
                    'verification':read('recovery_verification.json'),
                    'health':read('recovery_health.json')},
        'resources':{},'benchmark':None,'contention':None,'control':None}
    path=FOLDER/'resource_samples.jsonl'
    if path.exists():
        for line in path.read_text(encoding='utf-8-sig').splitlines():
            if not line.strip(): continue
            sample=json.loads(line)
            for item in sample['containers']:
                record=summary['resources'].setdefault(item['Name'],{'samples':0,'peak_memory_bytes':0,'peak_cpu_percent':0})
                record['samples']+=1
                record['peak_memory_bytes']=max(record['peak_memory_bytes'],memory_bytes(item['MemUsage']) or 0)
                record['peak_cpu_percent']=max(record['peak_cpu_percent'],float(item['CPUPerc'].rstrip('%')))
    for folder,key in [('benchmark','benchmark'),('benchmark_contention','contention'),('benchmark_control','control')]:
        before=read(folder+'/benchmark_before.json')
        after=read(folder+'/benchmark_after.json')
        comparison=read(folder+'/comparison.json')
        if not (before and after and comparison): continue
        rows=[]
        for query,value in comparison['queries'].items():
            rows.append({'query':query,'before':before['summary'].get(query),'after':after['summary'].get(query),**value})
        summary[key]={'same_workload':comparison['same_workload'],'environment_matches':comparison['environment_matches'],
            'clients':before['clients'],'samples':before['requested_samples'],'rt_as_of':before['rt_as_of'],'queries':rows,
            'database_bytes_before':before['environment']['database_bytes'],
            'database_bytes_after':after['environment']['database_bytes']}
    output=FOLDER/'implementation_summary.json'
    output.write_text(json.dumps(summary,indent=2,ensure_ascii=False),encoding='utf-8')
    recovery=summary['recovery']
    recovery_passed=(recovery['archives'] or {}).get('status')=='passed' and all(
        (recovery[key] or {}).get('passed') is True for key in ('verification','health'))
    print(json.dumps({'output':str(output),'base_source_rows':manifest['oltp_total_rows'],
                      'benchmark_paired':bool(summary['benchmark']),'recovery_passed':recovery_passed}))


if __name__=='__main__': main()
