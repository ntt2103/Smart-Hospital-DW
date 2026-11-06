from datetime import datetime,timedelta,timezone
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

with DAG('hospital_batch',schedule='0 * * * *',start_date=datetime(2026,1,1,tzinfo=timezone.utc),catchup=False,is_paused_upon_creation=True,
         max_active_runs=1,default_args={'retries':2,'retry_delay':timedelta(seconds=15)},
         dagrun_timeout=timedelta(minutes=45),tags=['phase3','historical']) as dag:
    batch=BashOperator(task_id='extract_validate_stage_dimensions_facts_marts_audit',
                       bash_command='cd /app && python -m batch_pipeline.jobs.batch',
                       do_xcom_push=False,
                       env={'BATCH_MODE':"{{ dag_run.conf.get('mode','incremental') }}",'BATCH_START_DATE':"{{ dag_run.conf.get('start_date','') }}",'BATCH_END_DATE':"{{ dag_run.conf.get('end_date','') }}",'BATCH_FAIL_FIRST':"{{ 'true' if dag_run.conf.get('inject_failure_once',false) else 'false' }}",'BATCH_ATTEMPT':"{{ ti.try_number }}"},append_env=True,
                       execution_timeout=timedelta(minutes=40))
