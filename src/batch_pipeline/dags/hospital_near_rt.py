from datetime import datetime,timedelta,timezone
from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

with DAG('hospital_near_rt',schedule='*/5 * * * *',start_date=datetime(2026,1,1,tzinfo=timezone.utc),catchup=False,is_paused_upon_creation=True,
         max_active_runs=1,default_args={'retries':2,'retry_delay':timedelta(seconds=10)},dagrun_timeout=timedelta(minutes=4),tags=['phase4','near-real-time']) as dag:
    refresh=BashOperator(task_id='dedup_event_time_window_publish_audit',bash_command='cd /app && python -m batch_pipeline.jobs.near_rt',do_xcom_push=False,execution_timeout=timedelta(seconds=180))
