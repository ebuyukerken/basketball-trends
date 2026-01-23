from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'dbt_transformation',
    default_args=default_args,
    description='Waits for ingestion (Logs, Schedule, Reddit), then runs dbt models',
    schedule_interval='30 10 * * *',
    start_date=datetime(2025, 10, 22),
    catchup=False,
    tags=['dbt', 'transformation'],
) as dag:

    # 1. Wait for NBA Game Logs Ingestion
    wait_for_nba_logs = ExternalTaskSensor(
        task_id='wait_for_nba_logs',
        external_dag_id='ingest_nba_data', # Must match the Game Logs DAG ID
        external_task_id=None, # Wait for the whole DAG to finish
        check_existence=True,
        mode='reschedule',
        timeout=3600,
        execution_delta=timedelta(minutes=30)
    )

    # 2. Wait for NBA Schedule Ingestion (NEW)
    wait_for_nba_schedule = ExternalTaskSensor(
        task_id='wait_for_nba_schedule',
        external_dag_id='ingest_nba_schedule', # Must match the Schedule DAG ID
        external_task_id=None,
        check_existence=True,
        mode='reschedule',
        timeout=3600,
        execution_delta=timedelta(minutes=30)
    )

    # 3. Wait for Reddit Ingestion
    wait_for_reddit = ExternalTaskSensor(
        task_id='wait_for_reddit',
        external_dag_id='ingest_reddit_data',
        external_task_id=None,
        check_existence=True,
        mode='reschedule',
        timeout=3600,
        execution_delta=timedelta(minutes=30)
    )

    # 4. Run dbt
    # Using 'run_dt' var to pass the logical date to dbt
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /opt/airflow/dbt_project && dbt run --profiles-dir . --vars \'{"run_dt": "{{ ds }}"}\''
    )

    # Set Dependencies
    # dbt runs only when ALL three ingestions are complete
    [wait_for_nba_logs, wait_for_nba_schedule, wait_for_reddit] >> dbt_run