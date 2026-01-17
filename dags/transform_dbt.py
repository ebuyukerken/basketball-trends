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
    description='Waits for ingestion, then runs dbt models',
    schedule_interval='0 10 * * *',
    start_date=datetime(2025, 10, 22),
    catchup=False,
    tags=['dbt', 'transformation'],
) as dag:

    # 1. Wait for NBA Ingestion
    wait_for_nba = ExternalTaskSensor(
        task_id='wait_for_nba',
        external_dag_id='ingest_nba_data',
        external_task_id=None,
        check_existence=True,
        mode='reschedule',
        timeout=3600,
    )

    # 2. Wait for Reddit Ingestion
    wait_for_reddit = ExternalTaskSensor(
        task_id='wait_for_reddit',
        external_dag_id='ingest_reddit_data',
        external_task_id=None,
        check_existence=True,
        mode='reschedule',
        timeout=3600,
    )

    # 3. Run dbt
    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command="""
            cd /opt/airflow/dbt_project && \
            dbt run --vars '{"run_dt": "{{ ds }}"}'
        """
    )

    [wait_for_nba, wait_for_reddit] >> dbt_run