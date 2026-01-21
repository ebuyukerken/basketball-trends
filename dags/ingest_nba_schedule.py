from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'ingest_nba_schedule',  # DAG ID'si unique olmalı
    default_args=default_args,
    description='Ingests NBA schedule/fixture data with UTC start times',
    schedule_interval='0 10 * * *',
    # Start of the current season
    start_date=datetime(2025, 10, 22),
    catchup=False,
    tags=['nba', 'ingestion', 'schedule'],
) as dag:

    run_schedule_extraction = BashOperator(
        task_id='extract_nba_schedule_task',
        bash_command='python /opt/airflow/scripts/extract_nba_schedule.py --date {{ ds }}'
    )