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
    'ingest_reddit_data',
    default_args=default_args,
    description='Ingests NBA subreddit posts from the last 48 hours',
    # Run daily at 10:00 UTC
    schedule_interval='0 10 * * *',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['reddit', 'ingestion', 'raw'],
) as dag:

    run_extraction = BashOperator(
        task_id='extract_reddit_posts',
        bash_command='python /opt/airflow/scripts/extract_reddit.py'
    )