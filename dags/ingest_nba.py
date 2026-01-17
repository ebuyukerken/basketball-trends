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
    'ingest_nba_data',
    default_args=default_args,
    description='Ingests NBA game logs for the logical date (yesterday)',
    # Run daily at 10:00 UTC
    schedule_interval='0 10 * * *',
    # Start date determines when the first run happens.
    # Adjust this to the start of the current season or today.
    start_date=datetime(2025, 10, 22),
    catchup=False,
    tags=['nba', 'ingestion', 'raw'],
) as dag:

    # The BashOperator runs inside the Docker container.
    # It has access to the 'scripts' folder because we mounted it in docker-compose.
    run_extraction = BashOperator(
        task_id='extract_nba_games',
        bash_command='python /opt/airflow/scripts/extract_nba.py --date {{ ds }}'
    )