from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from twitter_etl import twitter_extract_transfer_load

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 6, 8),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'twitter_dag',
    default_args = default_args,
    description='Twitter ETL process Dag',
    schedule_interval=timedelta(days=1)
)

run_etl = PythonOperator(
    task_id='my_etl_task',
    python_callable=twitter_extract_transfer_load,
    dag=dag,
)

run_etl