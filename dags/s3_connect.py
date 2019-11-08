from datetime import datetime, timedelta
import logging
import os

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators import StageJsonToS3
from airflow.hooks.S3_hook import S3Hook

from helpers import StockSymbols

default_args = {
    'owner': 'Sulman M',
    'start_date': datetime(2019, 11, 8),
    'depends_on_past': False,
    'retries': 0,
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=5),
    'catchup_by_default': False,
}

def create_bucket(*args, **kwargs):
    logging.info(f'Creating NEW S3 bucket')
    ed = kwargs['execution_date'].date()
    bname = f"airflow-usstock-{ed}"
    s3_hook = S3Hook(aws_conn_id='aws_credential')
    logging.info(f'MY BUCKET NAME {bname}')
    s3_hook.create_bucket(bucket_name=bname)

with DAG('S3_connect_DAG', schedule_interval='@once', default_args=default_args) as dag:
    # Task 1 - Begin
    start_operator = DummyOperator(
        task_id="Begin_createBucketS3"
        )

    create_bucket_task = PythonOperator(
        task_id="create_new_bucket",
        python_callable=create_bucket,
        provide_context=True
        )

    start_operator >> create_bucket_task