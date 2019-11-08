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
    'retries': 3,
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=5),
    'catchup_by_default': False,
}


def write_stock_symbols_to_tmp(industry=None):
	if not industry:
		logging.info(f'No industry provided {industry}')
		return True
	logging.info(f'Writing tmp {industry}')
	filenm, written_records = StockSymbols.write_stock_symbols_for_industry(industry)
	logging.info(f'Written {written_records} records to {filenm}')



with DAG('US_Stock_Symbols_DAG', schedule_interval='@once', default_args=default_args) as dag:
    # Task 1 - Begin
    start_operator = DummyOperator(
        task_id="Begin_StockSymbolsLoad"
        )

    automotive_stock_symbols_to_tmp = PythonOperator(
    	task_id="Fetch_AutomotiveStockSymbols_toTmp",
    	python_callable=write_stock_symbols_to_tmp,
    	op_kwargs={'industry': 'Automotive'}
    	)

    # automotive_stock_symbols_to_s3 = StageJsonToS3(
    # 	task_id="Stage_AutomotiveStockSymbols_toS3",


    # 	get_data=us_stock_symbol_by_industry,
    # 	)

    completion_operator = DummyOperator(
    	task_id="End_StockSymbolsLoad"
    	)

    start_operator >> automotive_stock_symbols_to_tmp >> completion_operator