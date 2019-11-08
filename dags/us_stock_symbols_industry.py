from datetime import datetime, timedelta
import logging
import os
import json

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators import StageJsonToS3
from airflow.operators import S3CreateBucket
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

stock_symbols = StockSymbols()

def write_stock_symbols_to_tmp(industry=None):
	if not industry:
		logging.info(f'No industry provided {industry}')
		return True
	logging.info(f'Writing tmp {industry}')
	filenm, written_records = stock_symbols.write_stock_symbols_for_industry(industry)
	logging.info(f'Written {written_records} records to {filenm}')


def check_files_written():
	stock_symbol_files = stock_symbols.US_STOCK_INDUSTRY_CODES
	for file in stock_symbol_files:
		filenm = "/usr/local/airflow/{}".format(stock_symbol_files[file]['filename'])
		if os.path.isfile(filenm):
			with open(filenm, "r") as f:
				if not len(json.load(f)) > 1:
					raise ValueError(f'Input file {filenm} has no data!')
				logging.info(f'Input file {filenm} written OK.')
		else:
			logging.info(f'Input file {filenm} does not exist!')				

with DAG('US_Stock_Symbols_DAG', schedule_interval='@once', default_args=default_args) as dag:
    # Task 1 - Begin
    start_operator = DummyOperator(
        task_id="Begin_StockSymbolsLoad"
        )
    # Automotive Industry
    automotive_stock_symbols_to_tmp = PythonOperator(
    	task_id="Fetch_AutomotiveStockSymbols_toTmp",
    	python_callable=write_stock_symbols_to_tmp,
    	op_kwargs={'industry': 'Automotive'}
    	)

    # Agriculture Industry
    agriculture_stock_symbols_to_tmp = PythonOperator(
    	task_id="Fetch_AgricultureStockSymbols_toTmp",
    	python_callable=write_stock_symbols_to_tmp,
    	op_kwargs={'industry': 'Agriculture'}
    	)

    # Basic Materials/Resources Industry
    materials_resources_stock_symbols_to_tmp = PythonOperator(
    	task_id="Fetch_MaterialsResourcesStockSymbols_toTmp",
    	python_callable=write_stock_symbols_to_tmp,
    	op_kwargs={'industry': 'Basic Materials/Resources'}
    	)

    # Business/Consumer Services Industry
    business_consumer_srv_stock_symbols_to_tmp = PythonOperator(
    	task_id="Fetch_BusinessConsumerSrvStockSymbols_toTmp",
    	python_callable=write_stock_symbols_to_tmp,
    	op_kwargs={'industry': 'Business/Consumer Services'}
    	)

    # Consumer Goods Industry
    consumer_goods_stock_symbols_to_tmp = PythonOperator(
    	task_id="Fetch_ConsumerGoodsStockSymbols_toTmp",
    	python_callable=write_stock_symbols_to_tmp,
    	op_kwargs={'industry': 'Consumer Goods'}
    	)

    # Energy Industry
    energy_stock_symbols_to_tmp = PythonOperator(
    	task_id="Fetch_EnergyStockSymbols_toTmp",
    	python_callable=write_stock_symbols_to_tmp,
    	op_kwargs={'industry': 'Energy'}
    	)

    # Financial Services Industry
    financial_srv_stock_symbols_to_tmp = PythonOperator(
    	task_id="Fetch_FinancialServicesStockSymbols_toTmp",
    	python_callable=write_stock_symbols_to_tmp,
    	op_kwargs={'industry': 'Financial Services'}
    	)    

    create_execution_date_s3_bucket = S3CreateBucket(
    	task_id="Create_S3Bucket",
    	aws_conn_id='aws_credential',
    	bucket_name="us-stock-data",
    	execution_date='{{ ds }}'
    	)

    check_inbound_files = PythonOperator(
    	task_id="Check_InboundFilesWritten",
    	python_callable=check_files_written
    	)
    # automotive_stock_symbols_to_s3 = StageJsonToS3(
    # 	task_id="Stage_AutomotiveStockSymbols_toS3",


    # 	get_data=us_stock_symbol_by_industry,
    # 	)

    completion_operator = DummyOperator(
    	task_id="End_StockSymbolsLoad"
    	)

    start_operator >> automotive_stock_symbols_to_tmp >> create_execution_date_s3_bucket >>  check_inbound_files >> completion_operator
    start_operator >> agriculture_stock_symbols_to_tmp >> create_execution_date_s3_bucket
    start_operator >> materials_resources_stock_symbols_to_tmp >> create_execution_date_s3_bucket
    start_operator >> business_consumer_srv_stock_symbols_to_tmp >> create_execution_date_s3_bucket
    start_operator >> consumer_goods_stock_symbols_to_tmp >> create_execution_date_s3_bucket
    start_operator >> energy_stock_symbols_to_tmp >> create_execution_date_s3_bucket
    start_operator >> financial_srv_stock_symbols_to_tmp >> create_execution_date_s3_bucket