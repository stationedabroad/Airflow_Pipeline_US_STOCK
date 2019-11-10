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
    'start_date': datetime(2019, 11, 9),
    'depends_on_past': False,
    'retries': 1,
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=1),
    'catchup_by_default': False ,
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
			raise ValueError(f'Input file {filenm} does not exist!')				


with DAG('US_Stock_Symbols_DAG', schedule_interval='@once', default_args=default_args) as dag:
    # Task 1 - Begin
    start_operator = DummyOperator(
        task_id="Begin_StockSymbolsLoad"
        )
    # Automotive Industry
    automotive_stock_symbols_to_tmp = PythonOperator(
    	task_id="Fetch_Automotive_StockSymbols_toTmp",
    	python_callable=write_stock_symbols_to_tmp,
    	op_kwargs={'industry': 'Automotive'}
    	)

    # Agriculture Industry
    agriculture_stock_symbols_to_tmp = PythonOperator(
    	task_id="Fetch_Agriculture_StockSymbols_toTmp",
    	python_callable=write_stock_symbols_to_tmp,
    	op_kwargs={'industry': 'Agriculture'}
    	)

    # Basic Materials/Resources Industry
    materials_resources_stock_symbols_to_tmp = PythonOperator(
    	task_id="Fetch_Materials_Resources_StockSymbols_toTmp",
    	python_callable=write_stock_symbols_to_tmp,
    	op_kwargs={'industry': 'Basic Materials/Resources'}
    	)

    # Business/Consumer Services Industry
    business_consumer_srv_stock_symbols_to_tmp = PythonOperator(
    	task_id="Fetch_Business_ConsumerSrv_StockSymbols_toTmp",
    	python_callable=write_stock_symbols_to_tmp,
    	op_kwargs={'industry': 'Business/Consumer Services'}
    	)

    # Consumer Goods Industry
    consumer_goods_stock_symbols_to_tmp = PythonOperator(
    	task_id="Fetch_ConsumerGoods_StockSymbols_toTmp",
    	python_callable=write_stock_symbols_to_tmp,
    	op_kwargs={'industry': 'Consumer Goods'}
    	)

    # Energy Industry
    energy_stock_symbols_to_tmp = PythonOperator(
    	task_id="Fetch_Energy_StockSymbols_toTmp",
    	python_callable=write_stock_symbols_to_tmp,
    	op_kwargs={'industry': 'Energy'}
    	)

    # Financial Services Industry
    financial_srv_stock_symbols_to_tmp = PythonOperator(
    	task_id="Fetch_FinancialServices_StockSymbols_toTmp",
    	python_callable=write_stock_symbols_to_tmp,
    	op_kwargs={'industry': 'Financial Services'}
    	)    

    # Health-care/Life-sciences Industry
    healthcare_lifesciences_stock_symbols_to_tmp = PythonOperator(
    	task_id="Fetch_HealthCare_LifeSciences_StockSymbols_toTmp",
    	python_callable=write_stock_symbols_to_tmp,
    	op_kwargs={'industry': 'Health Care/Life Sciences'}
    	)   

    # Industrial Goods Industry
    industrial_goods_stock_symbols_to_tmp = PythonOperator(
    	task_id="Fetch_IndustrialGoods_StockSymbols_toTmp",
    	python_callable=write_stock_symbols_to_tmp,
    	op_kwargs={'industry': 'Industrial Goods'}
    	)

    # Leisure/Arts/Hospitality Industry
    leisure_arts_hospitality_stock_symbols_to_tmp = PythonOperator(
    	task_id="Fetch_Leisure_Arts_Hospitality_StockSymbols_toTmp",
    	python_callable=write_stock_symbols_to_tmp,
    	op_kwargs={'industry': 'Leisure/Arts/Hospitality'}
    	)

    # Media/Entertainment Industry
    media_entertainment_stock_symbols_to_tmp = PythonOperator(
    	task_id="Fetch_Media_Entertainment_StockSymbols_toTmp",
    	python_callable=write_stock_symbols_to_tmp,
    	op_kwargs={'industry': 'Media/Entertainment'}
    	)

    # Real Estate/Construction Industry
    real_estate_construction_stock_symbols_to_tmp = PythonOperator(
    	task_id="Fetch_RealEstate_Construction_StockSymbols_toTmp",
    	python_callable=write_stock_symbols_to_tmp,
    	op_kwargs={'industry': 'Real Estate/Construction'}
    	)

    # Retail/Wholesale Industry
    retail_wholesale_stock_symbols_to_tmp = PythonOperator(
    	task_id="Fetch_Retail_Wholesale_StockSymbols_toTmp",
    	python_callable=write_stock_symbols_to_tmp,
    	op_kwargs={'industry': 'Retail/Wholesale'}
    	)  

    # Technology Industry
    technology_stock_symbols_to_tmp = PythonOperator(
    	task_id="Fetch_Technology_StockSymbols_toTmp",
    	python_callable=write_stock_symbols_to_tmp,
    	op_kwargs={'industry': 'Technology'}
    	)

    # Telecommunication Services Industry
    telocommunication_srv_stock_symbols_to_tmp = PythonOperator(
    	task_id="Fetch_TelecommunicationServices_StockSymbols_toTmp",
    	python_callable=write_stock_symbols_to_tmp,
    	op_kwargs={'industry': 'Telecommunication Services'}
    	)

    # Transportation/Logistics Industry
    transportation_logistics_stock_symbols_to_tmp = PythonOperator(
    	task_id="Fetch_Transportation_Logistics_StockSymbols_toTmp",
    	python_callable=write_stock_symbols_to_tmp,
    	op_kwargs={'industry': 'Transportation/Logistics'}
    	)

    # Utilities Industry
    utilities_stock_symbols_to_tmp = PythonOperator(
    	task_id="Fetch_Utilities_StockSymbols_toTmp",
    	python_callable=write_stock_symbols_to_tmp,
    	op_kwargs={'industry': 'Utilities'}
    	)

    # Automotive Industry Staging to S3
    automotive_stock_symbols_to_s3 = StageJsonToS3(
    	task_id='Stage_Automotive_StockSymbols_toS3',
    	aws_conn_id='aws_credential',
    	s3_bucket='us-stock-data',
    	s3_key='Automotive-{}.json',
    	execution_date='{{ ds }}',
    	path_to_data=stock_symbols.US_STOCK_INDUSTRY_CODES['Automotive']['filename'],
    	)

    # Agriculture Industry Staging to S3
    agriculture_stock_symbols_to_s3 = StageJsonToS3(
    	task_id='Stage_Agriculture_StockSymbols_toS3',
    	aws_conn_id='aws_credential',
    	s3_bucket='us-stock-data',
    	s3_key='Agriculture-{}.json',
    	execution_date='{{ ds }}',
    	path_to_data=stock_symbols.US_STOCK_INDUSTRY_CODES['Agriculture']['filename'],
    	)

    # Basic Materials/Resources Industry Staging to S3
    materials_resources_stock_symbols_to_s3 = StageJsonToS3(
    	task_id='Stage_MaterialsResources_StockSymbols_toS3',
    	aws_conn_id='aws_credential',
    	s3_bucket='us-stock-data',
    	s3_key='BasicMaterialsResources-{}.json',
    	execution_date='{{ ds }}',
    	path_to_data=stock_symbols.US_STOCK_INDUSTRY_CODES['Basic Materials/Resources']['filename'],
    	)

    # Business/Consumer Services Industry Staging to S3
    business_consumer_srv_stock_symbols_to_s3 = StageJsonToS3(
    	task_id='Stage_Business_ConsumerServices_StockSymbols_toS3',
    	aws_conn_id='aws_credential',
    	s3_bucket='us-stock-data',
    	s3_key='Business_ConsumerServices-{}.json',
    	execution_date='{{ ds }}',
    	path_to_data=stock_symbols.US_STOCK_INDUSTRY_CODES['Business/Consumer Services']['filename'],
    	)

    # Consumer Goods Industry Staging to S3
    consumer_goods_stock_symbols_to_s3 = StageJsonToS3(
    	task_id='Stage_ConsumerGoods_StockSymbols_toS3',
    	aws_conn_id='aws_credential',
    	s3_bucket='us-stock-data',
    	s3_key='ConsumerGoods-{}.json',
    	execution_date='{{ ds }}',
    	path_to_data=stock_symbols.US_STOCK_INDUSTRY_CODES['Consumer Goods']['filename'],
    	)

    # Energy Industry Staging to S3
    energy_stock_symbols_to_s3 = StageJsonToS3(
    	task_id='Stage_Energy_StockSymbols_toS3',
    	aws_conn_id='aws_credential',
    	s3_bucket='us-stock-data',
    	s3_key='Energy-{}.json',
    	execution_date='{{ ds }}',
    	path_to_data=stock_symbols.US_STOCK_INDUSTRY_CODES['Energy']['filename'],
    	)

    # Financial Services Industry Staging to S3
    financial_services_stock_symbols_to_s3 = StageJsonToS3(
    	task_id='Stage_FinancialServices_StockSymbols_toS3',
    	aws_conn_id='aws_credential',
    	s3_bucket='us-stock-data',
    	s3_key='FinancialServices-{}.json',
    	execution_date='{{ ds }}',
    	path_to_data=stock_symbols.US_STOCK_INDUSTRY_CODES['Financial Services']['filename'],
    	)

    # Health Care/Life Sciences Industry Staging to S3
    healthcare_lifesciences_stock_symbols_to_s3 = StageJsonToS3(
    	task_id='Stage_HealthCare_LifeSciences_StockSymbols_toS3',
    	aws_conn_id='aws_credential',
    	s3_bucket='us-stock-data',
    	s3_key='HealthCare-LifeSciences-{}.json',
    	execution_date='{{ ds }}',
    	path_to_data=stock_symbols.US_STOCK_INDUSTRY_CODES['Health Care/Life Sciences']['filename'],
    	)

    # Industrial Goods Industry Staging to S3
    industrial_goods_stock_symbols_to_s3 = StageJsonToS3(
    	task_id='Stage_IndustrialGoods_StockSymbols_toS3',
    	aws_conn_id='aws_credential',
    	s3_bucket='us-stock-data',
    	s3_key='IndustrialGoods-{}.json',
    	execution_date='{{ ds }}',
    	path_to_data=stock_symbols.US_STOCK_INDUSTRY_CODES['Industrial Goods']['filename'],
    	)

    # Leisure/Arts/Hospitality Industry Staging to S3
    leisure_arts_hospitality_stock_symbols_to_s3 = StageJsonToS3(
    	task_id='Stage_Leisure_Arts_Hospitality_StockSymbols_toS3',
    	aws_conn_id='aws_credential',
    	s3_bucket='us-stock-data',
    	s3_key='Leisure-Arts-Hospitality-{}.json',
    	execution_date='{{ ds }}',
    	path_to_data=stock_symbols.US_STOCK_INDUSTRY_CODES['Leisure/Arts/Hospitality']['filename'],
    	)

    # Media/Entertainment Industry Staging to S3
    media_entertainment_stock_symbols_to_s3 = StageJsonToS3(
    	task_id='Stage_MediaEntertainment_StockSymbols_toS3',
    	aws_conn_id='aws_credential',
    	s3_bucket='us-stock-data',
    	s3_key='MediaEntertainment-{}.json',
    	execution_date='{{ ds }}',
    	path_to_data=stock_symbols.US_STOCK_INDUSTRY_CODES['Media/Entertainment']['filename'],
    	)

    # Real Estate/Construction Industry Staging to S3
    real_estate_construction_stock_symbols_to_s3 = StageJsonToS3(
    	task_id='Stage_RealEstate_Construction_StockSymbols_toS3',
    	aws_conn_id='aws_credential',
    	s3_bucket='us-stock-data',
    	s3_key='RealEstate-Construction-{}.json',
    	execution_date='{{ ds }}',
    	path_to_data=stock_symbols.US_STOCK_INDUSTRY_CODES['Real Estate/Construction']['filename'],
    	)  

    # Retail/Wholesale Industry Staging to S3
    retail_wholesale_stock_symbols_to_s3 = StageJsonToS3(
    	task_id='Stage_RetailWholesale_StockSymbols_toS3',
    	aws_conn_id='aws_credential',
    	s3_bucket='us-stock-data',
    	s3_key='RetailWholesale-{}.json',
    	execution_date='{{ ds }}',
    	path_to_data=stock_symbols.US_STOCK_INDUSTRY_CODES['Retail/Wholesale']['filename'],
    	)

    # Technology Industry Staging to S3
    technology_stock_symbols_to_s3 = StageJsonToS3(
    	task_id='Stage_Technology_StockSymbols_toS3',
    	aws_conn_id='aws_credential',
    	s3_bucket='us-stock-data',
    	s3_key='Technology-{}.json',
    	execution_date='{{ ds }}',
    	path_to_data=stock_symbols.US_STOCK_INDUSTRY_CODES['Technology']['filename'],
    	)

    # Telecommunication Services Industry Staging to S3
    telecommunication_srv_stock_symbols_to_s3 = StageJsonToS3(
    	task_id='Stage_TelecommunicationServices_StockSymbols_toS3',
    	aws_conn_id='aws_credential',
    	s3_bucket='us-stock-data',
    	s3_key='TelecommunicationServices-{}.json',
    	execution_date='{{ ds }}',
    	path_to_data=stock_symbols.US_STOCK_INDUSTRY_CODES['Telecommunication Services']['filename'],
    	)

    # Transportation/Logistics Industry Staging to S3
    transportation_logistics_stock_symbols_to_s3 = StageJsonToS3(
    	task_id='Stage_TransportationLogistics_StockSymbols_toS3',
    	aws_conn_id='aws_credential',
    	s3_bucket='us-stock-data',
    	s3_key='TransportationLogistics-{}.json',
    	execution_date='{{ ds }}',
    	path_to_data=stock_symbols.US_STOCK_INDUSTRY_CODES['Transportation/Logistics']['filename'],
    	)

    # Utilities Industry Staging to S3
    utilities_stock_symbols_to_s3 = StageJsonToS3(
    	task_id='Stage_Utilities_StockSymbols_toS3',
    	aws_conn_id='aws_credential',
    	s3_bucket='us-stock-data',
    	s3_key='Utilities-{}.json',
    	execution_date='{{ ds }}',
    	path_to_data=stock_symbols.US_STOCK_INDUSTRY_CODES['Utilities']['filename'],
    	)

    create_execution_date_s3_bucket = S3CreateBucket(
    	task_id='Create_S3Bucket',
    	aws_conn_id='aws_credential',
    	bucket_name='us-stock-data',
    	execution_date='{{ ds }}'
    	)

    check_inbound_files = PythonOperator(
    	task_id='Check_InboundFilesWritten',
    	python_callable=check_files_written
    	)    

    completion_operator = DummyOperator(
    	task_id="End_StockSymbolsLoad"
    	)

    start_operator >> automotive_stock_symbols_to_tmp >> check_inbound_files >>  create_execution_date_s3_bucket >> automotive_stock_symbols_to_s3 >> completion_operator
    start_operator >> agriculture_stock_symbols_to_tmp >> check_inbound_files >> create_execution_date_s3_bucket >> agriculture_stock_symbols_to_s3 >> completion_operator
    start_operator >> materials_resources_stock_symbols_to_tmp >> check_inbound_files >> create_execution_date_s3_bucket >> materials_resources_stock_symbols_to_s3 >> completion_operator
    start_operator >> business_consumer_srv_stock_symbols_to_tmp >> check_inbound_files >> create_execution_date_s3_bucket >> business_consumer_srv_stock_symbols_to_s3 >> completion_operator
    start_operator >> consumer_goods_stock_symbols_to_tmp >> check_inbound_files >> create_execution_date_s3_bucket >> consumer_goods_stock_symbols_to_s3 >> completion_operator
    start_operator >> energy_stock_symbols_to_tmp >> check_inbound_files >> create_execution_date_s3_bucket >> energy_stock_symbols_to_s3 >> completion_operator
    start_operator >> financial_srv_stock_symbols_to_tmp >> check_inbound_files >> create_execution_date_s3_bucket >> financial_services_stock_symbols_to_s3 >> completion_operator
    start_operator >> healthcare_lifesciences_stock_symbols_to_tmp >> check_inbound_files >> create_execution_date_s3_bucket >> healthcare_lifesciences_stock_symbols_to_s3 >> completion_operator
    start_operator >> industrial_goods_stock_symbols_to_tmp >> check_inbound_files >> create_execution_date_s3_bucket >> industrial_goods_stock_symbols_to_s3 >> completion_operator
    start_operator >> leisure_arts_hospitality_stock_symbols_to_tmp >> check_inbound_files >> create_execution_date_s3_bucket >> leisure_arts_hospitality_stock_symbols_to_s3 >> completion_operator
    start_operator >> media_entertainment_stock_symbols_to_tmp >> check_inbound_files >> create_execution_date_s3_bucket >> media_entertainment_stock_symbols_to_s3 >> completion_operator
    start_operator >> real_estate_construction_stock_symbols_to_tmp >> check_inbound_files >>create_execution_date_s3_bucket >> real_estate_construction_stock_symbols_to_s3 >> completion_operator
    start_operator >> retail_wholesale_stock_symbols_to_tmp >> check_inbound_files >> create_execution_date_s3_bucket >> retail_wholesale_stock_symbols_to_s3 >> completion_operator
    start_operator >> technology_stock_symbols_to_tmp >> check_inbound_files >> create_execution_date_s3_bucket >> technology_stock_symbols_to_s3 >> completion_operator
    start_operator >> telocommunication_srv_stock_symbols_to_tmp >> check_inbound_files >> create_execution_date_s3_bucket >> telecommunication_srv_stock_symbols_to_s3 >> completion_operator
    start_operator >> transportation_logistics_stock_symbols_to_tmp >> check_inbound_files >> create_execution_date_s3_bucket >> transportation_logistics_stock_symbols_to_s3 >> completion_operator
    start_operator >> utilities_stock_symbols_to_tmp >> check_inbound_files >> create_execution_date_s3_bucket >> utilities_stock_symbols_to_s3 >> completion_operator
