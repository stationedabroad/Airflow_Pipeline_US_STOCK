from datetime import datetime, timedelta
import logging
import os
import json

from airflow import DAG
from airflow.models  import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

from airflow.operators import TiingoPricePerIndustryHistorical
from airflow.operators import StageJsonToS3
from airflow.operators import S3CreateBucket
from airflow.operators import TargetS3StockSymbols, TargetS3EodLoad
from airflow.hooks.S3_hook import S3Hook

from helpers import StockSymbols

default_args = {
    'owner': 'Sulman M',
    'start_date': datetime(2019, 11, 10),
    'depends_on_past': False,
    'retries': 1,
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=1),
    'catchup_by_default': False ,
}

stock_symbols = StockSymbols()
# Load variables
load_from_date = Variable.get("start_load_date")
load_to_date = Variable.get("end_load_date")

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

    automotive_stock_symbols_to_s3 = StageJsonToS3(
        task_id='Stage_Automotive_StockSymbols_toS3',
        aws_conn_id='aws_credential',
        s3_bucket='us-stock-data-sm',
        s3_key='Automotive-{}.json',
        execution_date='{{ ds }}',
        path_to_data=stock_symbols.US_STOCK_INDUSTRY_CODES['Automotive']['filename'],
        )

    automotive_eod_price_to_s3 = TiingoPricePerIndustryHistorical(
        task_id='Fetch_HistoricalAutomotive_Prices_toS3',
        industry='Automotive',
        stock_symbols=stock_symbols.get_stock_symbols_for_industry('Automotive'),
        frequency='daily',
        h_start_date=load_from_date,
        h_end_date=load_to_date,
        path_to_write='plugins/output/tmp',
        aws_conn_id='aws_credential',
        s3_bucket='us-stock-data-sm',
        s3_key='Automotive-eod-{start}-to-{end}-{ds}.json',
        execution_date='{{ ds }}'
        )

    automotive_stock_symbols_s3_to_cassandra = TargetS3StockSymbols(
        task_id='Load_Automotive_stock_symbol_to_cassandra',
        aws_conn_id='aws_credential',
        s3_bucket='us-stock-data-sm',
        s3_key=stock_symbols.US_STOCK_INDUSTRY_CODES['Automotive']['s3_key_stock_symbols'],
        execution_date='{{ ds }}',
        cass_cluster=['127.0.0.1'],
        industry='Automotive'
        )      

    automotive_eod_s3_to_cassandra = TargetS3EodLoad(
        task_id='Load_Automotive_EOD_Prices_to_cassandra',
        aws_conn_id='aws_credential',
        s3_bucket='us-stock-data-sm',
        s3_key=stock_symbols.US_STOCK_INDUSTRY_CODES['Automotive']['s3_key_eod'],
        execution_date='{{ ds }}',
        cass_cluster=['127.0.0.1'],
        industry='Automotive',
        stock_symbol_s3key=stock_symbols.US_STOCK_INDUSTRY_CODES['Automotive']['s3_key_stock_symbols'],
        load_from=load_from_date,
        load_to=load_to_date
        )    

    # Agriculture Industry
    agriculture_stock_symbols_to_tmp = PythonOperator(
    	task_id="Fetch_Agriculture_StockSymbols_toTmp",
    	python_callable=write_stock_symbols_to_tmp,
    	op_kwargs={'industry': 'Agriculture'}
    	)

    agriculture_stock_symbols_to_s3 = StageJsonToS3(
        task_id='Stage_Agriculture_StockSymbols_toS3',
        aws_conn_id='aws_credential',
        s3_bucket='us-stock-data-sm',
        s3_key='Agriculture-{}.json',
        execution_date='{{ ds }}',
        path_to_data=stock_symbols.US_STOCK_INDUSTRY_CODES['Agriculture']['filename'],
        )

    agriculture_eod_price_to_s3 = TiingoPricePerIndustryHistorical(
        task_id='Fetch_HistoricalAgriculture_Prices_toS3',
        industry='Agriculture',
        stock_symbols=stock_symbols.get_stock_symbols_for_industry('Agriculture'),
        frequency='daily',
        h_start_date=load_from_date,
        h_end_date=load_to_date,
        path_to_write='plugins/output/tmp',
        aws_conn_id='aws_credential',
        s3_bucket='us-stock-data-sm',
        s3_key='Agriculture-eod-{start}-to-{end}-{ds}.json',
        execution_date='{{ ds }}'
        )

    agriculture_stock_symbols_s3_to_cassandra = TargetS3StockSymbols(
        task_id='Load_Agriculture_stock_symbol_to_cassandra',
        aws_conn_id='aws_credential',
        s3_bucket='us-stock-data-sm',
        s3_key=stock_symbols.US_STOCK_INDUSTRY_CODES['Agriculture']['s3_key_stock_symbols'],
        execution_date='{{ ds }}',
        cass_cluster=['127.0.0.1'],
        industry='Agriculture'
        )      

    agriculture_eod_s3_to_cassandra = TargetS3EodLoad(
        task_id='Load_Agriculture_EOD_Prices_to_cassandra',
        aws_conn_id='aws_credential',
        s3_bucket='us-stock-data-sm',
        s3_key=stock_symbols.US_STOCK_INDUSTRY_CODES['Agriculture']['s3_key_eod'],
        execution_date='{{ ds }}',
        cass_cluster=['127.0.0.1'],
        industry='Agriculture',
        stock_symbol_s3key=stock_symbols.US_STOCK_INDUSTRY_CODES['Agriculture']['s3_key_stock_symbols'],
        load_from=load_from_date,
        load_to=load_to_date
        ) 

    # Basic Materials/Resources Industry
    materials_resources_stock_symbols_to_tmp = PythonOperator(
    	task_id="Fetch_Materials_Resources_StockSymbols_toTmp",
    	python_callable=write_stock_symbols_to_tmp,
    	op_kwargs={'industry': 'Basic Materials/Resources'}
    	)

    materials_resources_stock_symbols_to_s3 = StageJsonToS3(
        task_id='Stage_MaterialsResources_StockSymbols_toS3',
        aws_conn_id='aws_credential',
        s3_bucket='us-stock-data-sm',
        s3_key='BasicMaterialsResources-{}.json',
        execution_date='{{ ds }}',
        path_to_data=stock_symbols.US_STOCK_INDUSTRY_CODES['Basic Materials/Resources']['filename'],
        )

    materials_resources_stock_eod_price_to_s3 = TiingoPricePerIndustryHistorical(
        task_id='Fetch_HistoricalMaterials_Resources_Prices_toS3',
        industry='BasicMaterials_Resources',
        stock_symbols=stock_symbols.get_stock_symbols_for_industry('Basic Materials/Resources'),
        frequency='daily',
        h_start_date=load_from_date,
        h_end_date=load_to_date,
        path_to_write='plugins/output/tmp',
        aws_conn_id='aws_credential',
        s3_bucket='us-stock-data-sm',
        s3_key='Materials-Resources-eod-{start}-to-{end}-{ds}.json',
        execution_date='{{ ds }}'
        )

    materials_resources_stock_symbols_s3_to_cassandra = TargetS3StockSymbols(
        task_id='Load_Materials_Resources_stock_symbol_to_cassandra',
        aws_conn_id='aws_credential',
        s3_bucket='us-stock-data-sm',
        s3_key=stock_symbols.US_STOCK_INDUSTRY_CODES['Basic Materials/Resources']['s3_key_stock_symbols'],
        execution_date='{{ ds }}',
        cass_cluster=['127.0.0.1'],
        industry='Basic Materials/Resources'
        )      

    materials_resources_eod_s3_to_cassandra = TargetS3EodLoad(
        task_id='Load_Materials_Resources_EOD_Prices_to_cassandra',
        aws_conn_id='aws_credential',
        s3_bucket='us-stock-data-sm',
        s3_key=stock_symbols.US_STOCK_INDUSTRY_CODES['Basic Materials/Resources']['s3_key_eod'],
        execution_date='{{ ds }}',
        cass_cluster=['127.0.0.1'],
        industry='Basic Materials/Resources',
        stock_symbol_s3key=stock_symbols.US_STOCK_INDUSTRY_CODES['Basic Materials/Resources']['s3_key_stock_symbols'],
        load_from=load_from_date,
        load_to=load_to_date
        )         


    # Business/Consumer Services Industry
    business_consumer_srv_stock_symbols_to_tmp = PythonOperator(
    	task_id="Fetch_Business_ConsumerSrv_StockSymbols_toTmp",
    	python_callable=write_stock_symbols_to_tmp,
    	op_kwargs={'industry': 'Business/Consumer Services'}
    	)

    business_consumer_srv_stock_symbols_to_s3 = StageJsonToS3(
        task_id='Stage_Business_ConsumerServices_StockSymbols_toS3',
        aws_conn_id='aws_credential',
        s3_bucket='us-stock-data-sm',
        s3_key='Business_ConsumerServices-{}.json',
        execution_date='{{ ds }}',
        path_to_data=stock_symbols.US_STOCK_INDUSTRY_CODES['Business/Consumer Services']['filename'],
        )

    business_consumer_srv_stock_eod_price_to_s3 = TiingoPricePerIndustryHistorical(
        task_id='Fetch_HistoricalBusiness_ConsumerServices_Prices_toS3',
        industry='Business_ConsumerServices',
        stock_symbols=stock_symbols.get_stock_symbols_for_industry('Business/Consumer Services'),
        frequency='daily',
        h_start_date=load_from_date,
        h_end_date=load_to_date,
        path_to_write='plugins/output/tmp',
        aws_conn_id='aws_credential',
        s3_bucket='us-stock-data-sm',
        s3_key='Business-ConsumerServices-eod-{start}-to-{end}-{ds}.json',
        execution_date='{{ ds }}'
        )

    business_consumer_srv_stock_symbols_s3_to_cassandra = TargetS3StockSymbols(
        task_id='Load_Materials_Resources_stock_symbol_to_cassandra',
        aws_conn_id='aws_credential',
        s3_bucket='us-stock-data-sm',
        s3_key=stock_symbols.US_STOCK_INDUSTRY_CODES['Basic Materials/Resources']['s3_key_stock_symbols'],
        execution_date='{{ ds }}',
        cass_cluster=['127.0.0.1'],
        industry='Basic Materials/Resources'
        )      

    business_consumer_srv_eod_s3_to_cassandra = TargetS3EodLoad(
        task_id='Load_Materials_Resources_EOD_Prices_to_cassandra',
        aws_conn_id='aws_credential',
        s3_bucket='us-stock-data-sm',
        s3_key=stock_symbols.US_STOCK_INDUSTRY_CODES['Basic Materials/Resources']['s3_key_eod'],
        execution_date='{{ ds }}',
        cass_cluster=['127.0.0.1'],
        industry='Basic Materials/Resources',
        stock_symbol_s3key=stock_symbols.US_STOCK_INDUSTRY_CODES['Basic Materials/Resources']['s3_key_stock_symbols'],
        load_from=load_from_date,
        load_to=load_to_date
        )              

    # Consumer Goods Industry
    consumer_goods_stock_symbols_to_tmp = PythonOperator(
    	task_id="Fetch_ConsumerGoods_StockSymbols_toTmp",
    	python_callable=write_stock_symbols_to_tmp,
    	op_kwargs={'industry': 'Consumer Goods'}
    	)

    consumer_goods_stock_symbols_to_s3 = StageJsonToS3(
        task_id='Stage_ConsumerGoods_StockSymbols_toS3',
        aws_conn_id='aws_credential',
        s3_bucket='us-stock-data-sm',
        s3_key='ConsumerGoods-{}.json',
        execution_date='{{ ds }}',
        path_to_data=stock_symbols.US_STOCK_INDUSTRY_CODES['Consumer Goods']['filename'],
        )

    consumer_goods_stock_eod_price_to_s3 = TiingoPricePerIndustryHistorical(
        task_id='Fetch_HistoricalConsumerGoods_Prices_toS3',
        industry='ConsumerGoods',
        stock_symbols=stock_symbols.get_stock_symbols_for_industry('Consumer Goods'),
        frequency='daily',
        h_start_date=load_from_date,
        h_end_date=load_to_date,
        path_to_write='plugins/output/tmp',
        aws_conn_id='aws_credential',
        s3_bucket='us-stock-data-sm',
        s3_key='ConsumerGoods-eod-{start}-to-{end}-{ds}.json',
        execution_date='{{ ds }}'
        )

    consumer_goods_stock_symbols_s3_to_cassandra = TargetS3StockSymbols(
        task_id='Load_ConsumerGoods_stock_symbol_to_cassandra',
        aws_conn_id='aws_credential',
        s3_bucket='us-stock-data-sm',
        s3_key=stock_symbols.US_STOCK_INDUSTRY_CODES['Consumer Goods']['s3_key_stock_symbols'],
        execution_date='{{ ds }}',
        cass_cluster=['127.0.0.1'],
        industry='Consumer Goods'
        )      

    consumer_goods_eod_s3_to_cassandra = TargetS3EodLoad(
        task_id='Load_ConsumerGoods_EOD_Prices_to_cassandra',
        aws_conn_id='aws_credential',
        s3_bucket='us-stock-data-sm',
        s3_key=stock_symbols.US_STOCK_INDUSTRY_CODES['Consumer Goods']['s3_key_eod'],
        execution_date='{{ ds }}',
        cass_cluster=['127.0.0.1'],
        industry='Consumer Goods',
        stock_symbol_s3key=stock_symbols.US_STOCK_INDUSTRY_CODES['Consumer Goods']['s3_key_stock_symbols'],
        load_from=load_from_date,
        load_to=load_to_date
        )           

    # Energy Industry
    energy_stock_symbols_to_tmp = PythonOperator(
    	task_id="Fetch_Energy_StockSymbols_toTmp",
    	python_callable=write_stock_symbols_to_tmp,
    	op_kwargs={'industry': 'Energy'}
    	)

    energy_stock_symbols_to_s3 = StageJsonToS3(
        task_id='Stage_Energy_StockSymbols_toS3',
        aws_conn_id='aws_credential',
        s3_bucket='us-stock-data-sm',
        s3_key='Energy-{}.json',
        execution_date='{{ ds }}',
        path_to_data=stock_symbols.US_STOCK_INDUSTRY_CODES['Energy']['filename'],
        )

    energy_stock_eod_price_to_s3 = TiingoPricePerIndustryHistorical(
        task_id='Fetch_HistoricalEnergy_Prices_toS3',
        industry='Energy',
        stock_symbols=stock_symbols.get_stock_symbols_for_industry('Energy'),
        frequency='daily',
        h_start_date=load_from_date,
        h_end_date=load_to_date,
        path_to_write='plugins/output/tmp',
        aws_conn_id='aws_credential',
        s3_bucket='us-stock-data-sm',
        s3_key='Energy-eod-{start}-to-{end}-{ds}.json',
        execution_date='{{ ds }}'
        )

    energy_stock_symbols_s3_to_cassandra = TargetS3StockSymbols(
        task_id='Load_Energy_stock_symbol_to_cassandra',
        aws_conn_id='aws_credential',
        s3_bucket='us-stock-data-sm',
        s3_key=stock_symbols.US_STOCK_INDUSTRY_CODES['Energy']['s3_key_stock_symbols'],
        execution_date='{{ ds }}',
        cass_cluster=['127.0.0.1'],
        industry='Energy'
        )      

    energy_eod_s3_to_cassandra = TargetS3EodLoad(
        task_id='Load_Energy_EOD_Prices_to_cassandra',
        aws_conn_id='aws_credential',
        s3_bucket='us-stock-data-sm',
        s3_key=stock_symbols.US_STOCK_INDUSTRY_CODES['Energy']['s3_key_eod'],
        execution_date='{{ ds }}',
        cass_cluster=['127.0.0.1'],
        industry='Energy',
        stock_symbol_s3key=stock_symbols.US_STOCK_INDUSTRY_CODES['Energy']['s3_key_stock_symbols'],
        load_from=load_from_date,
        load_to=load_to_date
        )           

    # Financial Services Industry
    financial_srv_stock_symbols_to_tmp = PythonOperator(
    	task_id="Fetch_FinancialServices_StockSymbols_toTmp",
    	python_callable=write_stock_symbols_to_tmp,
    	op_kwargs={'industry': 'Financial Services'}
    	)

    financial_srv_stock_symbols_to_s3 = StageJsonToS3(
        task_id='Stage_FinancialServices_StockSymbols_toS3',
        aws_conn_id='aws_credential',
        s3_bucket='us-stock-data-sm',
        s3_key='FinancialServices-{}.json',
        execution_date='{{ ds }}',
        path_to_data=stock_symbols.US_STOCK_INDUSTRY_CODES['Financial Services']['filename'],
        )

    financial_srv_stock_eod_price_to_s3 = TiingoPricePerIndustryHistorical(
        task_id='Fetch_HistoricalFinancialServices_Prices_toS3',
        industry='FinancialServices',
        stock_symbols=stock_symbols.get_stock_symbols_for_industry('Financial Services'),
        frequency='daily',
        h_start_date=load_from_date,
        h_end_date=load_to_date,
        path_to_write='plugins/output/tmp',
        aws_conn_id='aws_credential',
        s3_bucket='us-stock-data-sm',
        s3_key='FinancialServices-eod-{start}-to-{end}-{ds}.json',
        execution_date='{{ ds }}'
        )     

    financial_srv_stock_symbols_s3_to_cassandra = TargetS3StockSymbols(
        task_id='Load_FinancialServices_stock_symbol_to_cassandra',
        aws_conn_id='aws_credential',
        s3_bucket='us-stock-data-sm',
        s3_key=stock_symbols.US_STOCK_INDUSTRY_CODES['Financial Services']['s3_key_stock_symbols'],
        execution_date='{{ ds }}',
        cass_cluster=['127.0.0.1'],
        industry='Financial Services'
        )      

    financial_srv_eod_s3_to_cassandra = TargetS3EodLoad(
        task_id='Load_FinancialServices_EOD_Prices_to_cassandra',
        aws_conn_id='aws_credential',
        s3_bucket='us-stock-data-sm',
        s3_key=stock_symbols.US_STOCK_INDUSTRY_CODES['Financial Services']['s3_key_eod'],
        execution_date='{{ ds }}',
        cass_cluster=['127.0.0.1'],
        industry='Financial Services',
        stock_symbol_s3key=stock_symbols.US_STOCK_INDUSTRY_CODES['Financial Services']['s3_key_stock_symbols'],
        load_from=load_from_date,
        load_to=load_to_date
        )                     

    # Health-care/Life-sciences Industry
    healthcare_lifesciences_stock_symbols_to_tmp = PythonOperator(
    	task_id="Fetch_HealthCare_LifeSciences_StockSymbols_toTmp",
    	python_callable=write_stock_symbols_to_tmp,
    	op_kwargs={'industry': 'Health Care/Life Sciences'}
    	)  

    healthcare_lifesciences_stock_symbols_to_s3 = StageJsonToS3(
        task_id='Stage_HealthCare_LifeSciences_StockSymbols_toS3',
        aws_conn_id='aws_credential',
        s3_bucket='us-stock-data-sm',
        s3_key='HealthCare-LifeSciences-{}.json',
        execution_date='{{ ds }}',
        path_to_data=stock_symbols.US_STOCK_INDUSTRY_CODES['Health Care/Life Sciences']['filename'],
        )

    healthcare_lifesciences_stock_eod_price_to_s3 = TiingoPricePerIndustryHistorical(
        task_id='Fetch_HistoricalHealthCare_LifeSciences_Prices_toS3',
        industry='HealthCare_LifeSciences',
        stock_symbols=stock_symbols.get_stock_symbols_for_industry('Health Care/Life Sciences'),
        frequency='daily',
        h_start_date=load_from_date,
        h_end_date=load_to_date,
        path_to_write='plugins/output/tmp',
        aws_conn_id='aws_credential',
        s3_bucket='us-stock-data-sm',
        s3_key='HealthCare-LifeSciences-eod-{start}-to-{end}-{ds}.json',
        execution_date='{{ ds }}'
        )

    healthcare_lifesciences_stock_symbols_s3_to_cassandra = TargetS3StockSymbols(
        task_id='Load_HealthCare_LifeSciences_stock_symbol_to_cassandra',
        aws_conn_id='aws_credential',
        s3_bucket='us-stock-data-sm',
        s3_key=stock_symbols.US_STOCK_INDUSTRY_CODES['Health Care/Life Sciences']['s3_key_stock_symbols'],
        execution_date='{{ ds }}',
        cass_cluster=['127.0.0.1'],
        industry='HealthCare_LifeSciences'
        )      

    healthcare_lifesciences_eod_s3_to_cassandra = TargetS3EodLoad(
        task_id='Load_HealthCare_LifeSciences_EOD_Prices_to_cassandra',
        aws_conn_id='aws_credential',
        s3_bucket='us-stock-data-sm',
        s3_key=stock_symbols.US_STOCK_INDUSTRY_CODES['Health Care/Life Sciences']['s3_key_eod'],
        execution_date='{{ ds }}',
        cass_cluster=['127.0.0.1'],
        industry='HealthCare_LifeSciences',
        stock_symbol_s3key=stock_symbols.US_STOCK_INDUSTRY_CODES['Health Care/Life Sciences']['s3_key_stock_symbols'],
        load_from=load_from_date,
        load_to=load_to_date
        )


    # Industrial Goods Industry
    industrial_goods_stock_symbols_to_tmp = PythonOperator(
    	task_id="Fetch_IndustrialGoods_StockSymbols_toTmp",
    	python_callable=write_stock_symbols_to_tmp,
    	op_kwargs={'industry': 'Industrial Goods'}
    	)

    industrial_goods_stock_symbols_to_s3 = StageJsonToS3(
        task_id='Stage_IndustrialGoods_StockSymbols_toS3',
        aws_conn_id='aws_credential',
        s3_bucket='us-stock-data-sm',
        s3_key='IndustrialGoods-{}.json',
        execution_date='{{ ds }}',
        path_to_data=stock_symbols.US_STOCK_INDUSTRY_CODES['Industrial Goods']['filename'],
        )

    industrial_goods_stock_eod_price_to_s3 = TiingoPricePerIndustryHistorical(
        task_id='Fetch_HistoricalIndustrialGoods_Prices_toS3',
        industry='IndustrialGoods',
        stock_symbols=stock_symbols.get_stock_symbols_for_industry('Industrial Goods'),
        frequency='daily',
        h_start_date=load_from_date,
        h_end_date=load_to_date,
        path_to_write='plugins/output/tmp',
        aws_conn_id='aws_credential',
        s3_bucket='us-stock-data-sm',
        s3_key='IndustrialGoods-eod-{start}-to-{end}-{ds}.json',
        execution_date='{{ ds }}'
        )

    industrial_goods_stock_symbols_s3_to_cassandra = TargetS3StockSymbols(
        task_id='Load_IndustrialGoods_stock_symbol_to_cassandra',
        aws_conn_id='aws_credential',
        s3_bucket='us-stock-data-sm',
        s3_key=stock_symbols.US_STOCK_INDUSTRY_CODES['Industrial Goods']['s3_key_stock_symbols'],
        execution_date='{{ ds }}',
        cass_cluster=['127.0.0.1'],
        industry='Industrial Goods'
        )      

    industrial_goods_eod_s3_to_cassandra = TargetS3EodLoad(
        task_id='Load_IndustrialGoods_EOD_Prices_to_cassandra',
        aws_conn_id='aws_credential',
        s3_bucket='us-stock-data-sm',
        s3_key=stock_symbols.US_STOCK_INDUSTRY_CODES['Industrial Goods']['s3_key_eod'],
        execution_date='{{ ds }}',
        cass_cluster=['127.0.0.1'],
        industry='Industrial Goods',
        stock_symbol_s3key=stock_symbols.US_STOCK_INDUSTRY_CODES['Industrial Goods']['s3_key_stock_symbols'],
        load_from=load_from_date,
        load_to=load_to_date
        )         

    # Leisure/Arts/Hospitality Industry
    leisure_arts_hospitality_stock_symbols_to_tmp = PythonOperator(
    	task_id="Fetch_Leisure_Arts_Hospitality_StockSymbols_toTmp",
    	python_callable=write_stock_symbols_to_tmp,
    	op_kwargs={'industry': 'Leisure/Arts/Hospitality'}
    	)

    leisure_arts_hospitality_stock_symbols_to_s3 = StageJsonToS3(
        task_id='Stage_Leisure_Arts_Hospitality_StockSymbols_toS3',
        aws_conn_id='aws_credential',
        s3_bucket='us-stock-data-sm',
        s3_key='Leisure-Arts-Hospitality-{}.json',
        execution_date='{{ ds }}',
        path_to_data=stock_symbols.US_STOCK_INDUSTRY_CODES['Leisure/Arts/Hospitality']['filename'],
        )

    leisure_arts_hospitality_stock_eod_price_to_s3 = TiingoPricePerIndustryHistorical(
        task_id='Fetch_HistoricalLeisure_Arts_Hospitality_Prices_toS3',
        industry='Leisure_Arts_Hospitality',
        stock_symbols=stock_symbols.get_stock_symbols_for_industry('Leisure/Arts/Hospitality'),
        frequency='daily',
        h_start_date=load_from_date,
        h_end_date=load_to_date,
        path_to_write='plugins/output/tmp',
        aws_conn_id='aws_credential',
        s3_bucket='us-stock-data-sm',
        s3_key='Leisure-Arts-Hospitality-eod-{start}-to-{end}-{ds}.json',
        execution_date='{{ ds }}'
        )

    leisure_arts_hospitality_stock_symbols_s3_to_cassandra = TargetS3StockSymbols(
        task_id='Load_Leisure_Arts_Hospitality_stock_symbol_to_cassandra',
        aws_conn_id='aws_credential',
        s3_bucket='us-stock-data-sm',
        s3_key=stock_symbols.US_STOCK_INDUSTRY_CODES['Leisure/Arts/Hospitality']['s3_key_stock_symbols'],
        execution_date='{{ ds }}',
        cass_cluster=['127.0.0.1'],
        industry='Leisure/Arts/Hospitality'
        )      

    leisure_arts_hospitality_eod_s3_to_cassandra = TargetS3EodLoad(
        task_id='Load_Leisure_Arts_Hospitality_EOD_Prices_to_cassandra',
        aws_conn_id='aws_credential',
        s3_bucket='us-stock-data-sm',
        s3_key=stock_symbols.US_STOCK_INDUSTRY_CODES['Leisure/Arts/Hospitality']['s3_key_eod'],
        execution_date='{{ ds }}',
        cass_cluster=['127.0.0.1'],
        industry='Leisure/Arts/Hospitality',
        stock_symbol_s3key=stock_symbols.US_STOCK_INDUSTRY_CODES['Leisure/Arts/Hospitality']['s3_key_stock_symbols'],
        load_from=load_from_date,
        load_to=load_to_date
        )

    # Media/Entertainment Industry
    media_entertainment_stock_symbols_to_tmp = PythonOperator(
    	task_id="Fetch_Media_Entertainment_StockSymbols_toTmp",
    	python_callable=write_stock_symbols_to_tmp,
    	op_kwargs={'industry': 'Media/Entertainment'}
    	)

    media_entertainment_stock_symbols_to_s3 = StageJsonToS3(
        task_id='Stage_MediaEntertainment_StockSymbols_toS3',
        aws_conn_id='aws_credential',
        s3_bucket='us-stock-data-sm',
        s3_key='MediaEntertainment-{}.json',
        execution_date='{{ ds }}',
        path_to_data=stock_symbols.US_STOCK_INDUSTRY_CODES['Media/Entertainment']['filename'],
        )

    media_entertainment_stock_eod_price_to_s3 = TiingoPricePerIndustryHistorical(
        task_id='Fetch_HistoricalMedia_Entertainment_Prices_toS3',
        industry='Media_Entertainment',
        stock_symbols=stock_symbols.get_stock_symbols_for_industry('Media/Entertainment'),
        frequency='daily',
        h_start_date=load_from_date,
        h_end_date=load_to_date,
        path_to_write='plugins/output/tmp',
        aws_conn_id='aws_credential',
        s3_bucket='us-stock-data-sm',
        s3_key='Media-Entertainment-eod-{start}-to-{end}-{ds}.json',
        execution_date='{{ ds }}'
        )    

    media_entertainment_stock_symbols_s3_to_cassandra = TargetS3StockSymbols(
        task_id='Load_Media_Entertainment_stock_symbol_to_cassandra',
        aws_conn_id='aws_credential',
        s3_bucket='us-stock-data-sm',
        s3_key=stock_symbols.US_STOCK_INDUSTRY_CODES['Media/Entertainment']['s3_key_stock_symbols'],
        execution_date='{{ ds }}',
        cass_cluster=['127.0.0.1'],
        industry='Media/Entertainment'
        )      

    media_entertainment_eod_s3_to_cassandra = TargetS3EodLoad(
        task_id='Load_Media_Entertainment_EOD_Prices_to_cassandra',
        aws_conn_id='aws_credential',
        s3_bucket='us-stock-data-sm',
        s3_key=stock_symbols.US_STOCK_INDUSTRY_CODES['Media/Entertainment']['s3_key_eod'],
        execution_date='{{ ds }}',
        cass_cluster=['127.0.0.1'],
        industry='Media/Entertainment',
        stock_symbol_s3key=stock_symbols.US_STOCK_INDUSTRY_CODES['Media/Entertainment']['s3_key_stock_symbols'],
        load_from=load_from_date,
        load_to=load_to_date
        )

    # Real Estate/Construction Industry
    real_estate_construction_stock_symbols_to_tmp = PythonOperator(
    	task_id="Fetch_RealEstate_Construction_StockSymbols_toTmp",
    	python_callable=write_stock_symbols_to_tmp,
    	op_kwargs={'industry': 'Real Estate/Construction'}
    	)

    real_estate_construction_stock_symbols_to_s3 = StageJsonToS3(
        task_id='Stage_RealEstate_Construction_StockSymbols_toS3',
        aws_conn_id='aws_credential',
        s3_bucket='us-stock-data-sm',
        s3_key='RealEstate-Construction-{}.json',
        execution_date='{{ ds }}',
        path_to_data=stock_symbols.US_STOCK_INDUSTRY_CODES['Real Estate/Construction']['filename'],
        )

    real_estate_construction_stock_eod_price_to_s3 = TiingoPricePerIndustryHistorical(
        task_id='Fetch_HistoricalRealEstate_Construction_Prices_toS3',
        industry='RealEstate_Construction',
        stock_symbols=stock_symbols.get_stock_symbols_for_industry('Real Estate/Construction'),
        frequency='daily',
        h_start_date=load_from_date,
        h_end_date=load_to_date,
        path_to_write='plugins/output/tmp',
        aws_conn_id='aws_credential',
        s3_bucket='us-stock-data-sm',
        s3_key='RealEstate-Construction-eod-{start}-to-{end}-{ds}.json',
        execution_date='{{ ds }}'
        )

    real_estate_construction_stock_symbols_s3_to_cassandra = TargetS3StockSymbols(
        task_id='Load_RealEstate_Construction_stock_symbol_to_cassandra',
        aws_conn_id='aws_credential',
        s3_bucket='us-stock-data-sm',
        s3_key=stock_symbols.US_STOCK_INDUSTRY_CODES['Real Estate/Construction']['s3_key_stock_symbols'],
        execution_date='{{ ds }}',
        cass_cluster=['127.0.0.1'],
        industry='Real Estate/Construction'
        )      

    real_estate_construction_eod_s3_to_cassandra = TargetS3EodLoad(
        task_id='Load_RealEstate_Construction_EOD_Prices_to_cassandra',
        aws_conn_id='aws_credential',
        s3_bucket='us-stock-data-sm',
        s3_key=stock_symbols.US_STOCK_INDUSTRY_CODES['Real Estate/Construction']['s3_key_eod'],
        execution_date='{{ ds }}',
        cass_cluster=['127.0.0.1'],
        industry='Real Estate/Construction',
        stock_symbol_s3key=stock_symbols.US_STOCK_INDUSTRY_CODES['Real Estate/Construction']['s3_key_stock_symbols'],
        load_from=load_from_date,
        load_to=load_to_date
        )    

    # Retail/Wholesale Industry
    retail_wholesale_stock_symbols_to_tmp = PythonOperator(
    	task_id="Fetch_Retail_Wholesale_StockSymbols_toTmp",
    	python_callable=write_stock_symbols_to_tmp,
    	op_kwargs={'industry': 'Retail/Wholesale'}
    	)

    retail_wholesale_stock_symbols_to_s3 = StageJsonToS3(
        task_id='Stage_RetailWholesale_StockSymbols_toS3',
        aws_conn_id='aws_credential',
        s3_bucket='us-stock-data-sm',
        s3_key='RetailWholesale-{}.json',
        execution_date='{{ ds }}',
        path_to_data=stock_symbols.US_STOCK_INDUSTRY_CODES['Retail/Wholesale']['filename'],
        )

    retail_wholesale_stock_eod_price_to_s3 = TiingoPricePerIndustryHistorical(
        task_id='Fetch_HistoricalRetail_Wholesale_Prices_toS3',
        industry='Retail_Wholesale',
        stock_symbols=stock_symbols.get_stock_symbols_for_industry('Retail/Wholesale'),
        frequency='daily',
        h_start_date=load_from_date,
        h_end_date=load_to_date,
        path_to_write='plugins/output/tmp',
        aws_conn_id='aws_credential',
        s3_bucket='us-stock-data-sm',
        s3_key='Retail-Wholesale-eod-{start}-to-{end}-{ds}.json',
        execution_date='{{ ds }}'
        )

    retail_wholesale_stock_symbols_s3_to_cassandra = TargetS3StockSymbols(
        task_id='Load_Retail_Wholesale_stock_symbol_to_cassandra',
        aws_conn_id='aws_credential',
        s3_bucket='us-stock-data-sm',
        s3_key=stock_symbols.US_STOCK_INDUSTRY_CODES['Retail/Wholesale']['s3_key_stock_symbols'],
        execution_date='{{ ds }}',
        cass_cluster=['127.0.0.1'],
        industry='Retail/Wholesale'
        )      

    retail_wholesale_eod_s3_to_cassandra = TargetS3EodLoad(
        task_id='Load_Retail_Wholesale_EOD_Prices_to_cassandra',
        aws_conn_id='aws_credential',
        s3_bucket='us-stock-data-sm',
        s3_key=stock_symbols.US_STOCK_INDUSTRY_CODES['Retail/Wholesale']['s3_key_eod'],
        execution_date='{{ ds }}',
        cass_cluster=['127.0.0.1'],
        industry='Retail/Wholesale',
        stock_symbol_s3key=stock_symbols.US_STOCK_INDUSTRY_CODES['Retail/Wholesale']['s3_key_stock_symbols'],
        load_from=load_from_date,
        load_to=load_to_date
        )    

    # Technology Industry
    technology_stock_symbols_to_tmp = PythonOperator(
    	task_id="Fetch_Technology_StockSymbols_toTmp",
    	python_callable=write_stock_symbols_to_tmp,
    	op_kwargs={'industry': 'Technology'}
    	)

    technology_stock_symbols_to_s3 = StageJsonToS3(
        task_id='Stage_Technology_StockSymbols_toS3',
        aws_conn_id='aws_credential',
        s3_bucket='us-stock-data-sm',
        s3_key='Technology-{}.json',
        execution_date='{{ ds }}',
        path_to_data=stock_symbols.US_STOCK_INDUSTRY_CODES['Technology']['filename'],
        )

    technology_stock_eod_price_to_s3 = TiingoPricePerIndustryHistorical(
        task_id='Fetch_HistoricalTechnology_Prices_toS3',
        industry='Technology',
        stock_symbols=stock_symbols.get_stock_symbols_for_industry('Technology'),
        frequency='daily',
        h_start_date=load_from_date,
        h_end_date=load_to_date,
        path_to_write='plugins/output/tmp',
        aws_conn_id='aws_credential',
        s3_bucket='us-stock-data-sm',
        s3_key='Technology-eod-{start}-to-{end}-{ds}.json',
        execution_date='{{ ds }}'
        )

    technology_stock_symbols_s3_to_cassandra = TargetS3StockSymbols(
        task_id='Load_Technology_stock_symbol_to_cassandra',
        aws_conn_id='aws_credential',
        s3_bucket='us-stock-data-sm',
        s3_key=stock_symbols.US_STOCK_INDUSTRY_CODES['Technology']['s3_key_stock_symbols'],
        execution_date='{{ ds }}',
        cass_cluster=['127.0.0.1'],
        industry='Technology'
        )      

    technology_eod_s3_to_cassandra = TargetS3EodLoad(
        task_id='Load_Technology_EOD_Prices_to_cassandra',
        aws_conn_id='aws_credential',
        s3_bucket='us-stock-data-sm',
        s3_key=stock_symbols.US_STOCK_INDUSTRY_CODES['Technology']['s3_key_eod'],
        execution_date='{{ ds }}',
        cass_cluster=['127.0.0.1'],
        industry='Technology',
        stock_symbol_s3key=stock_symbols.US_STOCK_INDUSTRY_CODES['Technology']['s3_key_stock_symbols'],
        load_from=load_from_date,
        load_to=load_to_date
        )    

    # Telecommunication Services Industry
    telocommunication_srv_stock_symbols_to_tmp = PythonOperator(
    	task_id="Fetch_TelecommunicationServices_StockSymbols_toTmp",
    	python_callable=write_stock_symbols_to_tmp,
    	op_kwargs={'industry': 'Telecommunication Services'}
    	)

    telecommunication_srv_stock_symbols_to_s3 = StageJsonToS3(
        task_id='Stage_TelecommunicationServices_StockSymbols_toS3',
        aws_conn_id='aws_credential',
        s3_bucket='us-stock-data-sm',
        s3_key='TelecommunicationServices-{}.json',
        execution_date='{{ ds }}',
        path_to_data=stock_symbols.US_STOCK_INDUSTRY_CODES['Telecommunication Services']['filename'],
        )

    telecommunication_srv_stock_eod_price_to_s3 = TiingoPricePerIndustryHistorical(
        task_id='Fetch_HistoricalTelecommunicationServices_Prices_toS3',
        industry='TelecommunicationServices',
        stock_symbols=stock_symbols.get_stock_symbols_for_industry('Telecommunication Services'),
        frequency='daily',
        h_start_date=load_from_date,
        h_end_date=load_to_date,
        path_to_write='plugins/output/tmp',
        aws_conn_id='aws_credential',
        s3_bucket='us-stock-data-sm',
        s3_key='TelecommunicationServices-eod-{start}-to-{end}-{ds}.json',
        execution_date='{{ ds }}'
        )

    telecommunication_srv_stock_symbols_s3_to_cassandra = TargetS3StockSymbols(
        task_id='Load_TelecommunicationServices_stock_symbol_to_cassandra',
        aws_conn_id='aws_credential',
        s3_bucket='us-stock-data-sm',
        s3_key=stock_symbols.US_STOCK_INDUSTRY_CODES['Telecommunication Services']['s3_key_stock_symbols'],
        execution_date='{{ ds }}',
        cass_cluster=['127.0.0.1'],
        industry='Telecommunication Services'
        )      

    telecommunication_srv_eod_s3_to_cassandra = TargetS3EodLoad(
        task_id='Load_TelecommunicationServices_EOD_Prices_to_cassandra',
        aws_conn_id='aws_credential',
        s3_bucket='us-stock-data-sm',
        s3_key=stock_symbols.US_STOCK_INDUSTRY_CODES['Telecommunication Services']['s3_key_eod'],
        execution_date='{{ ds }}',
        cass_cluster=['127.0.0.1'],
        industry='Telecommunication Services',
        stock_symbol_s3key=stock_symbols.US_STOCK_INDUSTRY_CODES['Telecommunication Services']['s3_key_stock_symbols'],
        load_from=load_from_date,
        load_to=load_to_date
        )    

    # Transportation/Logistics Industry
    transportation_logistics_stock_symbols_to_tmp = PythonOperator(
    	task_id="Fetch_Transportation_Logistics_StockSymbols_toTmp",
    	python_callable=write_stock_symbols_to_tmp,
    	op_kwargs={'industry': 'Transportation/Logistics'}
    	)

    transportation_logistics_stock_symbols_to_s3 = StageJsonToS3(
        task_id='Stage_TransportationLogistics_StockSymbols_toS3',
        aws_conn_id='aws_credential',
        s3_bucket='us-stock-data-sm',
        s3_key='TransportationLogistics-{}.json',
        execution_date='{{ ds }}',
        path_to_data=stock_symbols.US_STOCK_INDUSTRY_CODES['Transportation/Logistics']['filename'],
        )    

    transportation_logistics_stock_eod_price_to_s3 = TiingoPricePerIndustryHistorical(
        task_id='Fetch_HistoricalTransportation_Logistics_Prices_toS3',
        industry='Transportation_Logistics',
        stock_symbols=stock_symbols.get_stock_symbols_for_industry('Transportation/Logistics'),
        frequency='daily',
        h_start_date=load_from_date,
        h_end_date=load_to_date,
        path_to_write='plugins/output/tmp',
        aws_conn_id='aws_credential',
        s3_bucket='us-stock-data-sm',
        s3_key='Transportation-Logistics-eod-{start}-to-{end}-{ds}.json',
        execution_date='{{ ds }}'
        )

    transportation_logistics_stock_symbols_s3_to_cassandra = TargetS3StockSymbols(
        task_id='Load_Transportation_Logistics_stock_symbol_to_cassandra',
        aws_conn_id='aws_credential',
        s3_bucket='us-stock-data-sm',
        s3_key=stock_symbols.US_STOCK_INDUSTRY_CODES['Transportation/Logistics']['s3_key_stock_symbols'],
        execution_date='{{ ds }}',
        cass_cluster=['127.0.0.1'],
        industry='Transportation/Logistics'
        )      

    transportation_logistics_eod_s3_to_cassandra = TargetS3EodLoad(
        task_id='Load_Transportation_Logistics_EOD_Prices_to_cassandra',
        aws_conn_id='aws_credential',
        s3_bucket='us-stock-data-sm',
        s3_key=stock_symbols.US_STOCK_INDUSTRY_CODES['Transportation/Logistics']['s3_key_eod'],
        execution_date='{{ ds }}',
        cass_cluster=['127.0.0.1'],
        industry='Transportation/Logistics',
        stock_symbol_s3key=stock_symbols.US_STOCK_INDUSTRY_CODES['Transportation/Logistics']['s3_key_stock_symbols'],
        load_from=load_from_date,
        load_to=load_to_date
        )    


    # Utilities Industry
    utilities_stock_symbols_to_tmp = PythonOperator(
    	task_id="Fetch_Utilities_StockSymbols_toTmp",
    	python_callable=write_stock_symbols_to_tmp,
    	op_kwargs={'industry': 'Utilities'}
    	)

    utilities_stock_symbols_to_s3 = StageJsonToS3(
    	task_id='Stage_Utilities_StockSymbols_toS3',
    	aws_conn_id='aws_credential',
    	s3_bucket='us-stock-data-sm',
    	s3_key='Utilities-{}.json',
    	execution_date='{{ ds }}',
    	path_to_data=stock_symbols.US_STOCK_INDUSTRY_CODES['Utilities']['filename'],
    	)

    utilities_stock_eod_price_to_s3 = TiingoPricePerIndustryHistorical(
        task_id='Fetch_HistoricalUtilities_Prices_toS3',
        industry='Utilities',
        stock_symbols=stock_symbols.get_stock_symbols_for_industry('Utilities'),
        frequency='daily',
        h_start_date=load_from_date,
        h_end_date=load_to_date,
        path_to_write='plugins/output/tmp',
        aws_conn_id='aws_credential',
        s3_bucket='us-stock-data-sm',
        s3_key='Utilities-eod-{start}-to-{end}-{ds}.json',
        execution_date='{{ ds }}'
        )

    utilities_stock_symbols_s3_to_cassandra = TargetS3StockSymbols(
        task_id='Load_Utilities_stock_symbol_to_cassandra',
        aws_conn_id='aws_credential',
        s3_bucket='us-stock-data-sm',
        s3_key=stock_symbols.US_STOCK_INDUSTRY_CODES['Utilities']['s3_key_stock_symbols'],
        execution_date='{{ ds }}',
        cass_cluster=['127.0.0.1'],
        industry='Utilities'
        )      

    utilities_eod_s3_to_cassandra = TargetS3EodLoad(
        task_id='Load_Utilities_EOD_Prices_to_cassandra',
        aws_conn_id='aws_credential',
        s3_bucket='us-stock-data-sm',
        s3_key=stock_symbols.US_STOCK_INDUSTRY_CODES['Utilities']['s3_key_eod'],
        execution_date='{{ ds }}',
        cass_cluster=['127.0.0.1'],
        industry='Utilities',
        stock_symbol_s3key=stock_symbols.US_STOCK_INDUSTRY_CODES['Utilities']['s3_key_stock_symbols'],
        load_from=load_from_date,
        load_to=load_to_date
        )         

    create_execution_date_s3_bucket = S3CreateBucket(
    	task_id='Create_S3Bucket',
    	aws_conn_id='aws_credential',
    	bucket_name='us-stock-data-sm',
    	execution_date='{{ ds }}'
    	)

    check_inbound_files = PythonOperator(
    	task_id='Check_InboundFilesWritten',
    	python_callable=check_files_written
    	)    

    completion_operator = DummyOperator(
    	task_id="End_StockSymbolsLoad"
    	)

    start_eod_prices_operator = DummyOperator(
    	task_id="Begin_StockPricesLoad"
    	)   

    completion_eod_prices_operator = DummyOperator(
    	task_id="End_StockPricesLoad"
    	)
# Analytic Load -> S3 to Cassandra via Spark
    start_analytic_target_loads = DummyOperator(
        task_id="Start_Analytic_Target_Loads"
        )    

    completion_analytic_target_loads = DummyOperator(
        task_id="End_Analytic_Target_Loads"
        )    


    # Staging of Stock Symbols Web Scrape to AWS S3
    start_operator >> [automotive_stock_symbols_to_tmp,
                       agriculture_stock_symbols_to_tmp,
                       materials_resources_stock_symbols_to_tmp,
                       business_consumer_srv_stock_symbols_to_tmp,
                       consumer_goods_stock_symbols_to_tmp,
                       energy_stock_symbols_to_tmp,
                       financial_srv_stock_symbols_to_tmp,
                       healthcare_lifesciences_stock_symbols_to_tmp,
                       industrial_goods_stock_symbols_to_tmp,
                       leisure_arts_hospitality_stock_symbols_to_tmp,
                       media_entertainment_stock_symbols_to_tmp,
                       real_estate_construction_stock_symbols_to_tmp,
                       retail_wholesale_stock_symbols_to_tmp,
                       technology_stock_symbols_to_tmp,
                       telocommunication_srv_stock_symbols_to_tmp,
                       transportation_logistics_stock_symbols_to_tmp,
                       utilities_stock_symbols_to_tmp] >> check_inbound_files >> create_execution_date_s3_bucket >> \
                       [automotive_stock_symbols_to_s3,
                        agriculture_stock_symbols_to_s3,
                        materials_resources_stock_symbols_to_s3,
                        business_consumer_srv_stock_symbols_to_s3,
                        consumer_goods_stock_symbols_to_s3,
                        energy_stock_symbols_to_s3,
                        financial_srv_stock_symbols_to_s3,
                        healthcare_lifesciences_stock_symbols_to_s3,
                        industrial_goods_stock_symbols_to_s3,
                        leisure_arts_hospitality_stock_symbols_to_s3,
                        media_entertainment_stock_symbols_to_s3,
                        real_estate_construction_stock_symbols_to_s3,
                        retail_wholesale_stock_symbols_to_s3,
                        technology_stock_symbols_to_s3,
                        telecommunication_srv_stock_symbols_to_s3,
                        transportation_logistics_stock_symbols_to_s3,
                        utilities_stock_symbols_to_s3] >> completion_operator >> start_eod_prices_operator >> \
                        [automotive_eod_price_to_s3,
                        agriculture_eod_price_to_s3,
                        materials_resources_stock_eod_price_to_s3,
                        business_consumer_srv_stock_eod_price_to_s3,
                        consumer_goods_stock_eod_price_to_s3,
                        energy_stock_eod_price_to_s3,
                        financial_srv_stock_eod_price_to_s3,
                        healthcare_lifesciences_stock_eod_price_to_s3,
                        industrial_goods_stock_eod_price_to_s3,
                        leisure_arts_hospitality_stock_eod_price_to_s3,
                        media_entertainment_stock_eod_price_to_s3,
                        real_estate_construction_stock_eod_price_to_s3,
                        retail_wholesale_stock_eod_price_to_s3,
                        technology_stock_eod_price_to_s3,
                        telecommunication_srv_stock_eod_price_to_s3,
                        transportation_logistics_stock_eod_price_to_s3,
                        utilities_stock_eod_price_to_s3] >> completion_eod_prices_operator 
    completion_eod_prices_operator >>  start_analytic_target_loads >> automotive_stock_symbols_s3_to_cassandra >> automotive_eod_s3_to_cassandra >> completion_analytic_target_loads
    completion_eod_prices_operator >> start_analytic_target_loads >> agriculture_stock_symbols_s3_to_cassandra >> agriculture_eod_s3_to_cassandra >> completion_analytic_target_loads
    completion_eod_prices_operator >> start_analytic_target_loads >> materials_resources_stock_symbols_s3_to_cassandra >> materials_resources_eod_s3_to_cassandra >> completion_analytic_target_loads
    completion_eod_prices_operator >>  start_analytic_target_loads >> business_consumer_srv_stock_symbols_s3_to_cassandra >> business_consumer_srv_eod_s3_to_cassandra >> completion_analytic_target_loads
    completion_eod_prices_operator >>  start_analytic_target_loads >> consumer_goods_stock_symbols_s3_to_cassandra >> consumer_goods_eod_s3_to_cassandra >> completion_analytic_target_loads
    completion_eod_prices_operator >> start_analytic_target_loads >> energy_stock_symbols_s3_to_cassandra >> energy_eod_s3_to_cassandra >> completion_analytic_target_loads
    completion_eod_prices_operator >>  start_analytic_target_loads >> financial_srv_stock_symbols_s3_to_cassandra >> financial_srv_eod_s3_to_cassandra >> completion_analytic_target_loads
    completion_eod_prices_operator >> start_analytic_target_loads >> healthcare_lifesciences_stock_symbols_s3_to_cassandra >> healthcare_lifesciences_eod_s3_to_cassandra >> completion_analytic_target_loads
    completion_eod_prices_operator >> start_analytic_target_loads >> industrial_goods_stock_symbols_s3_to_cassandra >> industrial_goods_eod_s3_to_cassandra >> completion_analytic_target_loads
    completion_eod_prices_operator >> start_analytic_target_loads >> leisure_arts_hospitality_stock_symbols_s3_to_cassandra >> leisure_arts_hospitality_eod_s3_to_cassandra >> completion_analytic_target_loads
    completion_eod_prices_operator >> start_analytic_target_loads >> media_entertainment_stock_symbols_s3_to_cassandra >> media_entertainment_eod_s3_to_cassandra >> completion_analytic_target_loads
    completion_eod_prices_operator >> start_analytic_target_loads >> real_estate_construction_stock_symbols_s3_to_cassandra >> real_estate_construction_eod_s3_to_cassandra >> completion_analytic_target_loads
    completion_eod_prices_operator >> start_analytic_target_loads >> retail_wholesale_stock_symbols_s3_to_cassandra >> retail_wholesale_eod_s3_to_cassandra >> completion_analytic_target_loads
    completion_eod_prices_operator >> start_analytic_target_loads >> technology_stock_symbols_s3_to_cassandra >> technology_eod_s3_to_cassandra >> completion_analytic_target_loads
    completion_eod_prices_operator >> start_analytic_target_loads >> telecommunication_srv_stock_symbols_s3_to_cassandra >> telecommunication_srv_eod_s3_to_cassandra >> completion_analytic_target_loads
    completion_eod_prices_operator >> start_analytic_target_loads >> transportation_logistics_stock_symbols_s3_to_cassandra >> transportation_logistics_eod_s3_to_cassandra >> completion_analytic_target_loads
    completion_eod_prices_operator >> start_analytic_target_loads >> utilities_logistics_stock_symbols_s3_to_cassandra >> utilities_logistics_eod_s3_to_cassandra >> completion_analytic_target_loads