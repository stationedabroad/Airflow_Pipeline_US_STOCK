from datetime import datetime, timedelta
import logging
import os

from airflow import DAG
from airflow.models  import Variable
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators import StageJsonToS3
from airflow.operators import S3CreateBucket
from airflow.operators import TiingoPricePerIndustryHistorical
from airflow.operators import TargetS3StockSymbols, TargetS3EodLoad
from airflow.hooks.S3_hook import S3Hook

from helpers import StockSymbols

from pyspark.sql import SparkSession
from cassandra.cluster import Cluster

default_args = {
    'owner': 'Sulman M',
    'start_date': datetime(2019, 11, 15),
    'depends_on_past': False,
    'retries': 0,
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=5),
    # 'catchup_by_default': True,
}

stock_symbols = StockSymbols()
load_from_date = Variable.get("start_load_date")
load_to_date = Variable.get("end_load_date")

def get_params(*args, **kwargs):
    ds = kwargs.get('ds')
    logging.info(f'ARGS {kwargs}')
    logging.info(f'Execution date {ds}')
    main_date = {ds}

def create_bucket(*args, **kwargs):
    logging.info(f'Creating NEW S3 bucket')
    ed = kwargs['execution_date'].date()
    bname = f"airflow-usstock-{ed}"
    s3_hook = S3Hook(aws_conn_id='aws_credential')
    logging.info(f'MY BUCKET NAME {bname}')
    s3_hook.create_bucket(bucket_name=bname)

def get_cred(*args, **kwargs):
    s3_hook = S3Hook(aws_conn_id='aws_credential')
    a = s3_hook.get_credentials()
    logging.info(f'{a[0]}, {a[1]}')

def read_s3_with_spark(*args, **kwargs):
    print('Called ...')
    # access_key = ''
    # secret_key = ''
    # spark = SparkSession.builder.appName('cassy').getOrCreate()
    # sc=spark.sparkContext
    # hadoop_conf = sc._jsc.hadoopConfiguration()
    # hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    # hadoop_conf.set("fs.s3.awsAccessKeyId", access_key)
    # hadoop_conf.set("fs.s3.awsSecretAccessKey", secret_key)
    # df_agr = spark.read.json("s3://us-stock-data-sm-2019-11-10/Agriculture-2019-11-10.json")
    # logging.info(f'Spark called S3 --- {df_agr.count()}')

def test_cassandra(*args, **kwargs):
    access_key = ''
    secret_key = ''
    spark = SparkSession.builder.appName('cassy').getOrCreate()
    sc=spark.sparkContext
    hadoop_conf = sc._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
    hadoop_conf.set("fs.s3.awsAccessKeyId", access_key)
    hadoop_conf.set("fs.s3.awsSecretAccessKey", secret_key)

    df_agr = spark.read.json("s3://us-stock-data-sm-2019-11-10/FinancialServices-2019-11-10.json")

    cluster = Cluster(['127.0.0.1'])
    session = cluster.connect()
    query = "insert into us_stock.stock_symbols_us(ticker, description) values(%s, %s)"
    
    for row in df_agr.toPandas().values:
        session.execute(query, (row[1], row[0]))

load_from_date = Variable.get("start_load_date")
load_to_date = Variable.get("end_load_date")

def write_stock_symbols_to_tmp(industry=None):
    if not industry:
        logging.info(f'No industry provided {industry}')
        return True
    logging.info(f'Writing tmp {industry}')
    filenm, written_records = stock_symbols.write_stock_symbols_for_industry(industry)
    logging.info(f'Written {written_records} records to {filenm}')        
    

with DAG('S3_connect_DAG', schedule_interval='@once', catchup=True, default_args=default_args) as dag:
    # Task 1 - Begin
    start_operator = DummyOperator(
        task_id="Begin_createBucketS3"
        )

    # params = PythonOperator(
    #     task_id="get_params",
    #     python_callable=get_params,
    #     provide_context=True
    #     )

    # get_credentials = PythonOperator(
    #     task_id="get_credentials",
    #     python_callable=get_cred
    #     )

    # Automotive Industry
    automotive_stock_symbols_to_tmp = PythonOperator(
        task_id="Fetch_Automotive_StockSymbols_toTmp",
        python_callable=write_stock_symbols_to_tmp,
        op_kwargs={'industry': 'Automotive'}
        )

    create_execution_date_s3_bucket = S3CreateBucket(
        task_id='Create_S3Bucket',
        aws_conn_id='aws_credential',
        bucket_name='us-stock-data-sm',
        execution_date='{{ ds }}'
        )    

    # # Automotive Industry Staging to S3
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

#########################3
    automotive_stock_symbols_s3_to_cassandra = TargetS3StockSymbols(
        task_id='Load_Automotive_stock_symbol_to_cassandra',
        aws_conn_id='aws_credential',
        s3_bucket='us-stock-data-sm',
        s3_key=stock_symbols.US_STOCK_INDUSTRY_CODES['Automotive']['s3_key_stock_symbols'],
        execution_date='{{ ds }}',
        cass_cluster=['127.0.0.1'],
        industry='Automotive'
        )    

# End Of Day Loads
    automotive_eod_s3_to_cassandra = TargetS3EodLoad(
        task_id='Load_Automotive_EOD_Prices',
        aws_conn_id='aws_credential',
        s3_bucket='us-stock-data-sm',
        s3_key='Automotive-eod-{start}-to-{end}-{ds}.json',
        execution_date='{{ ds }}',
        cass_cluster=['127.0.0.1'],
        industry='Automotive',
        stock_symbol_s3key=stock_symbols.US_STOCK_INDUSTRY_CODES['Automotive']['s3_key_stock_symbols'],
        load_from=load_from_date,
        load_to=load_to_date
        )

    start_operator >> automotive_stock_symbols_to_tmp >> create_execution_date_s3_bucket >> \
    automotive_stock_symbols_to_s3 >> automotive_eod_price_to_s3 >> \
    automotive_stock_symbols_s3_to_cassandra  >> automotive_eod_s3_to_cassandra