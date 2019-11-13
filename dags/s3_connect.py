from datetime import datetime, timedelta
import logging
import os

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators import StageJsonToS3
from airflow.operators import TiingoPricePerIndustryHistorical
from airflow.operators import TargetS3StockSymbols
from airflow.hooks.S3_hook import S3Hook

from helpers import StockSymbols

from pyspark.sql import SparkSession
from cassandra.cluster import Cluster

default_args = {
    'owner': 'Sulman M',
    'start_date': datetime(2019, 11, 10),
    'depends_on_past': False,
    'retries': 0,
    'email_on_retry': False,
    'retry_delay': timedelta(minutes=5),
    'catchup_by_default': False,
}

stock_symbols = StockSymbols()

def create_bucket(*args, **kwargs):
    logging.info(f'Creating NEW S3 bucket')
    ed = kwargs['execution_date'].date()
    bname = f"airflow-usstock-{ed}"
    s3_hook = S3Hook(aws_conn_id='aws_credential')
    logging.info(f'MY BUCKET NAME {bname}')
    s3_hook.create_bucket(bucket_name=bname)

def read_s3_with_spark(*args, **kwargs):
    print('Called ...')
    # access_key = 'AKIAXCBZH7EEV3TH25N2'
    # secret_key = '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'
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
    

with DAG('S3_connect_DAG', schedule_interval=None, default_args=default_args) as dag:
    # Task 1 - Begin
    start_operator = DummyOperator(
        task_id="Begin_createBucketS3"
        )

    # historical_automotive = TiingoPricePerIndustryHistorical(
    #     task_id='Fetch_HistoricalAutomotivePrices',
    #     industry='Automotive',
    #     stock_symbols=stock_symbols.get_stock_symbols_for_industry('Automotive'),
    #     frequency='daily',
    #     h_start_date='2018-11-9',
    #     h_end_date='2019-11-10',
    #     path_to_write='plugins/output/tmp',
    #     aws_conn_id='aws_credential',
    #     s3_bucket='us-stock-data',
    #     s3_key='Automotive-eod-{start}-to-{end}-{ds}.json',
    #     execution_date='{{ ds }}'
    #     )

    # spark_to_s3 = PythonOperator(
    #     task_id="call_s3_with_spark",
    #     python_callable=read_s3_with_spark,
    #     provide_context=True
    #     )

    # cassadnra_test = PythonOperator(
    #     task_id="call_to_local_host_cassandra",
    #     python_callable=test_cassandra,
    #     provide_context=True
    #     )

    s3_spark_to_cassandra = TargetS3StockSymbols(
        task_id='stock_symbol_financial_services_to_cassandra',
        aws_conn_id='aws_credential',
        s3_bucket='us-stock-data-sm-2019-11-10',
        s3_key='FinancialServices-{}.json'.format('2019-11-10'),
        execution_date='{{ ds }}',
        cass_cluster=['127.0.0.1'],
        industry='Financial Services'
        )

    start_operator >> s3_spark_to_cassandra
