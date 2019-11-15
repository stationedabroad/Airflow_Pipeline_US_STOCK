from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.S3_hook import S3Hook

import logging
 
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import dayofweek, dayofmonth, dayofyear, date_format, weekofyear, hour, minute




class TargetDBWrite(BaseOperator):
	template_fields = ['execution_date']

	S3_PREFIX = r's3://'

	@apply_defaults
	def __init__(self, 
				 aws_conn_id, 
				 s3_bucket, 
				 s3_key,
				 execution_date, 
				 cass_cluster,
				 *args, **kwargs):

		super(TargetDBWrite, self).__init__(*args, **kwargs)

		self.aws_conn_id = aws_conn_id
		self.s3_bucket = s3_bucket
		self.s3_key = s3_key
		self.execution_date = execution_date
		self.s3_hook = S3Hook(self.aws_conn_id)
		aws_session = self.s3_hook.get_credentials()
		self.spark = SparkSession.builder.appName('s3_to_cassandra').getOrCreate()
		self.sc = self.spark.sparkContext
		hadoop_conf = self.sc._jsc.hadoopConfiguration()
		hadoop_conf.set("fs.s3.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
		# logging.info(f'CREDENTIALS : {aws_session}')
		hadoop_conf.set("fs.s3.awsAccessKeyId", aws_session[0])
		hadoop_conf.set("fs.s3.awsSecretAccessKey", aws_session[1])
		cluster = Cluster(cass_cluster)
		self.session = cluster.connect()	    

	def execute(self, context):
		pass


class TargetS3StockSymbols(TargetDBWrite):
	template_fields = ['execution_date']

	SQL_INSERT = """
		insert into us_stock.stock_symbols (ticker, description, industry) 
		values (%s, %s, %s)
	"""

	@apply_defaults
	def __init__(self,
				 aws_conn_id, 
				 s3_bucket, 
				 s3_key,
				 execution_date, 
				 cass_cluster,
				 industry,
				 *args, **kwargs):

		super(TargetS3StockSymbols, self).__init__(aws_conn_id=aws_conn_id,
			                                       s3_bucket=s3_bucket,
			                                       s3_key=s3_key,
			                                       cass_cluster=cass_cluster,
			                                       execution_date=execution_date,
			                                       *args, **kwargs)

		self.industry = industry
		
	def execute(self, context):
		key_to_write = self.s3_key.format(self.execution_date)
		bucket_to_write = f'{self.s3_bucket}-{self.execution_date}'
		s3_write_path = f'{TargetDBWrite.S3_PREFIX}{bucket_to_write}/{key_to_write}'
		#Get S3 data
		logging.info(f'Reading from S3 bucket {bucket_to_write} key {key_to_write}')
		s3_df = self.spark.read.json(s3_write_path)
		if s3_df:
			df_pd = s3_df.toPandas()
			logging.info(f'Shape of dataframe for stock symbols {self.industry} - {df_pd.shape}')
			written_records = 0
			for desc, industry, ticker in df_pd.values:
				self.session.execute(TargetS3StockSymbols.SQL_INSERT, (ticker, desc, industry))
				written_records += 1
			logging.info(f'Written {written_records} to cassandra cluster {self.session.hosts}')



class TargetS3EodLoad(TargetDBWrite):
	template_fields = ('execution_date',)

	SQL_INSERT_EOD = """
		insert into us_stock.eod_stock_price (date, ticker, adj_close, adj_high, adj_low, adj_open, adj_volume, close, company_name, 
		                                      dayofmonth, dayofweek, dayofyear, div_cash, high, hour, low, minute, open, split_factor,
		                                      ts, volume, weekofyear) 
		values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
	"""

	@apply_defaults
	def __init__(self,
				 aws_conn_id, 
				 s3_bucket, 
				 s3_key,
				 execution_date, 
				 cass_cluster,
				 industry,
				 stock_symbol_s3key,
				 load_from,
				 load_to,
				 *args, **kwargs):

		super(TargetS3EodLoad, self).__init__(aws_conn_id=aws_conn_id,
			                                       s3_bucket=s3_bucket,
			                                       s3_key=s3_key,
			                                       cass_cluster=cass_cluster,
			                                       execution_date=execution_date,
			                                       *args, **kwargs)

		self.industry = industry
		# self.s3_read_path_eod = f'{TargetDBWrite.S3_PREFIX}{self.s3_bucket}-{self.execution_date}/{self.s3_key}'
		self.stock_symbol_s3key = stock_symbol_s3key
		self.load_from = load_from
		self.load_to = load_to
		# self.s3_read_path_stock_symbols = f'{TargetDBWrite.S3_PREFIX}{self.s3_bucket}-{self.execution_date}/{self.stock_symbol_s3key}'

	def execute(self, context):
		# EOD prices S3 key
		self.s3_key = self.s3_key.format(start=self.load_from, end=self.load_to, ds=self.execution_date)
		self.s3_read_path_eod = f'{TargetDBWrite.S3_PREFIX}{self.s3_bucket}-{self.execution_date}/{self.s3_key}'
		# Stock Symbols S3 key
		self.stock_symbol_s3key = self.stock_symbol_s3key.format(format(self.execution_date))
		self.s3_read_path_stock_symbols = f'{TargetDBWrite.S3_PREFIX}{self.s3_bucket}-{self.execution_date}/{self.stock_symbol_s3key}'
		
		#Get S3 data
		logging.info(f'Reading EOD prices from S3 bucket/key {self.s3_read_path_eod}')
		df_s3_eod = self.spark.read.json(self.s3_read_path_eod)
		logging.info(f'Reading Stock Symbols from bucket/key {self.s3_read_path_stock_symbols}')
		df_s3_ss = self.spark.read.json(self.s3_read_path_stock_symbols)
		if df_s3_eod and df_s3_ss:
			df_tmp_join = df_s3_eod.join(df_s3_ss, df_s3_eod.ticker == df_s3_ss.symbol_code, 'inner') \
								   .alias('tmp_table') \
								   .select('tmp_table.*') \
								   .withColumn('i_date', date_format('date', 'dd/MM/yyyy')) \
   								   .withColumn('hour', hour('date')) \
   								   .withColumn('minute', minute('date')) \
                                   .withColumn('dayOfMonth', dayofmonth('date')) \
                                   .withColumn('dayOfWeek', dayofweek('date')) \
                                   .withColumn('dayOfYear', dayofyear('date')) \
                                   .withColumn('weekOfYear', weekofyear('date'))
			# logging.info(f'Shape of dataframe for EOD transformed prices, industry: {self.industry} - {df_tmp_join.shape}')
			# logging.info(f'COLUMNS - {df_tmp_join.columns}')
			written_records = 0
			for row in df_tmp_join.rdd.collect():
				self.session.execute(TargetS3EodLoad.SQL_INSERT_EOD, (row['i_date'], 
					                                                  row['ticker'], \
					                                                  row['adjClose'], \
					                                                  row['adjHigh'], \
					                                                  row['adjLow'], \
					                                                  row['adjOpen'], \
					                                                  row['adjVolume'], \
					                                                  row['close'], \
					                                                  row['company_name'], \
					                                                  row['dayOfMonth'], \
					                                                  row['dayOfWeek'], \
					                                                  row['dayOfYear'], \
					                                                  row['divCash'], \
																	  row['high'], \
																	  row['hour'], \
																	  row['low'], \
																	  row['minute'], \
																	  row['open'], \
																	  row['splitFactor'], \
																	  row['date'], \
																	  row['volume'], \
																	  row['weekOfYear']))
			# for row in df_tmp_join.values:
				# print(f'{row[16]}--- {row[12]}--- {row[0]}--- {row[1]}--- {row[2]}--- {row[3]}--- {row[4]}--- {row[5]}--- {row[14]}--- {row[19]}--- {row[20]}--- \
				# 	                                                         {row[21]}--- {row[7]}--- {row[8]}--- {row[17]}--- {row[9]}--- {row[18]}--- {row[10]}--- {row[11]}--- {row[6]}--- {row[13]}--- {row[22]}')
				# print(f'COLUMNS: {df_tmp_join.columns}')
				# break					                                                         
				# self.session.execute(TargetS3EodLoad.SQL_INSERT_EOD, (row[16], row[12], row[0], row[1], row[2], row[3], row[4], row[5], row[14], row[19], row[20], \
				# 	                                                         row[21], row[7], row[8], row[17], row[9], row[18], row[10], row[11], row[6], row[13], row[22]))
				written_records += 1
			logging.info(f'Written {written_records} to cassandra cluster {self.session.hosts} table')		