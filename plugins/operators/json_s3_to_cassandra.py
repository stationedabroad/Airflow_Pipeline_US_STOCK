from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.S3_hook import S3Hook

import logging
 
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession



class TargetDBWrite(BaseOperator):
	template_fields = ('execution_date',)

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
		logging.info(f'CREDENTIALS : {aws_session}')
		hadoop_conf.set("fs.s3.awsAccessKeyId", "AKIAXCBZH7EEV3TH25N2")
		hadoop_conf.set("fs.s3.awsSecretAccessKey", "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")
		cluster = Cluster(cass_cluster)
		self.session = cluster.connect()	    

	def execute(self, context):
		pass


class TargetS3StockSymbols(TargetDBWrite):

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
		self.s3_write_path = TargetDBWrite.S3_PREFIX + self.s3_bucket + r"/"

	def execute(self, context):
		#Get S3 data
		logging.info(f'Reading from S3 bucket {self.s3_bucket} key {self.s3_key}')
		s3_df = self.spark.read.json(self.s3_write_path + self.s3_key)
		logging.info(f'S3 read URL: {s3_df}')
		if s3_df:
			df_pd = s3_df.toPandas()
			logging.info(f'Shape of dataframe for stock symbols {self.industry} - {df_pd.shape}')
			written_records = 0
			for desc, ticker in df_pd.values:
				self.session.execute(TargetS3StockSymbols.SQL_INSERT, (ticker, desc, self.industry))
				written_records += 1
			logging.info(f'Written {written_records} to cassandra cluster {self.session.hosts}')	