from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.S3_hook import S3Hook

import logging


class StageJsonToS3(BaseOperator):
	template_fields = ('execution_date',)

	@apply_defaults
	def __init__(self, 
				 execution_date, 
				 aws_conn_id, 
				 s3_bucket, 
				 s3_key,
				 path_to_data,
				 *args, **kwargs):

		super(StageJsonToS3, self).__init__(*args, **kwargs)

		self.aws_conn_id = aws_conn_id
		self.s3_hook = S3Hook(self.aws_conn_id)
		self.s3_bucket = s3_bucket
		self.s3_key = s3_key


	def execute(self, context):
		pass


class S3CreateBucket(BaseOperator):
	template_fields = ('execution_date',)

	@apply_defaults
	def __init__(self, 
				 aws_conn_id, 
				 bucket_name,
				 execution_date,
				 *args, **kwargs):

		super(S3CreateBucket, self).__init__(*args, **kwargs)

		self.aws_conn_id = aws_conn_id
		self.bucket_name = bucket_name
		self.execution_date = execution_date
		self.s3_hook = S3Hook(aws_conn_id=aws_conn_id)


	def execute(self, context):
		bucket_to_create = f'{self.bucket_name}-{self.execution_date}'
		logging.info(f'Checking if bucket {bucket_to_create} already exists')
		if self.s3_hook.check_for_bucket(bucket_to_create):
			raise ValueError(f'Bucket -> {bucket_to_create}  already exists')
		
		logging.info(f'Creating S3 bucket -> {bucket_to_create}')
		self.s3_hook.create_bucket(bucket_name=bucket_to_create)
		if self.s3_hook.check_for_bucket(bucket_to_create):
			logging.info(f'S3 bucket -> {bucket_to_create} created successfully')		