from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.S3_hook import S3Hook


class S3CreateBucket(BaseOperator):
	template_fields = ('execution_date',)

	@apply_defaults
	def __init__(self, 
				 aws_conn_id, 
				 bucket_name,
				 execution_date,
				 *args, **kwargs):

		super(self, S3CreateBucket).__init(*args, **kwargs)

		self.aws_conn_id = aws_conn_id
		self.execution_date = execution_date
		s3_hook = S3Hook(aws_conn_id=aws_conn_id)


	def execute(self, context):
		bucket_to_create = f'{bucket_name}_{self.execution_date}'
		logging.info(f'Checking if bucket {bucket_to_create} already exists')
		if s3_hook.check_for_bucket(bucket_to_create):
			raise ValueError(f'Bucket -> {bucket_to_create}  already exists')
		
		logging.info(f'Creating S3 bucket -> {bucket_to_create}')
		s3_hook.create_bucket(bucket_name=bucket_to_create)
		if s3_hook.check_for_bucket(bucket_to_create):
			logging.info(f'S3 bucket -> {bucket_to_create} created successfully')