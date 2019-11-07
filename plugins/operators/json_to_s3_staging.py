from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.S3_hook import S3Hook


class StageJsonToS3(BaseOperator):
	template_fields = ('execution_date',)

	@apply_defaults
	def __init__(self, 
				 execution_date, 
				 aws_conn_id, 
				 s3_bucket, 
				 s3_key, 
				 *args, **kwargs):

		super(self, StageJsonToS3).__init(*args, **kwargs)

		self.aws_conn_id = aws_conn_id
		self.s3_hook = S3Hook(self.aws_conn_id)
		self.s3_bucket = s3_bucket
		self.s3_key = s3_key


	def execute(self, context):
		pass		