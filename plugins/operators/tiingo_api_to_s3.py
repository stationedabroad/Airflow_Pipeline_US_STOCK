import logging
import requests
import json
import sys

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.S3_hook import S3Hook


class TiingoApi(BaseOperator):

	JSON_header = {
		'Content-Type': 'application/json'
	}

	CSV_header = {
		'Content-Type': 'text/csv'
	}

	@apply_defaults
	def __init__(self,
				 endpoint,
				 header,
				 path_to_write,
				 *args, **kwargs):

		super(TiingoApi, self).__init__(*args, **kwargs)
		self.endpoint = endpoint
		self.header = header
		self.path_to_write = path_to_write

	def fetch_api(self, endpoint, header=None):
		if not header:
			header = self.header
		try:
			content = requests.get(url=endpoint, headers=header)
			if not content.status_code == 200:
				raise ValueError(f'')
			return {
				TiingoApi.JSON_header['Content-Type']: content.json(),
				TiingoApi.CSV_header['Content-Type']: content.text
			}[header['Content-Type']]
		except Exception as e:
			logging.info(f'endpoint yielded http error {content.status_code}, exception is {e}')

	def execute(self, context):
		pass



class TiingoPricePerIndustryHistorical(TiingoApi):

	ENDPOINT = r'https://api.tiingo.com/tiingo/daily/{ticker}/prices?startDate={start_date}&endDate={end_date}&token={api_key}&resampleFreq={frequency}'

	@apply_defaults
	def __init__(self,
				 industry,
				 stock_symbols,
				 frequency,
				 h_start_date,
				 h_end_date,
				 path_to_write,
				 *args, **kwargs):

		super(TiingoPricePerIndustryHistorical, self).__init__(endpoint=TiingoPricePerIndustryHistorical.ENDPOINT, header=TiingoApi.JSON_header, path_to_write=path_to_write, *args, **kwargs)
		self.industry = industry
		self.stock_symbols = stock_symbols
		self.frequency = frequency
		self.h_start_date = h_start_date
		self.h_end_date = h_end_date
		self.api_key = os.environ.get('TIINGO_API_KEY')
		self.file_to_write = "{path}/end_of_day_{start}_to_{end}_{ind}.json".format(
																	path=self.path_to_write, 
																	start=self.h_start_date, 
																	end=self.h_end_date, 
																	ind=self.industry)

	def execute(self, context):
		logging.info(f'Commencing fetch of ticker data for {self.industry} industry')

		entries_written = 0
		with open(self.file_to_write, 'w') as f:
			logging.info(f'Opening file for output -< {self.file_to_write}')
			with JSONArrayWriter(f) as jstream:
				for ticker in self.stock_symbols:
					ticker_sym = ticker['symbol_code']
					per_file_recs = 0
					url = self.ENDPOINT.format(ticker=ticker_sym, 
											   start_date=self.h_start_date, 
											   end_date=self.h_end_date, 
											   api_key=self.api_key, 
											   frequency=self.frequency)
					logging.info(f'url api -> {url}')
					content = self.fetch_api(url)
					if content:
						logging.info(f'Writing to stream content for {ticker}')
						for entry in content:
							entry['ticker'] = ticker_sym
							jstream.write(entry)
							per_file_recs += 1
						logging.info(f'Written {per_file_recs} records for ticker {ticker_sym}')
						entries_written += per_file_recs
				logging.info(f'Write completed, total written {entries_written} records')			



class JSONArrayWriter(object):
    """
    accepts a file path or a file like object
    writes the output as a json array
    in file

    """
    def __init__(self, o):

        if hasattr(o, 'read'):
            self.obj = o

        if isinstance(o, str):
            self.obj = open(o, "wb")

    def __enter__(self):
        """
        bound input with open square bracket
        """
        self.obj.write("[")
        return self

    def __exit__(self, _type, value, traceback):
        """
        bound input with close square bracket, then close the file
        """
        self.obj.write("]")
        self.obj.close()

    def write(self, obj):
        """
        writes the first row, then overloads self with delimited_write
        """
        try:
            self.obj.write(json.dumps(obj))
            setattr(self, "write", self.delimited_write)
        except:
            self.bad_obj(obj)

    def delimited_write(self, obj):
        """
        prefix json object with a comma
        """
        try:
            self.obj.write("," + json.dumps(obj))
        except:
            self.bad_obj(obj)

    def bad_obj(self, obj):
        raise SerializationError("%s object not not serializable"%type(obj))
 
class SerializationError(Exception):
    def __init__(self, value):
        self.value = value 

    def __str__(self):
        return repr(self.value) 
