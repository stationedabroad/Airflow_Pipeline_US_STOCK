# Airflow-Pipeline US Stock Prices (using apache spark & apache cassandra): *Industry Wide*
Scraping stock symbols for daily prices look ups (suprisingly difficult to get stock symbols in a master list fashion).  This data is then staged in S3 and fed into Apache Cassandra.
This data pipeline focuses on US stock by industry, stock symbols, stock prices and some basic aggregations to illustrate the use case of various technologies. The high level steps; getting a complete listing of US stock by industry, writing to an S3 bucket, processing via Apache Spark and eventually writing to an analytical layer in an Apache Cassandra standalone instance.  All this in the absense of !yahoo finance which no longer exists!

## Overall 'the what' of this data pipeline
Take some data from the web via scraping, use this as a basis to query a web API and gather historical data (though can be used for an ongoing daily batch) and:
1. Write locally the data as json or avro files.
2. Create AWS S3 bucket for that particular execution date
3. Update various data into AWS S3 bucket
4. Aggregate and transform data using Apache Spark, reading directly from the S3 bucket
5. Update data into Apache Cassandra

A nice **overview** diagram can be summed up by the DAG itself in airflow in its graphical view.  This shows the overall data pipeline flow and the various stages of the dag, namely; *web scrape read* **->** *web API read* **->** *write to S3* **->** *stock symbols & EOD prices* **->** *write to cassandra*:
![DAG](diag/overview_dag.png)


Apache Airflow was used with a number of custom operators which extend the base functionality.  These include the following in the code listings within this repo and which reside in the *plugins* directory:
* **StageJsonToS3** - writes files to S3 given a bucket and key reference. ![jsontos3staging](https://github.com/stationedabroad/Airflow_Pipeline_US_STOCK/blob/0cb35485e319f1ec8e1fa599ed8fdbbc12aa0a7b/plugins/operators/json_to_s3_staging.py#L8)
* **S3CreateBucket** - creates S3 bucket if it does not exist, in this case an execution date is appended onto the bucket. ![creates3bucket](https://github.com/stationedabroad/Airflow_Pipeline_US_STOCK/blob/0cb35485e319f1ec8e1fa599ed8fdbbc12aa0a7b/plugins/operators/json_to_s3_staging.py#L41)
* **TiingoPricePerIndustryHistorical** - This runs the API call to Tiingo for stock prices, and inherits from a common API parent.  ![tiingoapi](https://github.com/stationedabroad/Airflow_Pipeline_US_STOCK/blob/0cb35485e319f1ec8e1fa599ed8fdbbc12aa0a7b/plugins/operators/tiingo_api_to_s3.py#L54)
* **TargetS3StockSymbols** - This uses the spark engine to read the staged S3 data and write the tock symbol master data to a standalone Apache Cassandra instance, this inherits from a common parent which initialises various connections. ![s3tocassandrastocksym](https://github.com/stationedabroad/Airflow_Pipeline_US_STOCK/blob/0cb35485e319f1ec8e1fa599ed8fdbbc12aa0a7b/plugins/operators/json_s3_to_cassandra.py#L50)
* **TargetS3EodLoad** - This uses the spark engine to read stock price data, run some aggregations and joins and write to the same Cassandra instance. ![s3tocassstockprices](https://github.com/stationedabroad/Airflow_Pipeline_US_STOCK/blob/0cb35485e319f1ec8e1fa599ed8fdbbc12aa0a7b/plugins/operators/json_s3_to_cassandra.py#L95)

A helper class
## Scope & Data
Data was sourced from two sources:
1. For stock symbols by industry (complete listing hopefully) the website: http://bigcharts.marketwatch.com
2. For stock prices based on (1) the API reference https://api.tiingo.com/ (Tiingo).  This seemed a complete and easy tp use API with a very cheap pricing plan.  Since I wanted a fair amount of data, I did not want to be limited to 500 calls per day on a free tier, at the same time I did not want to pay a lot for access to quality data.  Tiingo found the middle ground for me in that respect.

The scope of the data would include all US stock, this I got by scraping (1) bigcharts.marketwatch.com.  They had a well laid out website, by industry and fairly easily navigable.  This allowed me to use regular expressions and knowledge of the paging on consecutive pages and *flick through* the US stock by industry listing and write to a local json file.  This formed the basis of the query for the API call, so the process was, get stock symbols (tickers) from web scrape, and run these through the finance API from Tiingo to get a nice historical time series data set.

Listing 1 shows the basic web scraper, which takes a industry sector code (of the form **WSJMXUSAGRI** for agriculture for example) and returns rows written and filen name written:

```	
	def write_stock_symbols_for_industry(self, industry):
		"""
		Webscrape for stock symbols from: http://bigcharts.marketwatch.com
		"""

		industry_code = self.US_STOCK_INDUSTRY_CODES[industry]['code']
		matcher = re.compile(r'{}'.format(self.config['USTOCK']['pattern']))

		industry_file = self.US_STOCK_INDUSTRY_CODES[industry]['filename']
		with open(industry_file, 'w') as f:

		    logging.info(f'Opened file {industry}')
		    page = 0
		    symbols_written = 0
		    data = []
		    while True:
		        url = self.ALL_STOCK_SYMBOLS_URL.format(code=industry_code, page=page)
		        url_open = req.urlopen(url)
		        url_bytes = url_open.read()
		        url_str = url_bytes.decode('utf8')
		        url_open.close()
		        logging.info(f'url {url} read ...')

		        results = matcher.findall(url_str)
		        if not results:
		            break
		        logging.info(f'matched {len(results)} results')

		        for match in results:
		            code_st = match.find(">")+1
		            code_ed = match.find("</td>")
		            symbol_code = match[code_st:code_ed]
		            desc_st = match.find("<div>")+5
		            desc_ed = match.find("</div>")
		            desc = match[desc_st:desc_ed]
		            data.append({"symbol_code": symbol_code, 
		            		     "company_name": desc,
		            		     "industry": industry})
		            symbols_written += 1   
		        page += 50
		    json.dump(data, f)  
		    logging.info(f'Symbols written {symbols_written} to {industry_file}')
		
		return industry_file, symbols_written
```
Writing to a local machine tmp directory, the output would be ready to upload into an S3 bucket.  The picture shows a log line generated and written to the airflow job log per API call:
![pic1](/diag/read_to_tmp.png)



## Exploration of Data
The data is fairly well formed coming from an API.  Some old ticker symbols may not return data but are still returned from the webscrape, these just return an error in the log but it is useful to see when and in which industry companies are falling on or off the stock market radar.  This pipeline shows an historic load, but can be run **daily** in batch mode also, in fact it is designed to do just that.  For the historical load I defined variables in airflow, but if it was to run in daily batch mode a shcedule interval could be set up using the DAG properties.

The data set for the year across all US stock/industry returns approximately 3-4m rows of data. A simple flow of a single industry of stocks would look like thisin airflow:
![single Industry pipeline](diag/automotive_flow_end_to_end.png)


## Data Model Definition
The data model I created in fairly simple and meets the need of analytical reporting.  The S3 staging allows ML models and data analysis to be carried out also, bu primarily the cassandra instance is used to store the data and farm it out at different aggregations, combining different datasets for end user analysis.  An example of the model definition is below in two tables, stock symbols and end of day prices (Which combines stock symbol data):

![stock table](diag/us_stock.stock_symbols.png)

![EOD prices](diag/us_stock.eod_stock_price.png)

## Apache Airflow Configuratin details


## Wrap-up

## Thanks to ...
Big thanks to **lysdexia** from whom I took and modified a basic stream writer for JSON list
https://github.com/lysdexia/python-json-stream-writer
