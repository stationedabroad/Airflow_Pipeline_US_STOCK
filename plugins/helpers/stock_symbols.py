import urllib.request as req
import configparser
import re
import json
import logging


class StockSymbols(object):

	def __init__(self):
		# Read config file
		with open("plugins/helpers/config.json", "r") as f:
		    self.config = json.load(f)

		self.ALL_STOCK_SYMBOLS_URL = self.config['USTOCK']['url']
		self.FILE_TO_WRITE = self.config['OUTPUT']
		
		self.US_STOCK_INDUSTRY_CODES = {
		    "Agriculture": {"code": "WSJMXUSAGRI", 
		    				"filename": self.FILE_TO_WRITE.format("Agriculture".replace("/", "_")), 
		    				"s3_key_stock_symbols": 'Agriculture-{}.json',
		    				"s3_key_eod": "Agriculture-eod-{start}-to-{end}-{ds}.json"},
		    "Automotive": {"code": "WSJMXUSAUTO", 
		    			   "filename": self.FILE_TO_WRITE.format("Automotive".replace("/", "_")), 
		    			   's3_key_stock_symbols': 'Automotive-{}.json',
		    			   "s3_key_eod": "Automotive-eod-{start}-to-{end}-{ds}.json"},
		    "Basic Materials/Resources": {"code": "WSJMXUSBSC", 
		    						      "filename": self.FILE_TO_WRITE.format("Basic Materials/Resources".replace("/", "_")), 
		    						      's3_key_stock_symbols': 'BasicMaterialsResources-{}.json',
		    						      "s3_key_eod": "Materials-Resources-eod-{start}-to-{end}-{ds}.json"},
		    "Business/Consumer Services": {"code": "WSJMXUSCYC", 
		    							   "filename": self.FILE_TO_WRITE.format("Business/Consumer Services".replace("/", "_")), 
		    							   "s3_key_stock_symbols": 'Business_ConsumerServices-{}.json',
		    							   "s3_key_eod": "Business-ConsumerServices-eod-{start}-to-{end}-{ds}.json" },
		    "Consumer Goods": {"code": "WSJMXUSNCY", 
		    				   "filename": self.FILE_TO_WRITE.format("Consumer Goods".replace("/", "_")), 
		    				   "s3_key_stock_symbols": 'ConsumerGoods-{}.json',
		    				   "s3_key_eod": "ConsumerGoods-eod-{start}-to-{end}-{ds}.json"},
		    "Energy": {"code": "WSJMXUSENE", 
		    		   "filename": self.FILE_TO_WRITE.format("Energy".replace("/", "_")), 
		    		   's3_key_stock_symbols': 'Energy-{}.json',
		    		   "s3_key_eod": "Energy-eod-{start}-to-{end}-{ds}.json"},
		    "Financial Services": {"code": "WSJMXUSFCL", 
		    					   "filename": self.FILE_TO_WRITE.format("Financial Services".replace("/", "_")), 
		    					   's3_key_stock_symbols': 'FinancialServices-{}.json',
		    					   "s3_key_eod": "FinancialServices-eod-{start}-to-{end}-{ds}.json"},
		    "Health Care/Life Sciences": {"code": "WSJMXUSHCR", 
		                                  "filename": self.FILE_TO_WRITE.format("Health Care/Life Sciences".replace("/", "_")), 
		                                  's3_key_stock_symbols': 'HealthCare-LifeSciences-{}.json',
		                                  "s3_key_eod": "HealthCare-LifeSciences-eod-{start}-to-{end}-{ds}.json"},
		    "Industrial Goods": {"code": "WSJMXUSIDU", 
		    					 "filename": self.FILE_TO_WRITE.format("Industrial Goods".replace("/", "_")), 
		    					 's3_key_stock_symbols': 'IndustrialGoods-{}.json',
		    					 "s3_key_eod": "IndustrialGoods-eod-{start}-to-{end}-{ds}.json"},
		    "Leisure/Arts/Hospitality": {"code": "WSJMXUSLEAH", 
		    							 "filename": self.FILE_TO_WRITE.format("Leisure/Arts/Hospitality".replace("/", "_")), 
		    							 's3_key_stock_symbols': 'Leisure-Arts-Hospitality-{}.json',
		    							 "s3_key_eod": "Leisure-Arts-Hospitality-eod-{start}-to-{end}-{ds}.json"},
		    "Media/Entertainment": {"code": "WSJMXUSMENT", 
		    						"filename": self.FILE_TO_WRITE.format("Media/Entertainment".replace("/", "_")), 
		    						's3_key_stock_symbols': 'MediaEntertainment-{}.json',
		    						"s3_key_eod": "Media-Entertainment-eod-{start}-to-{end}-{ds}.json"},
		    "Real Estate/Construction": {"code": "WSJMXUSRECN", 
		     							 "filename": self.FILE_TO_WRITE.format("Real Estate/Construction".replace("/", "_")), 
		     							 's3_key_stock_symbols': 'RealEstate-Construction-{}.json',
		     							 "s3_key_eod": "RealEstate-Construction-eod-{start}-to-{end}-{ds}.json"},
		    "Retail/Wholesale": {"code": "WSJMXUSRTWS", 
		    					 "filename": self.FILE_TO_WRITE.format("Retail/Wholesale".replace("/", "_")), 
		    					 's3_key_stock_symbols': 'RetailWholesale-{}.json',
		    					 "s3_key_eod": "Retail-Wholesale-eod-{start}-to-{end}-{ds}.json"},
		    "Technology": {"code": "WSJMXUSTEC", 
		    			   "filename": self.FILE_TO_WRITE.format("Technology".replace("/", "_")), 
		    			   's3_key_stock_symbols': 'Technology-{}.json',
		    			   "s3_key_eod": "Technology-eod-{start}-to-{end}-{ds}.json"},
		    "Telecommunication Services": {"code": "WSJMXUSTEL", 
		                                   "filename": self.FILE_TO_WRITE.format("Telecommunication Services".replace("/", "_")), 
		                                   's3_key_stock_symbols': 'TelecommunicationServices-{}.json',
		                                   "s3_key_eod": "TelecommunicationServices-eod-{start}-to-{end}-{ds}.json"},
		    "Transportation/Logistics": {"code": "WSJMXUSTRSH", 
		    							 "filename": self.FILE_TO_WRITE.format("Transportation/Logistics".replace("/", "_")), 
		    							 's3_key_stock_symbols': 'TransportationLogistics-{}.json',
		    							 "s3_key_eod": "Transportation-Logistics-eod-{start}-to-{end}-{ds}.json"},
		    "Utilities": {"code": "WSJMXUSUTI", 
		    			  "filename": self.FILE_TO_WRITE.format("Utilities".replace("/", "_")), 
		    			  's3_key_stock_symbols': 'Utilities-{}.json',
		    			  "s3_key_eod": "Utilities-eod-{start}-to-{end}-{ds}.json"}
		    }


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


	def get_stock_symbols_for_industry(self, industry):
		if industry in self.US_STOCK_INDUSTRY_CODES:
			file = self.US_STOCK_INDUSTRY_CODES[industry]['filename']
			with open(file) as f:
				symbols = json.load(f)
			return symbols