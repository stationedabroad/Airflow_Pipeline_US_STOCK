import urllib.request as req
import configparser
import re
import json
import logging


class StockSymbols:
	
	US_STOCK_INDUSTRY_CODES = {
	    "Agriculture": "WSJMXUSAGRI",
	    "Automotive": "WSJMXUSAUTO",
	    "Basic Materials/Resources": "WSJMXUSBSC",
	    "Business/Consumer Services": "WSJMXUSCYC",
	    "Consumer Goods": "WSJMXUSNCY",
	    "Energy": "WSJMXUSENE",
	    "Financial Services": "WSJMXUSFCL",
	    "Health Care/Life Sciences": "WSJMXUSHCR",
	    "Industrial Goods": "WSJMXUSIDU",
	    "Leisure/Arts/Hospitality": "WSJMXUSLEAH",
	    "Media/Entertainment": "WSJMXUSMENT",
	    "Real Estate/Construction": "WSJMXUSRECN",
	    "Retail/Wholesale": "WSJMXUSRTWS",
	    "Technology": "WSJMXUSTEC",
	    "Telecommunication Services": "WSJMXUSTEL",
	    "Transportation/Logistics": "WSJMXUSTRSH",
	    "Utilities": "WSJMXUSUTI",
	    }

	def write_stock_symbols_for_industry(industry):

		industry_code = StockSymbols.US_STOCK_INDUSTRY_CODES[industry]
		import os
		logging.info(f'CONFIG >>>> {os.getcwd()}')
		with open("plugins/helpers/config.json", "r") as f:
		    config = json.load(f)

		URL = config['USTOCK']['url']
		FILE_TO_WRITE = config['OUTPUT']
		matcher = re.compile(r'{}'.format(config['USTOCK']['pattern']))

		industry_file = FILE_TO_WRITE.format(industry.replace("/", "_"))
		with open(industry_file, 'a') as f:

		    logging.info(f'Opened file {industry}')
		    # f.write('stock_symbol, company_name\n')

		    page = 0
		    symbols_written = 0

		    while True:
		        url = URL.format(code=industry_code, page=page)
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
		            # f.write("{},{}\n".format(symbol_code, desc))
		            f.write(f'{json.dumps({"symbol_code": symbol_code, "company_name": desc})}\n')
		            symbols_written += 1
		        page += 50
		    logging.info(f'Symbols written {symbols_written} to {industry_file}')
		return industry_file, symbols_written