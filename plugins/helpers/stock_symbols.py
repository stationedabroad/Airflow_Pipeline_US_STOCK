import urllib.request as req
import configparser
import re
import json
import logging


class StockSymbols:
	
	US_STOCK_INDUSTRY_CODES = {
	    "WSJMXUSAGRI": "Agriculture",
	    "WSJMXUSAUTO": "Automotive",
	    "WSJMXUSBSC": "Basic Materials/Resources",
	    "WSJMXUSCYC": "Business/Consumer Services",
	    "WSJMXUSNCY": "Consumer Goods",
	    "WSJMXUSENE": "Energy",
	    "WSJMXUSFCL": "Financial Services",
	    "WSJMXUSHCR": "Health Care/Life Sciences",
	    "WSJMXUSIDU": "Industrial Goods",
	    "WSJMXUSLEAH": "Leisure/Arts/Hospitality",
	    "WSJMXUSMENT": "Media/Entertainment",
	    "WSJMXUSRECN": "Real Estate/Construction",
	    "WSJMXUSRTWS": "Retail/Wholesale",
	    "WSJMXUSTEC": "Technology",
	    "WSJMXUSTEL": "Telecommunication Services",
	    "WSJMXUSTRSH": "Transportation/Logistics",
	    "WSJMXUSUTI": "Utilities"
	    }

	def write_stock_symbols_for_industry(industry):

		with open("config.json", "r") as f:
		    config = json.load(f)

		URL = config['USTOCK']['url']
		FILE_TO_WRITE = config['OUTPUT']

		matcher = re.compile(r'{}'.format(config['USTOCK']['pattern']))

		ic = industry.replace("/", "_")
		with open(FILE_TO_WRITE.format(ic), 'w') as f:

		    logging.info(f'Opened file {US_STOCK_INDUSTRY_CODES[industry]}')
		    f.write('stock_symbol, company_name\n')
			
			page = 0
			symbols_written = 0

		    while True:
		        url = URL.format(code=industry, page=page)
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
		            f.write("{},{}\n".format(symbol_code, desc))
		            symbols_written += 1
		        page += 50

			logging.info(f'Symbols written {symbols_written} to {FILE_TO_WRITE.format(ic)}')	    
