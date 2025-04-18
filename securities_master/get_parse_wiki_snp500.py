import datetime 
from bs4 import BeautifulSoup
import requests 

URL = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'

def get_parse_wiki_snp500():

    """
    Fetches latest list of S&P500 companies by scraping the corresponding 
    table corresponding table from WikiPedia. 
    It extracts the ticker symbols, company name sector and other metadata.  
    """
    now =datetime.datetime.utcnow()
    response = requests.get(URL)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')
        table = soup.find('table', {'class': 'wikitable'})
        symbols = []
        for row in table.find_all('tr')[1:]:
            
            ticker  = row.find_all('td')[0].text.strip()
            name = row.find_all('td')[1].text.strip()
            sector = row.find_all('td')[2].text.strip()
            symbols.append((ticker,'Equities',name,sector,'USD',now,now))
            return symbols
    else:
       pass