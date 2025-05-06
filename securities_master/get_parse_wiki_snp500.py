import datetime 
from bs4 import BeautifulSoup
import requests
import yfinance as yf 
import psycopg2
from psycopg2 import OperationalError, DatabaseError

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


def get_exchange_name(symbols): 
    """Get the exchange name for each ticker """

    list_of_tickers = [symbol[0] for symbol in symbols]
    exchanges = []
    for ticker in list_of_tickers:  
        data = yf.Ticker(ticker)
        exchange = data.info.get('exchange', 'No exchange info available')
        exchanges.append(exchange)
    return exchanges


db_config = {
        'host': 'localhost',
        'database': 'securities_master',
        'password': 'Aminata95.27081995.',
        'port': 5432 ,
        'user':'postgres'
    }

def get_exchange_id_by_name(abbrev):
    """Connects to database and gets the foreign key exchange_id(s) for a list of exchange names."""
    connection = None
    try:
        connection = psycopg2.connect(**db_config)
        cursor = connection.cursor()
        
        
        placeholders = ','.join(['%s'] * len(abbrev))  # e.g. '%s,%s,%s' for 3 names
        print(placeholders)
        sql = f"""
            SELECT id
            FROM exchange
            WHERE abbrev IN ({placeholders})
        """

        cursor.execute(sql, tuple(abbrev))
        result = cursor.fetchall()  # returns list of (id, name) tuples
        return result[0][0]
    except Exception as e:
        print("Database error:", e)
    finally:
        if connection:
            cursor.close()
            connection.close()




