import datetime 
import psycopg2
from psycopg2 import OperationalError, DatabaseError 

Exchange_MAP = {
    "NMS": "National Market System (NASDAQ and other exchanges)",
    "NYQ": "New York Stock Exchange (NYSE)",
    "BATS": "BATS Global Markets",
    "TSE": "Tokyo Stock Exchange",
    "LSE": "London Stock Exchange",
    "TSX": "Toronto Stock Exchange",
    "FWB": "Frankfurt Stock Exchange",
    "HKG": "Hong Kong Stock Exchange",
    "SHG": "Shanghai Stock Exchange",
    "SHE": "Shenzhen Stock Exchange",
    "BOM": "Bombay Stock Exchange (India)",
    "KRX": "Korea Stock Exchange",
    "ASX": "Australian Securities Exchange",
    "SIX": "Swiss Exchange",
    "FRA": "Frankfurt Stock Exchange",
    "ISE": "International Securities Exchange",
    "XETR": "Xetra",
    "MEX": "Mexico Stock Exchange",
    "JSE": "Johannesburg Stock Exchange"
}

db_config = {
        'host': 'localhost',
        'database': 'securities_master',
        'password': 'Aminata95.27081995.',
        'port': 5432 ,
        'user':'postgres'
    }

def insert_exchange(items): 
    """Inserts exchanges name and abbreviations  into securities master Database. """
    connection = None 
    try: 
        connection = psycopg2.connect(**db_config)
        cursor     = connection.cursor()
        sql        = """
                    INSERT into exchange(
                    abbrev, 
                    name, 
                    created_date,
                    last_updated
                    ) VALUES (
                    %s, %s, %s, %s)
                    """
        
        cursor.executemany(sql, items)
        #connection.commit()
        print("Data was successfully inserted")
    except(OperationalError, DatabaseError) as e: 
        print("Database error", e)
    finally:
        if connection: 
            cursor.close()
            connection.close()


items = []
time_now = datetime.datetime.now()
for abbrev, name in Exchange_MAP.items(): 
    exchange_item = (abbrev, name, time_now, time_now)
    items.append(exchange_item)


if __name__ == "__main__": 
    insert_exchange(items)

