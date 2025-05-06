import psycopg2
from psycopg2 import OperationalError, DatabaseError
import get_parse_wiki_snp500 as gpws




db_config = {
        'host': 'localhost',
        'database': 'securities_master',
        'password': 'Aminata95.27081995.',
        'port': 5432 ,
        'user':'postgres'
    }


def insert_snp500_symbols(symbols, exchanges_id):

    """
    Insert the S&P500 symbols into my Postgres Database
    Expects symbols to be a list of tuples 
    """
    connection = None 
    id = (exchanges_id, )
    merged = [id + symbol for symbol in symbols]
    
    try: 
        connection = psycopg2.connect(**db_config)
        cursor = connection.cursor()
        sql = """
            INSERT INTO symbol (
            exchange_id,
            ticker,
            instrument,
            name,
            sector,
            currency,
            created_date,
            last_updated_date
            )  VALUES (
             %s, %s, %s, %s, %s, %s, %s,%s
            )
            """
        
        cursor.executemany(sql, merged)
        connection.commit()
        print("Data was successfully inserted")

    except(OperationalError, DatabaseError) as e: 
        print("Database error", e)

    finally: 
        if connection: 
            cursor.close()
            connection.close()
    
   

if __name__ == "__main__": 

    symbols = gpws.get_parse_wiki_snp500()
    #exchanges= gpws.get_exchange_name(symbols)
    abbrev = ['NYQ']* len(symbols) # all the symbols are listed in NYSE
    exchanges_id = gpws.get_exchange_id_by_name(abbrev)
    
    insert_snp500_symbols(symbols, exchanges_id)




