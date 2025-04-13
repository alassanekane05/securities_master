import psycopg2
from psycopg2 import OperationalError, DatabaseError




db_config = {
        'host': 'localhost',
        'database': 'securities_master',
        'password': 'Aminata95.27081995.',
        'port': 5432 
    }


def insert_snp500_symbols(symbols):

    """
    Insert the S&P500 symbols into my Postgres Database
    Expects symbols to be a list of tuples 
    """
    
    try: 
        connection = psycopg2.connect(**db_config)
        cursor = connection.cursor()
        sql = """
            INSERT INTO symbol (
            ticker,
            instrument,
            name,
            sector,
            currency,
            created_date,
            last_updated_date
            )  VALUES (
             %s, %s, %s, %s, %s, %s, %s
            )
            """
        
        cursor.executemany(sql, symbols)
        connection.commit()
        print("Data was successfully inserted")

    except(OperationalError, DatabaseError) as e: 
        print("Database error", e)

    finally: 
        if connection: 
            cursor.close()
            connection.close()
    
    

        


