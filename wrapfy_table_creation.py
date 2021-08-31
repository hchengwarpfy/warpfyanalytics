import json
import pandas as pd
from sqlalchemy import create_engine
import psycopg2
from psycopg2 import Error
# The Postgres queries for creating tables and db credentials are configured in queries_cred.py file as dictionaries
from queries_cred import queries, data


def create_table(user, password, host, port, database, query):
    try:
        connection = psycopg2.connect(user=user,
                                      password=password,
                                      host=host,
                                      port=port,
                                      database=database)

        cursor = connection.cursor()
        # SQL query to create a new table
        create_table_query = query
        # Execute a command: this creates a new table
        cursor.execute(create_table_query)
        connection.commit()
        print("Table created successfully in PostgreSQL ")

    except (Exception, Error) as error:
        print("Error while connecting to PostgreSQL", error)
    finally:
        if connection:
            cursor.close()
            connection.close()
            print("PostgreSQL connection is closed")
            
if __name__ == "__main__":
    create_table(data['user'], data['password'], data['host'], data['port'], data['database'], queries['asin_category_query'])
    create_table(data['user'], data['password'], data['host'], data['port'], data['database'], queries['master_keywords_market_query'])
    create_table(data['user'], data['password'], data['host'], data['port'], data['database'], queries['master_keywords_query_search_info'])
    
