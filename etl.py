import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

def load_staging_tables(cur, conn):
    """
    Load the JSON files from S3 buckets into the staging tables
    
    Arguments:
        cur - PostgreSQL cursor object
        conn - psycopg2 connection instance
        
    Returns:
        None
    """
    
    for query in copy_table_queries:
            cur.execute(query)
            conn.commit()

def insert_tables(cur, conn):
    """
    Load data into existing tables from the staging tables
    with names specified in the insert_table_queries list.
    
    Arguments:
        cur - PostgreSQL cursor object
        conn - psycopg2 connection instance
        
    Returns:
        None
    """
    for query in insert_table_queries:
            cur.execute(query)
            conn.commit()

def main():
    """
    Load the JSON files from S3 buckets into the staging tables, and then
    Load data into existing tables from the staging tables with names 
    specified in the insert_table_queries list.
    
    Arguments:
        None
        
    Returns:
        None
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    
    conn.close()

if __name__ == "__main__":
    main()