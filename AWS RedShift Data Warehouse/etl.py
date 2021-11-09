import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Description: This function is to load the log and song files into the two staging tables. 

    Arguments:
        cur: the cursor object. 
        filepath: connection to the cluster. 

    Returns:
        None
    """
    
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Description: This function is to load the data from the staging tables into the other tables 

    Arguments:
        cur: the cursor object. 
        filepath: song data file path. 

    Returns:
        None
    """
    
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Description: This function is to connect to the cluster, load the staing tables and inset data from the staging tables into the regular tables 
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