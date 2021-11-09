import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Description: This function is used to drop the tables if they exists before creating them 
    """
    
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Description: Used to create the tables including staging 
    """
    
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Description: This function can be used to create the tables, connect to the cluster using the credentials 

    Returns:
        None
    """
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()