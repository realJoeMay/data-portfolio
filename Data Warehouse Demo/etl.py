import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Executes Redshift queries to load the staging tables with the song and
    event datasets stored on S3.

    Args:
        cur: Psycopg cursor object
        conn: Psycopg connection object
    """
    for query in copy_table_queries:

        print('running query now')
        print(query)
        print()

        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Executes Redshift queries to transform the data from the staging tables,
    and load into the analysis tables.

    Args:
        cur: Psycopg cursor object
        conn: Psycopg connection object
    """
    for query in insert_table_queries:

        print('running query now')
        print(query)
        print()

        cur.execute(query)
        conn.commit()


def main():
    """
    Creates connection to the Redshift data warehouse, and loads the datasets 
    to the staging and analysis tables.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['DB'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()