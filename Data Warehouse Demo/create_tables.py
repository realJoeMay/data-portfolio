import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drops all staging and final tables from the redshift data warehouse.

    Args:
        cur: Psycopg cursor object
        conn: Psycopg connection object
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates all staging and final tables from the redshift data warehouse.

    Args:
        cur: Psycopg cursor object
        conn: Psycopg connection object
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Creates connection to the Redshift data warehouse and creates all of the
    staging and final tables.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['DB'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()