import configparser
from logging import getLogger
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

log = getLogger(__name__)

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DB_HOST = config.get('CLUSTER', 'DB_HOST')
# DB_NAME = config.get('DEFAULT', 'DB_NAME')
# DB_USER = config.get('S3', 'DB_USER')
# SONG_DATA = config.get('S3', 'SONG_DATA')


def load_staging_tables(cur, conn):
    log.info("On loading staging tables")
    for query in copy_table_queries:
        log.info(f"On running this copy query: {query}")
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    log.info("On inserting into dimensions and fact tables")
    for query in insert_table_queries:
        log.info(f"On running this insert query: {query}")
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    log.info("Getting database connection")
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    log.info("Connection established")
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()