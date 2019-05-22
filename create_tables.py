import configparser
from logging import getLogger
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

log = getLogger(__name__)


def drop_tables(cur, conn):
    log.info("Dropping tables if they exist")
    for query in drop_table_queries:
        log.info(f"Running this drop table query: {query}")
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    log.info("Creating tables")
    for query in create_table_queries:
        log.info(f"Running this create table query: {query}")
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    log.info("Getting database connection")
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    log.info("Connection established")
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()