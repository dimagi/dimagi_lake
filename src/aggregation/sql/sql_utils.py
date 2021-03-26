import psycopg2
from datalake_conts import (
DASHBOARD_DB_HOST,
DASHBOARD_DB_PORT,
DASHBOARD_DB_NAME,
DASHBOARD_DB_USERNAME,
DASHBOARD_DB_PASSWORD
)
from src.aggregation.sql import sql_queries


def connect_to_db():
    return psycopg2.connect(database=DASHBOARD_DB_NAME,
                            user=DASHBOARD_DB_USERNAME,
                            password=DASHBOARD_DB_PASSWORD,
                            host=DASHBOARD_DB_HOST,
                            port=DASHBOARD_DB_PORT)


def create_table(cur, base_table, table_name, domain):
    query = sql_queries.create_domain_table_query(base_table,table_name, domain)
    cur.execute(query)


def detach_partition(cur, base_table, table_name):
    query = sql_queries.dettach_partition_query(base_table, table_name)
    cur.execute(query)


def rename_table(cur, old_table_name, new_table_name):
    query = sql_queries.rename_table_query(old_table_name, new_table_name)
    cur.execute(query)


def attach_partition(cur, base_table, child_table):
    query = sql_queries.attach_partition_query(base_table, child_table)
    cur.execute(query)


def drop_table(cur, table_name):
    query = sql_queries.drop_table_query(table_name)
    cur.execute(query)
