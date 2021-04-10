from pyspark import SparkFiles

import env_settings
from dimagi_lake.utils import get_db_name
from dimagi_lake.migration.migration_config import DomainMigration
from spark_session_handler import SPARK


def drop_table_query(db_name, table_name):
    return f"DROP TABLE IF EXISTS {db_name}.{table_name}"


def create_table_query(db_name, table_name, data_path):
    return f"CREATE TABLE {db_name}.{table_name} using delta LOCATION  '{data_path}';"


def create_db(db_name):
    databases = [database.name for database in SPARK.catalog.listDatabases()]
    if db_name not in databases:
        SPARK.sql(f"CREATE DATABASE {db_name}")
        print(f"NEW DATABASE CREATED: {db_name}")


def recreate_tables(domain, db_name):

    def _get_migration_queries(migration_config):
        table_name = migration_config['table_name']
        data_path = f"{env_settings.HQ_DATA_PATH}{migration_config['data_path']}"
        return {
            "table_name": table_name,
            "drop_table": drop_table_query(db_name, table_name),
            "create_table": create_table_query(db_name, table_name, data_path)
        }

    def _run_migration(query):
        SPARK.sql(query['drop_table'])
        SPARK.sql(query['create_table'])
        print(f"migrated table : {query['table_name']}")

    tables = DomainMigration.domain_tables[domain]
    migration_config_df = SPARK.sqlContext.createDataframe(tables, ['table_name', 'data_path'])
    
    queries_rdd = migration_config_df.rdd.map(_get_migration_queries)

    for query in queries_rdd.collect():
        _run_migration(query)


def migrate_domain_tables(domain_name):
    database_name = get_db_name(domain_name)
    create_db(database_name)
    recreate_tables(domain, database_name)
