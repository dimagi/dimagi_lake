from datalake_conts import DIMAGI_LAKE_DIR
from spark_session_handler import SPARK
from src.utils import clean_name


def drop_table_query(db_name, table_name):
    return f"DROP TABLE IF EXISTS {db_name}.{table_name}"


def create_table_query(db_name, table_name, data_path):
    return f"CREATE TABLE {db_name}.{table_name} using delta LOCATION  '{data_path}';"


def create_db(db_name):
    databases = [database.name for database in SPARK.catalog.listDatabases()]
    if db_name not in databases:
        SPARK.sql(f"CREATE DATABASE {db_name}")
        print(f"NEW DATABASE CREATED: {db_name}")


def recreate_tables(db_name):

    def _get_migration_queries(migration_config):
        return {
            "table_name": migration_config['table_name'],
            "drop_table": drop_table_query(db_name, migration_config['table_name']),
            "create_table": create_table_query(db_name, migration_config['table_name'], migration_config['data_path'])
        }

    migration_file = f"file:///{DIMAGI_LAKE_DIR}/src/migration/migration_config/{db_name}.json"
    migration_config_df = SPARK.read.format('json').load(migration_file)
    queries = migration_config_df.rdd.map(_get_migration_queries)

    for query in queries:
        SPARK.sql(query['drop_table'])
        SPARK.sql(query['create_table'])
        print(f"migrated table : {query['table_name']}")


def migrate_domain_tables(domain_name):
    database_name = f"commcarehq_{clean_name(domain_name)}"
    create_db(database_name)
    recreate_tables(database_name)
