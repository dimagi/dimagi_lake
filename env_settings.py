import os

KAFKA_BOOTSTRAP_SERVER_HOST = os.getenv('KAFKA_BOOTSTRAP_SERVER_HOST', 'localhost')
KAFKA_BOOTSTRAP_SERVER_PORT = os.getenv('KAFKA_BOOTSTRAP_SERVER_PORT', '9092')

# HIVE METASTORE
METASTORE_HOST = os.getenv('METASTORE_HOST', 'localhost')
METASTORE_PORT = os.getenv('METASTORE_PORT', '5432')
METASTORE_DB = os.getenv('METASTORE_DB', 'metastore_db')
METASTORE_USERNAME = os.getenv('METASTORE_USERNAME', '')
METASTORE_PASSWORD = os.getenv('METASTORE_PASSWORD', '')

METASTORE_CONNECT_URI = f'jdbc:postgresql://{METASTORE_HOST}:{METASTORE_PORT}/{METASTORE_DB}?ceateDatabaseIfNotExist=true'


DASHBOARD_DB_HOST = os.getenv('DASHBOARD_DB_HOST', 'localhost')
DASHBOARD_DB_PORT = os.getenv('DASHBOARD_DB_PORT', '5432')
DASHBOARD_DB_NAME = os.getenv('DASHBOARD_DB_NAME', 'custom_warehouse')
DASHBOARD_DB_USERNAME = os.getenv('DASHBOARD_DB_USERNAME', 'commcarehq')
DASHBOARD_DB_PASSWORD = os.getenv('DASHBOARD_DB_PASSWORD', 'commcarehq')
DASHBOARD_JDBC_URL = f"jdbc:postgresql://{DASHBOARD_DB_HOST}:{DASHBOARD_DB_PORT}/{DASHBOARD_DB_NAME}"
JDBC_PROPS = {
    "user": DASHBOARD_DB_USERNAME,
    "password": DASHBOARD_DB_PASSWORD,
    "driver": "org.postgresql.Driver"
}


CHECKPOINT_BASE_DIR = os.getenv('CHECKPOINT_BASE_DIR', 'file:///tmp/dimagi-lake/kafka_offsets')
HQ_DATA_PATH = os.getenv('HQ_DATA_PATH', 'file:///tmp/dimagi-lake/commcare')
AGG_DATA_PATH = os.getenv('AGG_DATA_PATH', 'file:///tmp/dimagi-lake/aggregated')
