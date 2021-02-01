import os

KAFKA_CASE_TOPIC = 'generator-case'
KAFKA_FORM_TOPIC = 'generator-form'
KAFKA_LEDGER_TOPIC = 'generator-ledger'
KAFKA_BOOTSTRAP_SERVER_HOST = os.getenv('KAFKA_BOOTSTRAP_SERVER_HOST', 'localhost')
KAFKA_BOOTSTRAP_SERVER_PORT = os.getenv('KAFKA_BOOTSTRAP_SERVER_PORT', 'localhost')
HADOOP_SERVER_HOST = os.getenv('HADOOP_SERVER_HOST', 'localhost')
HADOOP_SERVER_PORT = os.getenv('HADOOP_SERVER_PORT', '9000')
HDFS_HQ_DATA_TABLE_DIR = os.getenv('HDFS_HQ_DATA_TABLE_DIR', '')
HDFS_KAFKA_OFFSET_TABLE_DIR = os.getenv('HDFS_KAFKA_OFFSET_TABLE_DIR', '')

DATABASE_SETTINGS = {
    'host': os.get('DATABASE_HOST'),
    'database': os.get('DATABASE_NAME'),
    'user': os.get('DATABASE_USER'),
    'password': os.get('DATABASE_PASS')
}
