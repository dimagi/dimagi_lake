
KAFKA_CASE_TOPIC = 'case-sql'
KAFKA_FORM_TOPIC = 'form-sql'
KAFKA_BOOTSTRAP_SERVER_HOST = 'localhost'
KAFKA_BOOTSTRAP_SERVER_PORT = 9092
HADOOP_SERVER_HOST = 'localhost'
HADOOP_SERVER_PORT = 9000

METASTORE_HOST = 'localhost'
METASTORE_PORT = 5432
METASTORE_DB = 'metastore_db'
METASTORE_USERNAME = ''
METASTORE_PASSWORD = ''

METASTORE_CONNECT_URI = f'jdbc:postgresql://{METASTORE_HOST}:{METASTORE_PORT}/{METASTORE_DB}?ceateDatabaseIfNotExist=true'

CHECKPOINT_BASE_DIR = f'hdfs://{HADOOP_SERVER_HOST}:{HADOOP_SERVER_PORT}/kafka_offsets'
HQ_DATA_PATH = f'hdfs://{HADOOP_SERVER_HOST}:{HADOOP_SERVER_PORT}/commcare'
DATABASE_SETTINGS = {
    'host': 'localhost',
    'database': 'dimagi',
    'user': 'dimagi',
    'password': 'dimagi'
}
MAX_RECORDS_TO_PROCESS = 500
DATA_LAKE_DOMAIN = 'nutrition-project'

ALLOWED_FORMS = [
        "http://openrosa.org/formdesigner/23145798-6570-4EEA-BF9F-17AA80D99103",
        "http://openrosa.org/formdesigner/EB3CBEB0-396E-438B-AA81-DCC3CCD4E55E",
]
ALLOWED_CASES = [
        "person_case"
]
