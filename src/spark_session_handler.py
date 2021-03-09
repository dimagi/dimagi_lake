from pyspark.sql import SparkSession
from src.settings import (
    METASTORE_CONNECT_URI,
    METASTORE_USERNAME,
    METASTORE_PASSWORD,
    S3_BLOB_DB_SETTINGS,
)

SPARK = (SparkSession.builder
         .appName('my_awesome')
         .config('spark.sql.shuffle.partitions', '2')
         .config('spark.databricks.delta.merge.repartitionBeforeWrite.enabled', 'true')
         .config("javax.jdo.option.ConnectionURL", METASTORE_CONNECT_URI)
         .config("javax.jdo.option.ConnectionDriverName", "org.postgresql.Driver")
         .config("javax.jdo.option.ConnectionUserName", METASTORE_USERNAME)
         .config("javax.jdo.option.ConnectionPassword", METASTORE_PASSWORD)
         .enableHiveSupport()
         .getOrCreate())
SPARK.sparkContext.setLogLevel("ERROR")
SPARK._jsc.hadoopConfiguration().set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
SPARK._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
SPARK._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
SPARK._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.ap-south-1.amazonaws.com")
SPARK._jsc.hadoopConfiguration().set("fs.s3a.access.key", S3_BLOB_DB_SETTINGS['access_key'])
SPARK._jsc.hadoopConfiguration().set("fs.s3a.secret.key", S3_BLOB_DB_SETTINGS['secret_key'])