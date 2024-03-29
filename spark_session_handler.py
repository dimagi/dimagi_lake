from pyspark.sql import SparkSession
from datalake_conts import (
    METASTORE_CONNECT_URI,
    METASTORE_USERNAME,
    METASTORE_PASSWORD,
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
SPARK._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.ap-south-1.amazonaws.com")