from pyspark.sql import SparkSession

import env_settings

SPARK = (SparkSession.builder
         .appName('my_awesome')
         .config('spark.sql.shuffle.partitions', '2')
         .config('spark.databricks.delta.merge.repartitionBeforeWrite.enabled', 'true')
        #  .config("javax.jdo.option.ConnectionURL", env_settings.METASTORE_CONNECT_URI)
        #  .config("javax.jdo.option.ConnectionDriverName", "org.postgresql.Driver")
        #  .config("javax.jdo.option.ConnectionUserName", env_settings.METASTORE_USERNAME)
        #  .config("javax.jdo.option.ConnectionPassword", env_settings.METASTORE_PASSWORD)
         .enableHiveSupport()
         .getOrCreate())
SPARK.sparkContext.setLogLevel("ERROR")
SPARK._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.ap-south-1.amazonaws.com")