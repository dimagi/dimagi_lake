from pyspark.sql import SparkSession


SPARK = (SparkSession.builder
         .appName('my_awesome')
         .config('spark.sql.shuffle.partitions', '2')
         .config('spark.databricks.delta.merge.repartitionBeforeWrite.enabled', 'true')
         .getOrCreate())
SPARK.sparkContext.setLogLevel("ERROR")
