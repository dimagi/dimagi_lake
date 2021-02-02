from src.spark_session_handler import SPARK
from src.settings import KAFKA_OFFSET_PATH
from pyspark.sql.functions import max as pyspark_max
from pyspark.sql import Row
from pyspark.sql.utils import AnalysisException


class SparkOffset:

    def __init__(self, topic, partition, offset=-2):
        self.topic = topic
        self.partition = partition
        self.offset = offset

    @classmethod
    def get_latest_offset_by_topic_partition(cls, topic, partition):
        try:
            offset_df = SPARK.read.format('delta').load(KAFKA_OFFSET_PATH)
        except AnalysisException:
            print("INITIALIZING OFFSETS")
            offset_df = cls.initialize_offset_table(topic, partition).to_spark_df()

        return (offset_df
                .filter((offset_df['topic'] == topic) & (offset_df['partition'] == partition))
                .agg(pyspark_max('offset').alias('max_offset'))
                .collect()[0].max_offset
                )

    @classmethod
    def initialize_offset_table(cls, topic, partition):
        spark_offset_obj = cls(topic=topic, partition=partition)
        spark_offset_obj.save()
        return spark_offset_obj

    def save(self):
        self.to_spark_df().write.format('delta').mode('append').save(KAFKA_OFFSET_PATH)

    def to_spark_df(self):
        offset_row = Row(topic=self.topic, partition=self.partition, offset=self.offset)
        return SPARK.createDataFrame([offset_row])
