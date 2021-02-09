from src.offset_manager import SparkOffset
import json
import base64
from pyspark.sql.functions import max as pyspark_max, min as pyspark_min, to_json as pyspark_json, struct
from src.pull_records import pull_cases_with_meta, pull_forms_with_meta, pull_ledgers_with_meta
from src.spark_session_handler import SPARK
from delta.tables import DeltaTable
from pyspark.sql.utils import AnalysisException
from src.settings import HQ_DATA_PATH

class KafkaSink:

    spark_session = None
    topic = None
    partition = 0
    bootstrap_server = None

    def __init__(self, spark_session, topic, partition, bootstrap_server):
        self.spark_session = spark_session
        self.topic = topic
        self.partition = partition
        self.bootstrap_server = bootstrap_server

    @property
    def latest_offset(self):
        return SparkOffset.get_latest_offset_by_topic_partition(
            topic=self.topic,
            partition=self.partition
        )

    def sink_kafka(self):
        kafka_messages = self.pull_messages_since_last_read()

        if kafka_messages.count() == 0:
            print("ALL MESSAGES FROM KAFKA ARE READ, NONE LEFT!!")
            return

        min_offset = kafka_messages.agg(pyspark_min('offset').alias('min_offset')).first().min_offset
        max_offset = kafka_messages.agg(pyspark_max('offset').alias('max_offset')).first().max_offset
        print(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>")
        print(min_offset)
        print(max_offset)
        
        decoded_kafka_messages = self.decode_kafka_message_df(kafka_messages.toJSON().collect())
        all_records = self.get_all_records(decoded_kafka_messages)
        del kafka_messages
        del decoded_kafka_messages
        self.merge_location_information(all_records)
        self.bulk_merge(all_records)
        self.repartition_records()
        self.save_offset(offset=max_offset+1)

    def decode_kafka_message_df(self, kafka_messages):
        messages = list()
        for message in kafka_messages:
            message = json.loads(message)
            message['key'] = base64.b64decode(message['key'])
            message['value'] = json.loads(base64.b64decode(message['value']))
            messages.append(message)
        return messages

    def pull_messages_since_last_read(self):
        df = (self.spark_session.read
              .format("kafka")
              .option("kafka.bootstrap.servers", self.bootstrap_server)
              .option("subscribe", self.topic)
              .option("startingOffsets", json.dumps({self.topic: {"0": self.latest_offset}}))
              .load())


        return df if df.count()<=50000 else self.spark_session.createDataFrame(df.head(50000))

    def get_all_records(self, record_metadata):

        if self.topic == 'generator-case':
            return pull_cases_with_meta(record_metadata)
        elif self.topic == 'generator-form':
            return pull_forms_with_meta(record_metadata)
        else:
            return pull_ledgers_with_meta(record_metadata)

    # TODO change this while integrating with HQ
    def merge_location_information(self, records):
        for doc_type, docs in records.items():
            for doc in docs:
                flw_num = doc['owner_id'].split("_")[1]
                doc['supervisor_id'] = f'supervisor_{flw_num}'
                doc['block_id'] = f'block_{flw_num}'
                doc['district_id'] = f'district_{flw_num}'
                doc['state_id'] = f'state_{flw_num}'
            records[doc_type] = docs

    def bulk_merge(self, all_records):
        for doc_type, docs in all_records.items():
            cases_df = SPARK.read.json(SPARK.sparkContext.parallelize([json.dumps(doc) for doc in docs]))
            try:
                print(f"{len(docs)} docs being written")
                records_table = f"{HQ_DATA_PATH}/{doc_type}"
                deltaTable = DeltaTable.forPath(SPARK, records_table)
                deltaTable.alias("existing_cases").merge(
                    cases_df.alias("incoming_cases"),
                    "existing_cases.case_id = incoming_cases.case_id and existing_cases.district_id=incoming_cases.district_id") \
                    .whenMatchedUpdateAll() \
                    .whenNotMatchedInsertAll() \
                    .execute()
            except AnalysisException:
                print("FIRST RECORD BEING WRITTEN")
                cases_df.coalesce(3).write.format('delta').partitionBy("district_id").mode('overwrite').save(records_table)

    def repartition_records(self):
        pass

    def save_offset(self, offset):
        SparkOffset(topic=self.topic, partition=0, offset=offset).save()


