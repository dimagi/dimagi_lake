import json
from src.pull_records import pull_cases_with_meta, pull_forms_with_meta, pull_ledgers_with_meta
from src.spark_session_handler import SPARK
from delta.tables import DeltaTable
from pyspark.sql.utils import AnalysisException
from src.settings import HQ_DATA_PATH, KAFKA_CASE_TOPIC, MAX_RECORDS_TO_PROCESS, CHECKPOINT_BASE_DIR


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

    def sink_kafka(self):
        kafka_messages = self.pull_messages_since_last_read()

        def process_records(kafka_msgs_df, batch_id):
            docs_meta = kafka_msgs_df.select('value').collect()
            all_records = self.get_all_records(docs_meta)
            self.merge_location_information(all_records)
            self.bulk_merge(all_records)
            self.repartition_records()

        kafka_messages.writeStream.foreachBatch(process_records).option("checkpointLocation",
                                                                        f"{CHECKPOINT_BASE_DIR}/{self.topic}").start()

    def pull_messages_since_last_read(self):
        df = (self.spark_session.readSteam
              .format("kafka")
              .option("kafka.bootstrap.servers", self.bootstrap_server)
              .option("subscribe", self.topic)
              .option("maxOffsetsPerTrigger", MAX_RECORDS_TO_PROCESS)
              .load())
        return df

    def get_all_records(self, record_metadata):

        if self.topic == 'generator-case':
            return pull_cases_with_meta(record_metadata)
        elif self.topic == 'generator-form':
            return pull_forms_with_meta(record_metadata)
        else:
            return pull_ledgers_with_meta(record_metadata)

    # TODO change this while integrating with HQ
    def merge_location_information(self, records):
        for doc in records:
            if self.topic == KAFKA_CASE_TOPIC:
                flw_num = doc['owner_id'].split("_")[1]
            else:
                flw_num = doc['user_id'].split("_")[1]
            doc['supervisor_id'] = f'supervisor_{flw_num}'
            doc['block_id'] = f'block_{flw_num}'
            doc['district_id'] = f'district_{flw_num}'
            doc['state_id'] = f'state_{flw_num}'

    def bulk_merge(self, all_records):
        docs_df = SPARK.read.json(SPARK.sparkContext.parallelize([json.dumps(doc) for doc in all_records]))
        try:
            print(f"{len(docs)} docs being written")
            records_table = f"{HQ_DATA_PATH}/{self.topic}"
            delta_table = DeltaTable.forPath(SPARK, records_table)
            delta_table.alias("existing_docs").merge(
                docs_df.alias("incoming_docs"),
                "existing_docs.type=incoming_docs.type and existing_docs._id = incoming_docs._id ") \
                .whenMatchedUpdateAll() \
                .whenNotMatchedInsertAll() \
                .execute()
        except AnalysisException:
            print("FIRST RECORD BEING WRITTEN")
            docs_df.coalesce(2).write.format('delta').partitionBy('type', 'supervisor_id').mode('overwrite').save(records_table)

    def repartition_records(self):
        pass
