import json
from datalake_conts import (
    HQ_DATA_PATH,
    KAFKA_CASE_TOPIC,
    KAFKA_FORM_TOPIC,
    MAX_RECORDS_TO_PROCESS,
    CHECKPOINT_BASE_DIR,
    ALLOWED_FORMS,
    ALLOWED_CASES,
    DATA_LAKE_DOMAIN
)
from collections import defaultdict
from src.ingestion.record_processor import FormProcessor, CaseProcessor
from src.ingestion.datalake_writer import DatalakeWriter
from spark_session_handler import SPARK
from src.ingestion.utils import trim_xmlns_id


class KafkaSink:
    topic = None
    bootstrap_server = None

    def __init__(self, topic, bootstrap_server):
        self.topic = topic
        self.bootstrap_server = bootstrap_server
        self.processor_cls = CaseProcessor if topic == KAFKA_CASE_TOPIC else FormProcessor

    def sink_kafka(self):
        kafka_messages = self.pull_messages_since_last_read()

        query = (kafka_messages.writeStream
                 .foreachBatch(self.process_batch)
                 .option("checkpointLocation", f"{CHECKPOINT_BASE_DIR}/{self.topic}")
                 .start())
        query.awaitTermination()

    def process_batch(self, kafka_msgs_df, batch_id):
        splitted_messages = self.split_message_by_domain(kafka_msgs_df)
        if len(splitted_messages) == 0:
            print("NO RECORDS")
            return

        total_records_processed = 0
        for domain, messages in splitted_messages.items():
            for doc_type, record_ids in messages.items():
                record_type = doc_type
                if self.topic == KAFKA_FORM_TOPIC:
                    record_type = trim_xmlns_id(doc_type)
                data_url = f"{HQ_DATA_PATH}/{domain}/{self.topic}/{record_type}"
                table_name = f"{domain}_{self.topic}_{record_type}".replace('-', '_').lower()
                record_processor = self.processor_cls(domain, record_ids)
                datalake_writer = DatalakeWriter(data_url)

                processed_records = record_processor.get_processed_records()

                # TODO Write in parallel
                datalake_writer.write_data(table_name, processed_records)
                total_records_processed += len(record_ids)
        print("COMPLETED BATCH OF {} records".format(total_records_processed))

    def pull_messages_since_last_read(self):
        df = (SPARK.readStream
              .format("kafka")
              .option("kafka.bootstrap.servers", self.bootstrap_server)
              .option("subscribe", self.topic)
              .option("maxOffsetsPerTrigger", MAX_RECORDS_TO_PROCESS)
              .load())
        return df

    def split_message_by_domain(self, kafka_msgs_df):
        kafka_messages = kafka_msgs_df.select('value').collect()

        splitted_messages = defaultdict(lambda: defaultdict(list))
        for msg in kafka_messages:
            msg_meta = json.loads(msg.value)
            if not ((msg_meta['document_subtype'] in ALLOWED_CASES or
                    msg_meta['document_subtype'] in ALLOWED_FORMS) and
                    msg_meta['domain'] in DATA_LAKE_DOMAIN):
                continue
            splitted_messages[msg_meta['domain']][msg_meta['document_subtype']].append(msg_meta['document_id'])
        return splitted_messages

    def reset_check_point(self):
        pass
