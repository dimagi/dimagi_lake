import json
from datalake_conts import (
    HQ_DATA_PATH,
    KAFKA_CASE_TOPIC,
    MAX_RECORDS_TO_PROCESS,
    CHECKPOINT_BASE_DIR,
    ALLOWED_FORMS,
    ALLOWED_CASES,
    DATA_LAKE_DOMAIN
)
from collections import defaultdict
from src.ingestion.record_processor import FormProcessor, CaseProcessor
from src.ingestion.datalake_writer import DatalakeWriter


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

        for domain, messages in splitted_messages.items():
            for doc_type, record_ids in messages.items():
                data_url = f"{HQ_DATA_PATH}/{domain}/{self.topic}/{doc_type}"
                table_name = f"{domain}_{self.topic}_{doc_type}".replace('-', '_').lower()
                record_processor = self.processor_cls(domain, record_ids)
                datalake_writer = DatalakeWriter(data_url)

                processed_records = record_processor.get_processed_records()
                datalake_writer.write_data(table_name, processed_records)

    def pull_messages_since_last_read(self):
        df = (self.spark_session.readStream
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
                    msg['domain'] in DATA_LAKE_DOMAIN):
                continue
            splitted_messages[msg_meta['domain']][msg_meta['document_subtype']].append(msg_meta['document_id'])
        return splitted_messages

    def reset_check_point(self):
        pass
