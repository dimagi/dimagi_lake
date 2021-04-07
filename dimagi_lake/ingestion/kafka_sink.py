import json
from collections import defaultdict
import localsettings
from consts import (ALLOWED_CASES, ALLOWED_FORMS, DATA_LAKE_DOMAIN,
                    KAFKA_CASE_TOPIC, KAFKA_FORM_TOPIC, KAFKA_LOCATION_TOPIC,
                    MAX_RECORDS_TO_PROCESS)
from dimagi_lake.ingestion.datalake_writer import DatalakeWriter
from dimagi_lake.ingestion.record_processor import (CaseProcessor,
                                                    FormProcessor,
                                                    LocationProcessor)
from dimagi_lake.ingestion.utils import trim_xmlns_id
from spark_session_handler import SPARK


class BaseKafkaSink:
    topic = None
    bootstrap_server = None
    processor_cls = None

    def __init__(self, bootstrap_server):
        self.bootstrap_server = bootstrap_server

    def sink_kafka(self):
        kafka_messages = self.pull_messages_since_last_read()

        query = (kafka_messages.writeStream
                 .foreachBatch(self.process_batch)
                 .option("checkpointLocation", f"{localsettings.CHECKPOINT_BASE_DIR}/{self.topic}")
                 .start())
        query.awaitTermination()

    def process_batch(self, kafka_msgs_df, batch_id):
        records_to_upsert, records_to_delete = self.get_splitted_records(kafka_msgs_df)

        if len(records_to_upsert)>0:
            self.upsert_records(records_to_upsert)
        if len(records_to_delete)>0:
            self.delete_records(records_to_delete)

    def delete_records(self, records_to_delete):
        for domain, messages in records_to_delete.items():
            for doc_type, record_ids in messages.items():
                data_url = f"{localsettings.HQ_DATA_PATH}/{domain}/{self.topic}/{doc_type}"
                table_name = f"{domain}_{self.topic}_{doc_type}".replace('-', '_').lower()
                datalake_writer = DatalakeWriter(data_url)
                datalake_writer.delete(table_name, record_ids)

    def upsert_records(self, records_to_upsert):
        total_records_processed = 0
        for domain, messages in records_to_upsert.items():
            for doc_type, record_ids in messages.items():
                data_url = f"{localsettings.HQ_DATA_PATH}/{domain}/{self.topic}/{doc_type}"
                table_name = f"{domain}_{self.topic}_{doc_type}".replace('-', '_').lower()
                record_processor = self.processor_cls(domain, record_ids)
                datalake_writer = DatalakeWriter(data_url)

                processed_records = record_processor.get_processed_records()
                if processed_records.count()>0:
                    partition_columns = self.processor_cls.partition_columns
                    datalake_writer.write_data(table_name, processed_records, partition_columns) # TODO Write in parallel
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

    def get_splitted_records(self, kafka_msgs_df):
        pass

    def reset_check_point(self):
        pass


class CaseKafkaSink(BaseKafkaSink):
    processor_cls = CaseProcessor
    topic = KAFKA_CASE_TOPIC
    def get_splitted_records(self, kafka_msgs_df):
        kafka_messages = kafka_msgs_df.select('value').collect()

        records_to_upsert = defaultdict(lambda: defaultdict(list))
        records_to_delete = defaultdict(lambda: defaultdict(list))
        for msg in kafka_messages:
            msg_meta = json.loads(msg.value)
            if not (msg_meta['document_subtype'] in ALLOWED_CASES and
                    msg_meta['domain'] in DATA_LAKE_DOMAIN):
                continue
            if msg_meta['is_deletion']:
                records_to_delete[msg_meta['domain']][msg_meta['document_subtype']].append(msg_meta['document_id'])
            else:
                records_to_upsert[msg_meta['domain']][msg_meta['document_subtype']].append(msg_meta['document_id'])
        return records_to_upsert, records_to_delete


class FormKafkaSink(BaseKafkaSink):
    processor_cls = FormProcessor
    topic = KAFKA_FORM_TOPIC

    def get_splitted_records(self, kafka_msgs_df):
        kafka_messages = kafka_msgs_df.select('value').collect()

        records_to_upsert = defaultdict(lambda: defaultdict(list))
        records_to_delete = defaultdict(lambda: defaultdict(list))
        for msg in kafka_messages:
            msg_meta = json.loads(msg.value)
            if not (msg_meta['document_subtype'] in ALLOWED_FORMS and
                    msg_meta['domain'] in DATA_LAKE_DOMAIN):
                continue
            record_type = trim_xmlns_id(msg_meta['document_subtype'])
            if msg_meta['is_deletion']:
                records_to_delete[msg_meta['domain']][record_type].append(msg_meta['document_id'])
            else:
                records_to_upsert[msg_meta['domain']][record_type].append(msg_meta['document_id'])
        return records_to_upsert, records_to_delete


class LocationKafkaSink(BaseKafkaSink):
    processor_cls = LocationProcessor
    topic = KAFKA_LOCATION_TOPIC

    def get_splitted_records(self, kafka_msgs_df):
        kafka_messages = kafka_msgs_df.select('value').collect()

        records_to_upsert = defaultdict(lambda: defaultdict(list))
        records_to_delete = defaultdict(lambda: defaultdict(list))
        for msg in kafka_messages:
            msg_meta = json.loads(msg.value)
            if msg_meta['domain'] not in DATA_LAKE_DOMAIN:
                continue
            if msg_meta['is_deletion']:
                records_to_delete[msg_meta['domain']][msg_meta['document_type']].append(msg_meta['document_id'])
            else:
                records_to_upsert[msg_meta['domain']][msg_meta['document_type']].append(msg_meta['document_id'])
        return records_to_upsert, records_to_delete
