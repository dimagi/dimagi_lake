import json
from src.pull_records import pull_cases_by_ids, pull_forms_by_ids
from src.settings import (
    HQ_DATA_PATH,
    KAFKA_CASE_TOPIC,
    KAFKA_FORM_TOPIC,
    MAX_RECORDS_TO_PROCESS,
    CHECKPOINT_BASE_DIR,
    ALLOWED_FORMS,
    ALLOWED_CASES
)
from corehq.apps.es import users
from corehq.apps.locations.models import SQLLocation
from corehq.util.json import CommCareJSONEncoder
from collections import defaultdict
from src.utils import get_flatten_df


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
        self.db_tables = [table.name for table in self.spark_session.catalog.listTables('default')]
        self.spark_session.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")

    def sink_kafka(self):
        kafka_messages = self.pull_messages_since_last_read()

        def process_records(kafka_msgs_df, batch_id):

            splitted_messages = self.split_message_by_domain(kafka_msgs_df)
            if len(splitted_messages) == 0:
                print("NO RECORDS")
                return

            for domain, messages in splitted_messages:
                all_records = self.get_all_records(domain, messages)
                self.merge_location_information(all_records)
                self.bulk_merge(all_records)
                self.repartition_records()

        query = (kafka_messages.writeStream
                 .foreachBatch(process_records)
                 .option("checkpointLocation", f"{CHECKPOINT_BASE_DIR}/{self.topic}")
                 .start())
        query.awaitTermination()

    def split_message_by_domain(self, kafka_msgs_df):
        kafka_messages = kafka_msgs_df.select('value').collect()

        splitted_messages = defaultdict(list)
        for msg in kafka_messages:
            msg_meta = json.loads(msg.value)
            if not (msg_meta['document_subtype'] in ALLOWED_CASES or
                    msg_meta['document_subtype'] in ALLOWED_FORMS):
                continue
            splitted_messages[msg_meta['domain']].append(msg_meta['document_id'])
        return splitted_messages

    def pull_messages_since_last_read(self):
        df = (self.spark_session.readStream
              .format("kafka")
              .option("kafka.bootstrap.servers", self.bootstrap_server)
              .option("subscribe", self.topic)
              .option("maxOffsetsPerTrigger", MAX_RECORDS_TO_PROCESS)
              .load())
        return df

    def get_all_records(self, domain, record_metadata):

        if self.topic == KAFKA_CASE_TOPIC:
            return pull_cases_by_ids(domain, record_metadata)
        elif self.topic == KAFKA_FORM_TOPIC:
            return pull_forms_by_ids(domain, record_metadata)

    def merge_location_information(self, records):
        def get_user_id(record):
            return record.get('user_id') or record.get('meta', {}).get('userID')

        user_ids = [get_user_id(record) for record in records if get_user_id(record)]
        user_with_loc = {user['_id']: user['location_id'] for user in (users.UserES()
                                                                        .user_ids(user_ids)
                                                                        .fields(['_id', 'location_id'])
                                                                        .run().hits)}
        for doc in records:
            if not get_user_id(doc):
                continue
            location_id = user_with_loc[get_user_id(doc)]
            ancestors = SQLLocation.by_location_id(location_id).get_ancestors(include_self=True)
            for loc in ancestors:
                location_type = loc.location_type.name
                doc[f"{location_type}_id"] = loc.location_id
                doc[f"{location_type}_name"] = loc.name

    def bulk_merge(self, domain, all_records):

        docs_df = (self.spark_session
                   .read.json(self.spark_session
                              .sparkContext.parallelize([json.dumps(doc, cls=CommCareJSONEncoder).replace(": []", ": [{}]") for doc in all_records])))

        flattened_df = get_flatten_df(docs_df)
        table_name = f"{domain}_{self.topic}".replace('-', '_').lower()
        if table_name in self.db_tables:
            flattened_df.createOrReplaceTempView(f"{table_name}_updates")
            self.spark_session.sql(self.merge_query(existing_tablename=table_name,
                                                    updates_tablename=f"{table_name}_updates"))
        else:
            print(f"New Table is being created with name {self.topic}")
            flattened_df.write.partitionBy('type', 'supervisor_id')\
                .option('overwriteSchema', True)\
                .saveAsTable(table_name,
                             format='delta',
                             mode='overwrite',
                             path=f"{HQ_DATA_PATH}/{domain}/{self.topic}/{table_name}")
            self.db_tables = [table.name for table in self.spark_session.catalog.listTables('default')]

        print(f"{len(all_records)} docs were written to {self.topic}")

    def merge_query(self, existing_tablename, updates_tablename):
        return f"""
        MERGE INTO {existing_tablename} existing_records 
        USING {updates_tablename} updates 
        ON existing_records.type = updates.type AND existing_records._id = updates._id 
        WHEN MATCHED THEN UPDATE SET * 
        WHEN NOT MATCHED THEN INSERT *
        """

    def repartition_records(self):
        pass
        # print("REPARTITIONING")
        # records_table = f"{HQ_DATA_PATH}/{self.topic}"
        # records = self.spark_session.read.format('delta').load(records_table)
        # records.coalesce(2).write.format('delta')\
        #     .partitionBy('type', 'supervisor_id')\
        #     .mode('overwrite').save(records_table)
