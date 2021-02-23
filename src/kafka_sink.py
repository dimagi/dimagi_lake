import json
from src.pull_records import pull_cases_with_meta, pull_forms_with_meta, pull_ledgers_with_meta
from src.settings import (
    HQ_DATA_PATH,
    KAFKA_CASE_TOPIC,
    KAFKA_FORM_TOPIC,
    MAX_RECORDS_TO_PROCESS,
    CHECKPOINT_BASE_DIR,
    METASTORE_DB
)
from corehq.apps.es import users
from corehq.apps.locations.models import SQLLocation


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
        self.table_name = self.topic.replace('-', '_')
        self.spark_session.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")

    def sink_kafka(self):
        kafka_messages = self.pull_messages_since_last_read()

        def process_records(kafka_msgs_df, batch_id):
            if kafka_msgs_df.count() == 0:
                print("NO RECORDS")
                return
            all_records = self.get_all_records(kafka_msgs_df.select('value').collect())
            self.merge_location_information(all_records)
            self.bulk_merge(all_records)
            self.repartition_records()

        query = (kafka_messages.writeStream
                 .foreachBatch(process_records)
                 .option("checkpointLocation", f"{CHECKPOINT_BASE_DIR}/{self.topic}")
                 .start())
        query.awaitTermination()

    def pull_messages_since_last_read(self):
        df = (self.spark_session.readStream
              .format("kafka")
              .option("kafka.bootstrap.servers", self.bootstrap_server)
              .option("subscribe", self.topic)
              .option("maxOffsetsPerTrigger", MAX_RECORDS_TO_PROCESS)
              .load())
        return df

    def get_all_records(self, record_metadata):

        if self.topic == KAFKA_CASE_TOPIC:
            return pull_cases_with_meta(record_metadata)
        elif self.topic == KAFKA_FORM_TOPIC:
            return pull_forms_with_meta(record_metadata)

    def merge_location_information(self, records):
        user_ids = [record['user_id'] for record in records]
        user_with_loc = {user['_id']: user['location_id'] for user in (users.UserES()
                                                                        .user_ids(user_ids)
                                                                        .fields(['_id', 'location_id'])
                                                                        .run().hits)}

        for doc in records:
            location_id = user_with_loc[doc['user_id']]
            ancestors = SQLLocation.by_location_id(location_id).get_ancestors(include_self=True)
            location_info = dict()
            for loc in ancestors:
                location_type = loc.location_type.name
                location_info[f"{location_type}_id"] = loc.location_id
                location_info[f"{location_type}_name"] = loc.name

            doc['location_info'] = location_info

    def bulk_merge(self, all_records):
        docs_df = (self.spark_session
                   .read.json(self.spark_session
                              .sparkContext.parallelize([json.dumps(doc).replace(": []",": [{}]") for doc in all_records])))

        if self.topic in self.db_tables:
            docs_df.createOrReplaceTempView(f"{self.table_name}_updates")
            self.spark_session.sql(self.merge_query(existing_tablename=self.table_name,
                                                    updates_tablename=f"{self.table_name}_updates"))

            # This commented Code is actually more straight forward way to merge data.
            # But because the bug(fixed in https://github.com/apache/spark/pull/29667) it Throws error.
            # This can be used when next version of spark is released with above fix.
            #
            # delta_table = DeltaTable.forPath(self.spark_session, records_table)
            # delta_table.alias("existing_docs").merge(
            #     docs_df.alias("incoming_docs"),
            #     "existing_docs.type=incoming_docs.type and existing_docs._id = incoming_docs._id ") \
            #     .whenMatchedUpdateAll() \
            #     .whenNotMatchedInsertAll() \
            #     .execute()
        else:
            print(f"New Table is being created with name {self.topic}")
            docs_df.write.partitionBy('type', 'supervisor_id').saveAsTable(self.table_name,
                                                                           format='delta',
                                                                           mode='overwrite',
                                                                           path=f"{HQ_DATA_PATH}/{self.topic}")
            self.db_tables = [table.name for table in self.spark_session.catalog.listTables('default')]

        print(f"{len(docs)} docs were written to {self.topic}")

    def merge_query(self, existing_tablename, updates_tablename):
        return f"""
        MERGE INTO "{existing_tablename}" existing_records 
        USING "{updates_tablename}" updates 
        ON existing_records.type = updates.type AND existing_records._id = updates._id 
        WHEN MATCHED THEN UPDATE SET * 
        WHEN NOT MATCHED THEN INSERT *
        """

    def repartition_records(self):
        print("REPARTITIONING")
        records_table = f"{HQ_DATA_PATH}/{self.topic}"
        records = self.spark_session.read.format('delta').load(records_table)
        records.coalesce(2).write.format('delta')\
            .partitionBy('type', 'supervisor_id')\
            .mode('overwrite').save(records_table)
