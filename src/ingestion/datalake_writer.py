from spark_session_handler import SPARK
from delta.tables import DeltaTable


class DatalakeWriter:
    available_tables = [table.name for table in SPARK.catalog.listTables('default')]

    def __init__(self, data_url):
        self.data_url = data_url
        SPARK.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")

    def write_data(self, table_name, data_df, partition_columns):
        if table_name not in DatalakeWriter.available_tables:
            self.write_new_table(table_name, data_df, partition_columns)
            DatalakeWriter.available_tables.append(table_name)
        else:
            self.upsert(table_name, data_df)

    def write_new_table(self, table_name, data_df, partition_columns):
        data_df.write.partitionBy(*partition_columns) \
            .option('overwriteSchema', True) \
            .saveAsTable(table_name,
                         format='delta',
                         mode='overwrite',
                         path=self.data_url)

    def upsert(self, table_name, data_df):
        data_df.createOrReplaceTempView(f"{table_name}_updates")
        SPARK.sql(self.__merge_query(existing_tablename=table_name,
                                     updates_tablename=f"{table_name}_updates"))

    def delete(self, table_name, record_ids):
        if table_name in DatalakeWriter.available_tables:
            record_ids_param = ",'".join(record_ids)
            SPARK.sql(f"DELETE from {table_name} where _id in ('{record_ids_param}')")
        else:
            print("TABLE DOES NOT EXISTS")

    def repartition(self):
        records = SPARK.read.format('delta').load(self.data_url)
        records.coalesce(3).write.format('delta').partitionBy('type', 'supervisor_id').mode('overwrite').save(self.data_url)

    def clear_older_versions(self, hours_old=0):
        delta_table = DeltaTable.forPath(SPARK, self.data_url)
        delta_table.vacuum(hours_old)

    def __merge_query(self, existing_tablename, updates_tablename):
        return f"""
        MERGE INTO {existing_tablename} existing_records 
        USING {updates_tablename} updates 
        ON existing_records._id = updates._id 
        WHEN MATCHED THEN UPDATE SET * 
        WHEN NOT MATCHED THEN INSERT *
        """
