from spark_session_handler import SPARK
from delta.tables import DeltaTable


class DatalakeWriter:
    available_tables = [table.name for table in SPARK.catalog.listTables('default')]

    def __init__(self, data_url):
        self.data_url = data_url
        SPARK.sql("SET spark.databricks.delta.schema.autoMerge.enabled = true")

    def write_data(self, table_name, data_df):
        if table_name not in DatalakeWriter.available_tables:
            self.write_new_table(table_name, data_df)
            DatalakeWriter.available_tables.append(table_name)
        else:
            self.upsert_data(table_name, data_df)

    def write_new_table(self, table_name, data_df):
        data_df.write.partitionBy('month', 'supervisor_id') \
            .option('overwriteSchema', True) \
            .saveAsTable(table_name,
                         format='delta',
                         mode='overwrite',
                         path=self.data_url)

    def upsert_data(self, table_name, data_df):
        data_df.createOrReplaceTempView(f"{table_name}_updates")
        SPARK.sql(self.__merge_query(existing_tablename=table_name,
                                     updates_tablename=f"{table_name}_updates"))

    def repartition(self):
        records = SPARK.read.format('delta').load(self.data_url)
        records.coalesce(3).write.format('delta').partitionBy('type', 'supervisor_id').mode('overwrite').save(self.data_url)

    def clear_older_versions(self, hours_old=0):
        delta_table = DeltaTable.forPath(SPARK, self.data_url)
        delta_table.vacuum(0)

    def __merge_query(self, existing_tablename, updates_tablename):
        return f"""
        MERGE INTO {existing_tablename} existing_records 
        USING {updates_tablename} updates 
        ON existing_records.type = updates.type AND existing_records._id = updates._id 
        WHEN MATCHED THEN UPDATE SET * 
        WHEN NOT MATCHED THEN INSERT *
        """
