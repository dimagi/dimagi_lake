from abc import ABC

from pyspark.sql.types import IntegerType, StringType, StructField, StructType

import env_settings
from consts import FLWC_LOCATION_TABLE
from dimagi_lake.aggregation.aggregation_helpers.agg_location import \
    AggLocationHelper
from dimagi_lake.aggregation.sql.sql_utils import (attach_partition,
                                                   connect_to_db, create_table,
                                                   detach_partition,
                                                   drop_table, rename_table)
from dimagi_lake.utils import clean_name, get_db_name
from spark_session_handler import SPARK


class BaseTable(ABC):
    """
    Responsible For:
    1. Defining final schema required for a particular spark table.
    2. Writing data to Datalake
    3. Load data to PostgreSQL
    4. Invoke relevant aggregation helper class to aggregate data.
    """
    _aggregator = None
    _warehouse_base_table = None

    def __init__(self, domain, month):
        self._domain = domain
        self._month = month
        self._database_name = get_db_name(domain)
        self.datalake_tablename = f"{clean_name(self._database_name)}.{clean_name(self._warehouse_base_table)}"
        self.datalake_tablepath = f'{env_settings.AGG_DATA_PATH}/{self._domain}/{self._warehouse_base_table}'

    def write_to_datalake(self, df):
        """
        Write New Data to datalake overwriting existing one.
        """
        (df.write.partitionBy(*self._partition_columns)
         .option('overwriteSchema', True)  # Ensures overwriting existing Schema with new one
         .saveAsTable(self.datalake_tablename,
                      format='delta',  # Ensures to write data with delta logs, important to ensure ACID writes
                      mode='overwrite',  # Ensures overwriting existing Data with new one
                      path=self.datalake_tablepath))


class FlwcLocation(BaseTable):

    _aggregator = AggLocationHelper
    _warehouse_base_table = FLWC_LOCATION_TABLE
    _partition_columns = ('location_level',)

    # Schema needs to be kept in sync with the dashboard django model
    schema = StructType(fields=[
        StructField('flwc_id', StringType(), True),
        StructField('flwc_site_code', StringType(), True),
        StructField('flwc_name', StringType(), True),
        StructField('supervisor_id', StringType(), True),
        StructField('supervisor_site_code', StringType(), True),
        StructField('supervisor_name', StringType(), True),
        StructField('project_id', StringType(), True),
        StructField('project_site_code', StringType(), True),
        StructField('project_name', StringType(), True),
        StructField('district_id', StringType(), True),
        StructField('district_site_code', StringType(), True),
        StructField('district_name', StringType(), True),
        StructField('state_id', StringType(), True),
        StructField('state_site_code', StringType(), True),
        StructField('state_name', StringType(), True),
        StructField('location_level', IntegerType(), True),
        StructField('domain', StringType(), True),
    ])

    def aggregate(self):
        aggregator = self._aggregator(self._database_name,
                                      self._domain,
                                      self._month,
                                      self.schema)
        agg_df = aggregator.aggregate()
        self.write_to_datalake(agg_df)
        # agg_df.show(110,False)

    def write_to_warehouse(self):
        """
        Method loads Spark Data table data to Postgres DB used by Dashboard.
        Order of each SQL query matters
        """
        df = SPARK.sql(f'select * from {self.datalake_tablename}')
        child_table = f"{self._warehouse_base_table}_{clean_name(self._domain)}"
        staging_table = f"{child_table}_stg"
        prev_table = f"{child_table}_prev"
        conn = connect_to_db()

        try:
            with conn:
                with conn.cursor() as cursor:
                    drop_table(cursor, staging_table)
                    drop_table(cursor, prev_table)
                    create_table(cursor, self._warehouse_base_table, staging_table, self._domain)

            df.write.jdbc(url=env_settings.DASHBOARD_JDBC_URL,
                          table=staging_table,
                          mode='append',
                          properties=env_settings.JDBC_PROPS)

            with conn:
                with conn.cursor() as cursor:  # Do all following operations in a single transaction
                    detach_partition(cursor, self._warehouse_base_table, child_table)
                    rename_table(cursor, child_table, prev_table)
                    rename_table(cursor, staging_table, child_table)
                    attach_partition(cursor, self._warehouse_base_table, child_table)
                    drop_table(cursor, prev_table)
        finally:
            conn.close()
