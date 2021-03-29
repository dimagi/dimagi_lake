from pyspark.sql.types import StringType, IntegerType, StructType, StructField, DateType

from datalake_conts import (
    FLWC_LOCATION_TABLE,
    CHILD_CARE_MONTHLY_TABLE,
    SERVICE_ENROLLMENT_TABLE,
    AGG_DATA_PATH,
    DASHBOARD_JDBC_URL,
    JDBC_PROPS,
)
from src.aggregation.aggregation_helpers.agg_location import (
    AggLocationHelper
)
from src.aggregation.sql.sql_utils import connect_to_db, create_table, detach_partition, rename_table, attach_partition, drop_table
from spark_session_handler import SPARK
from src.utils import clean_name, get_db_name


class BaseTable:
    _aggregator = None
    _warehouse_base_table = None

    def __init__(self, domain, month):
        self._domain = domain
        self._month = month
        self._database_name = get_db_name(domain)
        self.datalake_tablename = f"{clean_name(self._database_name)}.{clean_name(self._warehouse_base_table)}"
        self.datalake_tablepath = f'{AGG_DATA_PATH}/{self._domain}/{self._warehouse_base_table}'

    def write_to_datalake(self, df):
        (df.write.partitionBy(*self._partition_columns)
         .option('overwriteSchema', True)
         .saveAsTable(self.datalake_tablename,
                      format='delta',
                      mode='overwrite',
                      path=self.datalake_tablepath))


class FlwcLocation(BaseTable):
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

    _aggregator = AggLocationHelper
    _warehouse_base_table = FLWC_LOCATION_TABLE
    _partition_columns = ('location_level',)

    def aggregate(self):
        aggregator = self._aggregator(self._database_name,
                                      self._domain,
                                      self._month,
                                      self.schema)
        agg_df = aggregator.aggregate()
        self.write_to_datalake(agg_df)
        self.write_to_warehouse()

    def write_to_warehouse(self,):
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

            mode = "append"
            df.write.jdbc(url=DASHBOARD_JDBC_URL, table=staging_table, mode=mode, properties=JDBC_PROPS)

            with conn:
                with conn.cursor() as cursor:  # Do all following operations in a single transaction
                    detach_partition(cursor, self._warehouse_base_table, child_table)
                    rename_table(cursor, child_table, prev_table)
                    rename_table(cursor, staging_table, child_table)
                    attach_partition(cursor, self._warehouse_base_table, child_table)
                    drop_table(cursor, prev_table)
        finally:
            conn.close()


class ChildCareMonthly(BaseTable):
    schema = StructType(fields=[
        StructField('domain', StringType(), True),
        StructField('month', DateType(), True),
        StructField('flwc_id', StringType(), True),
        StructField('supervisor_id', StringType(), True),
        StructField('project_id', StringType(), True),
        StructField('district_id', StringType(), True),
        StructField('state_id', StringType(), True),
        StructField('case_id', StringType(), True),
        StructField('member_case_id', StringType(), True),
        StructField('mother_member_case_id', StringType(), True),
        StructField('name', StringType(), True),
        StructField('dob', DateType(), True),
        StructField('gender', StringType(), True),
        StructField('opened_on', DateType(), True),
        StructField('date_death', DateType(), True),
        StructField('age_in_months', IntegerType(), True),
        StructField('alive_in_month', IntegerType(), True),
        StructField('is_migrated', IntegerType(), True),
        StructField('want_nutrtion_services', IntegerType(), True),
        StructField('want_growth_tracking_services', IntegerType(), True),
        StructField('want_counselling_services', IntegerType(), True)
    ])

    _aggregator = AggLocationHelper
    _warehouse_base_table = CHILD_CARE_MONTHLY_TABLE
    _partition_columns = ('month',)


class ServiceEnrollment(BaseTable):
    schema = StructType(fields=[
        StructField('domain', StringType(), True),
        StructField('month', DateType(), True),
        StructField('flwc_id', StringType(), True),
        StructField('supervisor_id', StringType(), True),
        StructField('project_id', StringType(), True),
        StructField('district_id', StringType(), True),
        StructField('state_id', StringType(), True),
        StructField('member_case_id', StringType(), True),
        StructField('want_nutrition_services', IntegerType(), True),
        StructField('want_growth_tracking_services', IntegerType(), True),
        StructField('want_counselling_services', IntegerType(), True)
    ])

    _aggregator = AggLocationHelper
    _warehouse_base_table = SERVICE_ENROLLMENT_TABLE
    _partition_columns = ('month',)
