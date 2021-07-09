from abc import ABC

from pyspark.sql.types import (DateType, DoubleType, IntegerType, StringType,
                               StructField, StructType)

import env_settings
from consts import (CHILD_CARE_MONTHLY_TABLE, FLWC_LOCATION_TABLE,
                    SERVICE_ENROLLMENT_TABLE, CHILD_WEIGHT_HEIGHT_FORM_TABLE, CHILD_THR_FORM_TABLE, SUPPLEMENTARY_NUTRITION_FORM_TABLE)
from dimagi_lake.aggregation.aggregation_helpers.agg_location import \
    AggLocationHelper
from dimagi_lake.aggregation.aggregation_helpers.child_care_monthly import \
    ChildCareMonthlyAggregationHelper
from dimagi_lake.aggregation.aggregation_helpers.child_weight_height_form import \
    ChildWeightHeightAggregationHelper
from dimagi_lake.aggregation.aggregation_helpers.service_enrollment_form import \
    ServiceEnrollmentAggregationHelper
from dimagi_lake.aggregation.aggregation_helpers.child_snd_form import \
    SupplementaryNutritionAggregationHelper
from dimagi_lake.aggregation.aggregation_helpers.child_thr_form import \
    ChildTHRAggregationHelper

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

    def aggregate(self):
        aggregator = self._aggregator(self._database_name,
                                      self._domain,
                                      self._month,
                                      self.schema)
        agg_df = aggregator.aggregate()
        self.write_monthly_data(agg_df)

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

    def write_monthly_data(self, df):
        (df.write.partitionBy(*self._partition_columns)
         .option('overwriteSchema', True)
         .option("replaceWhere", f"month = '{self._month}'")
         .mode("overwrite")
         .saveAsTable(self.datalake_tablename,
                      format='delta',
                      mode='overwrite',
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
        StructField('death_date', DateType(), True),
        StructField('age_in_months', DoubleType(), True),
        StructField('age_in_months_end', DoubleType(), True),
        StructField('want_nutrition_services', IntegerType(), True),
        StructField('want_growth_tracking_services', IntegerType(), True),
        StructField('want_counselling_services', IntegerType(), True),
        StructField('alive_in_month', IntegerType(), True),
        StructField('born_in_month', IntegerType(), True),
        StructField('birth_weight', DoubleType(), True),
        StructField('low_birth_weight', IntegerType(), True),
        StructField('immediate_bf', IntegerType(), True),
        StructField('weight', DoubleType(), True),
        StructField('height', DoubleType(), True),
        StructField('zscore_grading_hfa', IntegerType(), True),
        StructField('zscore_grading_wfh', IntegerType(), True),
        StructField('zscore_grading_wfa', IntegerType(), True)

    ])

    _aggregator = ChildCareMonthlyAggregationHelper
    _warehouse_base_table = CHILD_CARE_MONTHLY_TABLE
    _partition_columns = ('month',)


class ServiceEnrollment(BaseTable):
    schema = StructType(fields=[
        StructField('domain', StringType(), True),
        StructField('month', DateType(), True),
        StructField('member_case_id', StringType(), True),
        StructField('registered_on', DateType(), True),
        StructField('want_nutrition_services', IntegerType(), True),
        StructField('want_growth_tracking_services', IntegerType(), True),
        StructField('want_counselling_services', IntegerType(), True)
    ])

    _aggregator = ServiceEnrollmentAggregationHelper
    _warehouse_base_table = SERVICE_ENROLLMENT_TABLE
    _partition_columns = ('month',)


class ChildWeightHeightForm(BaseTable):
    schema = StructType(fields=[
        StructField('domain', StringType(), True),
        StructField('month', DateType(), True),
        StructField('child_case_id', StringType(), True),
        StructField('weight', DoubleType(), True),
        StructField('last_weight_recorded_date', DateType(), True),
        StructField('height', DoubleType(), True),
        StructField('last_height_recorded_date', DateType(), True),
        StructField('zscore_grading_hfa', StringType(), True),
        StructField('last_zscore_grading_hfa_recorded_date', DateType(), True),
        StructField('zscore_grading_wfh', StringType(), True),
        StructField('last_zscore_grading_wfh_recorded_date', DateType(), True),
        StructField('zscore_grading_wfa', StringType(), True),
        StructField('last_zscore_grading_wfa_recorded_date', DateType(), True),
    ])

    _aggregator = ChildWeightHeightAggregationHelper
    _warehouse_base_table = CHILD_WEIGHT_HEIGHT_FORM_TABLE
    _partition_columns = ('month',)


class SupplementaryNutritionForm(BaseTable):
    schema = StructType(fields=[
        StructField('domain', StringType(), True),
        StructField('month', DateType(), True),
        StructField('last_timeend_processed', DateType(), True),
        StructField('child_case_id', StringType(), True),
        StructField('total_pse_attended', IntegerType(), True),
        StructField('total_snd_given', IntegerType(), True),
    ])

    _aggregator = SupplementaryNutritionAggregationHelper
    _warehouse_base_table = SUPPLEMENTARY_NUTRITION_FORM_TABLE
    _partition_columns = ('month',)


class ChildTHRForm(BaseTable):
    schema = StructType(fields=[
        StructField('domain', StringType(), True),
        StructField('month', DateType(), True),
        StructField('child_case_id', StringType(), True),
        StructField('days_thr_given', IntegerType(), True)
    ])

    _aggregator = ChildTHRAggregationHelper
    _warehouse_base_table = CHILD_THR_FORM_TABLE
    _partition_columns = ('month',)
