from src.aggregation.aggregation_helpers.base_helper import BaseAggregationHelper
from spark_session_handler import SPARK
from src.utils import clean_name
from datalake_conts import SERVICE_ENROLLMENT_TABLE
import datetime

class AggServiceEnrollment(BaseAggregationHelper):

    @property
    def source_tablename(self):
        return "service_enrollment"

    @property
    def columns(self):
        return {
            'want_counselling_services_check': (
                'form',
                'want_counselling_services_check'
            ),
            'want_growth_tracking_services_check': (
                'form',
                'want_growth_tracking_services_check'
            ),
            'want_nutrition_services_check': (
                'form',
                'want_nutrition_services_check'
            ),
            "member_caseid_loaded": (
                'form',
                'member_caseid_loaded'
            ),
            "timeend": (
                'form',
                'meta',
                'timeEnd'
            ),

        }

    def aggregate(self):
        df = self.preprocess()
        df.show(2,False)
        df = self.aggregate_data(df)
        df.show(2, False)
        return self.enforce_schema(df)

    def preprocess(self):
        return SPARK.sql(f"""
        SELECT  
            distinct 
            '{self.domain}' as domain,
            to_date('{self.month}','yyyy-MM-dd') as month,
            {self.get_column('member_caseid_loaded')} as member_case_id,
            LAST_VALUE({self.get_column('timeend')}) OVER case_service as registered_on,
            CASE WHEN LAST_VALUE({self.get_column('want_counselling_services_check')}) OVER case_service 
                 IS DISTINCT FROM 'no' THEN 1 ELSE 0 END 
                as want_counselling_services,
            CASE WHEN LAST_VALUE({self.get_column('want_growth_tracking_services_check')}) OVER case_service
                 IS DISTINCT FROM 'no' THEN 1 ELSE 0  END 
                as want_growth_tracking_services,
            CASE WHEN LAST_VALUE({self.get_column('want_nutrition_services_check')}) OVER case_service
                 IS DISTINCT FROM 'no' THEN 1 ELSE 0 END
                as want_nutrition_services
        FROM {clean_name(self.database_name)}.{self.source_tablename} service_enrollment
        where month = '{self.month}'
        WINDOW
        case_service AS (
                PARTITION BY  {self.get_column('member_caseid_loaded')}
                ORDER BY
                    {self.get_column('timeend')} RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            )
        """)

    def aggregate_data(self, df):
        df.createOrReplaceTempView("pass1")
        lastMonth = datetime.datetime.strptime(self.month,'%Y-%m-%d').date() - datetime.timedelta(days=1)
        previous_month = lastMonth.replace(day=1).strftime('%Y-%m-%d')
        if SERVICE_ENROLLMENT_TABLE not in [table.name for table in SPARK.catalog.listTables(self.database_name)]:
            print("Not table exists")
            return df

        return SPARK.sql(f"""
        SELECT
            '{self.domain}' as domain,
            to_date('{self.month}','yyyy-MM-dd') as month,
            COALESCE(pass1.registered_on, previous_month.registered_on) as registered_on,
            COALESCE(pass1.member_case_id, previous_month.member_case_id) as member_case_id,
            COALESCE(pass1.want_counselling_services, previous_month.want_counselling_services) as want_counselling_services,
            COALESCE(pass1.want_growth_tracking_services, previous_month.want_growth_tracking_services) as want_growth_tracking_services,
            COALESCE(pass1.want_nutrition_services, previous_month.want_nutrition_services) as want_nutrition_services
        from pass1
        FULL OUTER JOIN (
            SELECT * from {clean_name(self.database_name)}.{SERVICE_ENROLLMENT_TABLE} WHERE month='{previous_month}'
        ) as previous_month ON (
            pass1.member_case_id = previous_month.member_case_id
        )
        WHERE coalesce(pass1.month, '{self.month}') = '{self.month}' AND
              add_months(coalesce(previous_month.month,'{previous_month}'), 1)  = '{self.month}'
        """)
