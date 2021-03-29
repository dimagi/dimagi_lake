from src.aggregation.aggregation_helpers.base_helper import BaseAggregationHelper
from spark_session_handler import SPARK
from src.utils import clean_name
from datalake_conts import SERVICE_ENROLLMENT_TABLE


class AggChildCareMonthly(BaseAggregationHelper):

    @property
    def source_tablename(self):
        return "service_enrollment_form"

    @property
    def member_case_table(self):
        return "member_case"

    @property
    def location_table(self):
        return "flwc_location"

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
            )
        }
    def aggregate(self):
        df = self.preprocess()
        df = self.aggregate_data(df)
        return self.enforce_schema(df)

    def preprocess(self):
        return SPARK.sql(f"""
        SELECT
            service_enrollment.flwc_id as flwc_id,
            service_enrollment.supervisor_id as supervisor_id,
            service_enrollment.project_id as project_id,
            service_enrollment.district_id as district_id,
            service_enrollment.state_id as state_id,
            {self.get_column('member_caseid_loaded')} as member_case_id,
            LAST_VALUE({self.get_column('want_counselling_services_check')}) as want_counselling_services,
            LAST_VALUE({self.get_column('want_growth_tracking_services_check')}) as want_growth_tracking_services,
            LAST_VALUE({self.get_column('want_nutrition_services_check')}) as want_nutrition_services
        FROM {clean_name(self.domain)}.{self.source_tablename} as service_enrollment
        WHERE month = '{self.month}'
        WINDOW
        case_service AS (
                PARTITION BY {self.get_column('member_caseid_loaded')}
                ORDER BY
                    timeend RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            )
        """)

    def aggregate_data(self, df):
        df.createOrReplaceTempView("pass1")

        if SERVICE_ENROLLMENT_TABLE not in SPARK.sql(f"show tables in {self.database_name}").show():
            return df

        return SPARK.sql(f"""
        SELECT
            '{self.domain}' as domain,
            to_date('{self.month}','yyyy-MM-dd') as month,
            COALESCE(pass1.flwc_id, previous_month.flwc_id) as flwc_id,
            COALESCE(pass1.supervisor_id, previous_month.supervisor_id) as supervisor_id,
            COALESCE(pass1.project_id, previous_month.project_id) as project_id,
            COALESCE(pass1.district_id, previous_month.district_id) as district_id,
            COALESCE(pass1.state_id, previous_month.state_id) as state_id,
            COALESCE(pass1.want_counselling_services, previous_month.want_counselling_services) as want_counselling_services,
            COALESCE(pass1.want_growth_tracking_services, previous_month.want_growth_tracking_services) as want_growth_tracking_services,
            COALESCE(pass1.want_nutrition_services, previous_month.want_nutrition_services) as want_nutrition_services
          
        from pass1
        FULL OUTER JOIN {SERVICE_ENROLLMENT_TABLE} as previous_month ON (
            pass1.member_case_id = previous_month.member_case_id
        )
        WHERE coalesce(pass1.month, '{self.month}') = '{self.month}' AND
              add_months(coalesce(prev_month.month, %(previous_month)s), 1)  = '{self.month}'
        
        
        """)
