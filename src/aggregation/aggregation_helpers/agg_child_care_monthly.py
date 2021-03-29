from src.aggregation.aggregation_helpers.base_helper import BaseAggregationHelper
from spark_session_handler import SPARK
from src.utils import clean_name
from datalake_conts import SERVICE_ENROLLMENT_TABLE


class AggChildCareMonthly(BaseAggregationHelper):

    @property
    def source_tablename(self):
        return "child_care_case"

    @property
    def member_case_table(self):
        return "member_case"

    @property
    def location_table(self):
        return "flwc_location"

    @property
    def service_enrollment_table(self):
        return SERVICE_ENROLLMENT_TABLE

    def aggregate(self):
        df = self.preprocess()
        return self.enforce_schema(df)

    def preprocess(self):
        SPARK.sql(f"""
        SELECT
            case_id,
            opened_on,
            case_references.referenced_id as member_case_id,
            case_references.referenced_type as reference_type
        FROM {clean_name(self.domain)}.{self.source_tablename}
        LATERAL VIEW explode(indices) as case_references
        WHERE (
            case_references.referenced_type='member' AND
            case_references.identifier = 'parent' AND
            opened_on < add_months({self.month}, 1) AND
            (closed_on is null or closed_on >= {self.month})
        )
        """)

    def aggregate_data(self, df):

        age_in_months = f"({self.month}-pass1.dob)/30.4"
        want_nutrition_services = f"service_enrollment.want_nutrition_services is distinct from 'no'"
        want_growth_tracking_services = f"service_enrollment.want_growth_tracking_services is distinct from 'no'"
        want_counselling_services = f"service_enrollment.want_counselling_services is distinct from 'no'"

        df.createOrReplaceTempView("pass1")
        SPARK.sql(f"""
        SELECT
            location.state_id,
            location.district_id,
            location.project_id,
            location.supervisor_id,
            pass1.flwc_id,
            pass1.case_id,
            pass1.opened_on,
            member_case.case_id as member_case_id,
            member_case.name as name,
            member_case.case_json__member_dob as dob,
            member_case.case_json__member_gender as gender,
            member_case.case_json__mother_case_id as mother_member_case_id
            {age_in_months} as age_in_months,
            null as date_death,
            int({want_nutrition_services}) as want_nutrtion_services,
            int({want_growth_tracking_services}) as want_growth_tracking_services,
            int({want_counselling_services}) as want_counselling_services
        from pass1
        left join {self.member_case_table} member_case ON (
            pass1.member_case_id = member_case.case_id
        )
        left join {self.location_table} location ON (
            pass1.flwc_id = location.flwc_id AND
            location.location_level = 5
        )
        left join {self.service_enrollment_table} as service_enrollment ON (
            pass1.member_case_id = service_enrollment.member_case_id AND
            service_enrollment.month = '{self.month}'
        )

        """)
