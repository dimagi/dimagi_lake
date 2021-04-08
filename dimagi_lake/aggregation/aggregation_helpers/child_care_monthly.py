from consts import FLWC_LOCATION_TABLE, SERVICE_ENROLLMENT_TABLE, CHILD_WEIGHT_HEIGHT_FORM_TABLE
from dimagi_lake.aggregation.aggregation_helpers.base_helper import \
    BaseAggregationHelper
from spark_session_handler import SPARK


class ChildCareMonthlyAggregationHelper(BaseAggregationHelper):

    @property
    def source_tablename(self):
        return f"{self.database_name}.raw_child_care_case"

    @property
    def member_case_table(self):
        return f"{self.database_name}.raw_member_case"

    @property
    def location_table(self):
        return f"{self.database_name}.{FLWC_LOCATION_TABLE}"

    @property
    def service_enrollment_table(self):
        return f"{self.database_name}.{SERVICE_ENROLLMENT_TABLE}"

    @property
    def gm_table(self):
        return f"{self.database_name}.{CHILD_WEIGHT_HEIGHT_FORM_TABLE}"

    def aggregate(self):
        df = self.preprocess()
        df.show(10,False)
        df = self.aggregate_data(df)
        df.show()
        return self.enforce_schema(df)

    def preprocess(self):
        return SPARK.sql(f"""
        SELECT
            case_id,
            opened_on,
            owner_id as flwc_id,
            case_json__low_birth_weight as low_birth_weight,
            case_json__breastfeeding_initiated as breastfeeding_initiated,
            case_json__birth_weight as birth_weight,
            case_references.referenced_id as member_case_id,
            case_references.referenced_type as reference_type
        FROM {self.source_tablename}
        LATERAL VIEW explode(indices) as case_references
        WHERE (
            case_references.referenced_type='member' AND
            case_references.identifier = 'parent' AND
            month <='{self.month}' AND
            (closed_on is null or closed_on >= '{self.month}')
        )
        """)

    def aggregate_data(self, df):

        age_in_months = f"datediff('{self.month}', member_case.case_json__member_dob)/30.4"
        age_in_months_end = f"datediff(add_months('{self.month}', 1), member_case.case_json__member_dob)/30.4"

        want_nutrition_services = (f"(service_enrollment.want_nutrition_services is distinct from 0 "
                                   f" OR service_enrollment.registered_on>='{self.month}')")
        want_growth_tracking_services = (f"(service_enrollment.want_growth_tracking_services is distinct from 0 "
                                         f" OR service_enrollment.registered_on>='{self.month}')")
        want_counselling_services = (f"(service_enrollment.want_counselling_services is distinct from 0 "
                                     f"OR service_enrollment.registered_on>='{self.month}')")
        born_in_month = (f"date_trunc('MONTH',case_json__member_dob)='{self.month}'")
        gm_eligible = f"({age_in_months}<=60) AND {want_growth_tracking_services}"


        df.createOrReplaceTempView("child_cases")

        return SPARK.sql(f"""
        SELECT
            '{self.domain}' as domain,
            to_date('{self.month}','yyyy-MM-dd') as month,
            location.state_id,
            location.district_id,
            location.project_id,
            location.supervisor_id,
            child_cases.flwc_id,
            child_cases.case_id,
            child_cases.opened_on,
            member_case.case_id as member_case_id,
            member_case.name as name,
            member_case.case_json__member_dob as dob,
            member_case.case_json__member_gender as gender,
            member_case.case_json__mother_case_id as mother_member_case_id,
            child_cases.birth_weight as birth_weight,
            member_case.case_json__member_death_date as death_date,
            {age_in_months} as age_in_months,
            {age_in_months_end} as age_in_months_end,
            {want_nutrition_services} as want_nutrition_services,
            {want_growth_tracking_services} as want_growth_tracking_services,
            {want_counselling_services} as want_counselling_services,
            CASE
                WHEN member_case.case_json__member_death_date is NULL THEN 1
                ELSE 0
            END as alive_in_month,
            CASE
                WHEN {born_in_month} THEN 1
                ELSE 0
            END as born_in_month,
            CASE
                WHEN low_birth_weight='yes' THEN 1
                ELSE 0
            END as low_birth_weight,
            CASE
                WHEN breastfeeding_initiated='yes' THEN 1
                ELSE 0
            END as immediate_bf,
            CASE
                WHEN {gm_eligible} AND date_trunc('MONTH', gm.last_weight_recorded_date) = '{self.month}' THEN gm.weight 
                ELSE NULL 
            END as weight,
            CASE
                WHEN {gm_eligible} AND date_trunc('MONTH', gm.last_height_recorded_date) = '{self.month}' THEN gm.height 
                ELSE NULL 
            END as height,
            CASE
                WHEN {gm_eligible} AND date_trunc('MONTH', gm.last_zscore_grading_hfa_recorded_date) = '{self.month}' THEN gm.zscore_grading_hfa 
                ELSE NULL 
            END as zscore_grading_hfa,
            CASE
                WHEN {gm_eligible} AND date_trunc('MONTH', gm.last_zscore_grading_wfh_recorded_date) = '{self.month}' THEN gm.zscore_grading_wfh 
                ELSE NULL 
            END as zscore_grading_wfh,
            CASE
                WHEN {gm_eligible} AND date_trunc('MONTH', gm.last_zscore_grading_wfa_recorded_date) = '{self.month}' THEN gm.zscore_grading_wfa 
                ELSE NULL 
            END as zscore_grading_wfa
        from child_cases
        left join {self.member_case_table} member_case ON (
            child_cases.member_case_id = member_case.case_id
        )
        left join {self.location_table} location ON (
            child_cases.flwc_id = location.flwc_id AND
            location.location_level = 5
        )
        left join {self.service_enrollment_table} as service_enrollment ON (
            child_cases.member_case_id = service_enrollment.member_case_id AND
            service_enrollment.month = '{self.month}'
        )
        left join {self.gm_table} as gm ON (
            child_cases.case_id = gm.child_case_id AND
            gm.month = '{self.month}'
        )
        """)