from src.aggregation.aggregation_helpers.base_helper import BaseAggregationHelper
from spark_session_handler import SPARK
from src.utils import clean_name


class AggGrowthMonitoringForm(BaseAggregationHelper):

    @property
    def source_tablename(self):
        return f"{clean_name(self.database_name)}.raw_growth_monitoring_form"

    @property
    def columns(self):
        return {
            'children_cases': (
                'form',
                'children_cases',
                'item'
            ),
            'present_children': (
                'form',
                'children_attendance',
                'present_children'
            ),
            "timeend": (
                'form',
                'meta',
                'timeEnd'
            ),

        }

    def aggregate(self):
        df = self.aggregate_data()
        df.show(10, False)
        return self.enforce_schema(df)

    def aggregate_data(self):
        df1 = SPARK.sql(f"""
        SELECT
            '{self.domain}' as domain,
            '{self.month}' as month,
            `form__case_child_care_case_0__case__@case_id` as child_case_id,
            LAST_VALUE(form__weight) over weight as weight,
            LAST_VALUE(form__height) over height as height,
            LAST_VALUE(form__height_provided__zscore_grading_hfa) over zscore_grading_hfa as zscore_grading_hfa,
            LAST_VALUE(form__height_provided__zscore_grading_wfh) over zscore_grading_wfh as zscore_grading_wfh,
            LAST_VALUE(form__weight_provided__zscore_grading_wfa) over zscore_grading_wfa as zscore_grading_wfa,
            CASE 
                WHEN LAST_VALUE(form__weight) over weight IS NULL THEN NULL 
                ELSE LAST_VALUE({self.get_column('timeend')}) over weight 
            END as last_weight_recorded_date,
  
            CASE 
                WHEN LAST_VALUE(form__height) over height IS NULL THEN NULL 
                ELSE LAST_VALUE({self.get_column('timeend')}) over height 
            END as last_height_recorded_date,
            
            CASE 
                WHEN LAST_VALUE(form__height_provided__zscore_grading_hfa) over zscore_grading_hfa IS NULL THEN NULL 
                ELSE LAST_VALUE({self.get_column('timeend')}) over zscore_grading_hfa 
            END as last_zscore_grading_hfa_recorded_date,
            
            CASE 
                WHEN LAST_VALUE(form__height_provided__zscore_grading_wfh) over zscore_grading_wfh IS NULL THEN NULL 
                ELSE LAST_VALUE({self.get_column('timeend')}) over zscore_grading_wfh 
            END as last_zscore_grading_wfh_recorded_date,
            
            CASE 
                WHEN LAST_VALUE(form__weight_provided__zscore_grading_wfa) over zscore_grading_wfa IS NULL THEN NULL 
                ELSE LAST_VALUE({self.get_column('timeend')}) over zscore_grading_wfa 
            END as last_zscore_grading_wfa_recorded_date

        from {self.source_tablename}
        WHERE month='{self.month}'
        WINDOW
            weight AS (
                PARTITION BY `form__case_child_care_case_0__case__@case_id`
                ORDER BY
                    CASE WHEN form__weight IS NULL THEN 0 ELSE 1 END ASC,
                    {self.get_column('timeend')} RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ),
            height AS (
                PARTITION BY `form__case_child_care_case_0__case__@case_id`
                ORDER BY
                    CASE WHEN form__height IS NULL THEN 0 ELSE 1 END ASC,
                    {self.get_column('timeend')} RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ),
            zscore_grading_hfa AS (
                PARTITION BY `form__case_child_care_case_0__case__@case_id`
                ORDER BY
                    CASE WHEN form__height_provided__zscore_grading_hfa IS NULL THEN 0 ELSE 1 END ASC,
                    {self.get_column('timeend')} RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ),
            zscore_grading_wfh AS (
                PARTITION BY `form__case_child_care_case_0__case__@case_id`
                ORDER BY
                    CASE WHEN form__height_provided__zscore_grading_wfh IS NULL THEN 0 ELSE 1 END ASC,
                    {self.get_column('timeend')} RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ),
            zscore_grading_wfa AS (
                PARTITION BY `form__case_child_care_case_0__case__@case_id`
                ORDER BY
                    CASE WHEN form__weight_provided__zscore_grading_wfa IS NULL THEN 0 ELSE 1 END ASC,
                    {self.get_column('timeend')} RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            )

        """)
        return df1


