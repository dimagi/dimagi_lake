from dimagi_lake.aggregation.aggregation_helpers.base_helper import BaseAggregationHelper
from spark_session_handler import SPARK


class ChildWeightHeightAggregationHelper(BaseAggregationHelper):

    @property
    def source_tablename(self):
        return f"{self.database_name}.raw_child_weight_height_form"

    @property
    def columns(self):
        return {
            'case_id': (
                'form',
                'case_child_care_case_0',
                'case',
                '@case_id'
            ),
            'weight': (
                'form',
                'weight'
            ),
            'height': (
                'form',
                'height'
            ),
            'hfa': (
                'form',
                'height_provided',
                'zscore_grading_hfa'
            ),
            'wfh': (
                'form',
                'height_provided',
                'zscore_grading_wfh'
            ),
            'wfa': (
                'form',
                'weight_provided',
                'zscore_grading_wfa'
            ),
            "timeend": (
                'form',
                'meta',
                'timeEnd'
            )
        }

    def aggregate(self):
        df = self.aggregate_data()
        return self.enforce_schema(df)

    def aggregate_data(self):
        return SPARK.sql(f"""
        SELECT
            '{self.domain}' as domain,
            '{self.month}' as month,
            `{self.get_column('case_id')}` as child_case_id,
            LAST_VALUE({self.get_column('weight')}) over weight as weight,
            LAST_VALUE({self.get_column('height')}) over height as height,
            LAST_VALUE({self.get_column('hfa')}) over zscore_grading_hfa as zscore_grading_hfa,
            LAST_VALUE({self.get_column('wfh')}) over zscore_grading_wfh as zscore_grading_wfh,
            LAST_VALUE({self.get_column('wfa')}) over zscore_grading_wfa as zscore_grading_wfa,
            CASE 
                WHEN LAST_VALUE({self.get_column('weight')}) over weight IS NULL THEN NULL 
                ELSE LAST_VALUE({self.get_column('timeend')}) over weight 
            END as last_weight_recorded_date,
  
            CASE 
                WHEN LAST_VALUE({self.get_column('height')}) over height IS NULL THEN NULL 
                ELSE LAST_VALUE({self.get_column('timeend')}) over height 
            END as last_height_recorded_date,
            
            CASE 
                WHEN LAST_VALUE({self.get_column('hfa')}) over zscore_grading_hfa IS NULL THEN NULL 
                ELSE LAST_VALUE({self.get_column('timeend')}) over zscore_grading_hfa 
            END as last_zscore_grading_hfa_recorded_date,
            
            CASE 
                WHEN LAST_VALUE({self.get_column('wfh')}) over zscore_grading_wfh IS NULL THEN NULL 
                ELSE LAST_VALUE({self.get_column('timeend')}) over zscore_grading_wfh 
            END as last_zscore_grading_wfh_recorded_date,
            
            CASE 
                WHEN LAST_VALUE({self.get_column('wfa')}) over zscore_grading_wfa IS NULL THEN NULL 
                ELSE LAST_VALUE({self.get_column('timeend')}) over zscore_grading_wfa 
            END as last_zscore_grading_wfa_recorded_date
        from {self.source_tablename}
        WHERE month='{self.month}'
        WINDOW
            weight AS (
                PARTITION BY `{self.get_column('case_id')}`
                ORDER BY
                    CASE WHEN {self.get_column('weight')} IS NULL THEN 0 ELSE 1 END ASC,
                    {self.get_column('timeend')} RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ),
            height AS (
                PARTITION BY `{self.get_column('case_id')}`
                ORDER BY
                    CASE WHEN {self.get_column('height')} IS NULL THEN 0 ELSE 1 END ASC,
                    {self.get_column('timeend')} RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ),
            zscore_grading_hfa AS (
                PARTITION BY `{self.get_column('case_id')}`
                ORDER BY
                    CASE WHEN {self.get_column('hfa')} IS NULL THEN 0 ELSE 1 END ASC,
                    {self.get_column('timeend')} RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ),
            zscore_grading_wfh AS (
                PARTITION BY `{self.get_column('case_id')}`
                ORDER BY
                    CASE WHEN {self.get_column('wfh')} IS NULL THEN 0 ELSE 1 END ASC,
                    {self.get_column('timeend')} RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ),
            zscore_grading_wfa AS (
                PARTITION BY `{self.get_column('case_id')}`
                ORDER BY
                    CASE WHEN {self.get_column('wfa')} IS NULL THEN 0 ELSE 1 END ASC,
                    {self.get_column('timeend')} RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            )
        """)
