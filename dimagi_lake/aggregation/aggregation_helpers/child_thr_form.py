from dimagi_lake.aggregation.aggregation_helpers.base_helper import BaseAggregationHelper
from spark_session_handler import SPARK


class ChildTHRAggregationHelper(BaseAggregationHelper):

    @property
    def source_tablename(self):
        return f"{self.database_name}.raw_child_thr_form"

    @property
    def columns(self):
        return {
            'children_cases': (
                'form',
                'children_0_3_cases',
                'item'
            ),
            'children_received_thr_id': (
                'form',
                'thr_details_group',
                'children_received_thr_id'
            ),
            'number_of_days_thr_given': (
                'form',
                'thr_details_group',
                'number_of_days_thr_given'
            ),
            "timeend": (
                'form',
                'meta',
                'timeEnd'
            ),

        }

    def aggregate(self):
        df = self.preprocess()
        df = self.aggregate_data(df)
        return self.enforce_schema(df)

    def preprocess(self):
        return SPARK.sql(f"""
        SELECT  
            children.`@id` as child_case_id,
            date_trunc('DAY', {self.get_column('timeend')}) as timeend,
            CASE
                WHEN array_contains(split({self.get_column('children_received_thr_id')}, ' '), children.`@id`) THEN {self.get_column('number_of_days_thr_given')}
                ELSE 0
            END as days_thr_given
        from {self.source_tablename}
        LATERAL VIEW explode({self.get_column('children_cases')}) as children
        WHERE month='{self.month}'
        """)

    def aggregate_data(self, df):
        df.createOrReplaceTempView('pass1')

        return SPARK.sql(f"""
        SELECT
            child_case_id,
            '{self.domain}' as domain,
            '{self.month}' as month,
            max(timeend) as last_timeend_processed,
            sum(days_thr_given) as days_thr_given
        FROM pass1
        GROUP BY child_case_id
        """)