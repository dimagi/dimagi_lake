from src.aggregation.aggregation_helpers.base_helper import BaseAggregationHelper
from spark_session_handler import SPARK
from src.utils import clean_name


class AggSupplementaryNutritionForm(BaseAggregationHelper):

    @property
    def source_tablename(self):
        return f"{clean_name(self.database_name)}.child_3_6_form"

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
            "children_food_served": (
                'form',
                'children_food_served'
            ),
            "timeend": (
                'form',
                'meta',
                'timeEnd'
            ),

        }

    def aggregate(self):
        df = self.preprocess()
        df.show(10,False)
        df = self.aggregate_data(df)
        df.show(10, False)
        return self.enforce_schema(df)

    def preprocess(self):
        df1 = SPARK.sql(f"""
        SELECT  
            children.`@id` as child_case_id,
            date_trunc('DAY', {self.get_column('timeend')}) as timeend,
            CASE 
                WHEN array_contains(split({self.get_column('present_children')}, ' '), children.`@id`) THEN 1 
                ELSE 0 
            END as pse_attended,
            CASE 
                WHEN array_contains(split({self.get_column('children_food_served')}, ' '), children.`@id`) THEN 1
                ELSE 0
            END as snd_given
        from {self.source_tablename}
        LATERAL VIEW explode({self.get_column('children_cases')}) as children
        WHERE month='{self.month}'
        """)
        return df1

    def aggregate_data(self, df):
        df.createOrReplaceTempView('pass1')

        return SPARK.sql(f"""
        SELECT
            child_case_id,
            '{self.domain}' as domain,
            '{self.month}' as month,
            max(timeend) as last_timeend_processed,
            sum(pse_attended) as total_pse_attended,
            sum(snd_given) as total_snd_given
        FROM pass1
        GROUP BY child_case_id
        """)


