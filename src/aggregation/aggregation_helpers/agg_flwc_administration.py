from datetime import datetime
from src.aggregation.aggregation_helpers.base_helper import BaseAggregationHelper
from spark_session_handler import SPARK
from src.utils import clean_name

class AggFlwcAdministartionHelper(BaseAggregationHelper):


    @property
    def source_tablename(self):
        return "flwc_management_flwc_administration"

    def aggregate(self):
        df = self.pre_process()
        df.show()
        df = self.apply_transformation(df)

        return self.enforce_schema(df)

    @property
    def columns(self):
        return {
            "infrastructure_available": (
                'form',
                'flwc_information_and_infrastructure_group',
                'infrastructure_at_flwc_details_group',
                'infrastructure_available'
            ),
            "gme_available": (
                "form",
                "essentials_at_flwc_details_group",
                "growth_measuring_equipments_details_group",
                "gme_available"
            ),
            "utility_items_list": (
                "form",
                "essentials_at_flwc_details_group",
                "other_utility_items_details_group",
                "utility_items_list"
            ),
            "timeend": (
                "form",
                "meta",
                "timeEnd"
            )
        }

    def pre_process(self):
        df = SPARK.sql(f"""
        SELECT
            flwc_id,
            supervisor_id,
            project_id,
            district_id,
            state_id,
            {self.get_column('timeend')} as timeend,
            int(array_contains(split({self.get_column('infrastructure_available')}, ' '), 'safe_drinking_water')) as safe_drinking_water,
            int(array_contains(split({self.get_column('infrastructure_available')}, ' '), 'toilet')) as functional_toilet,
            int(array_contains(split({self.get_column('gme_available')}, ' '), 'weighing_scale_infants'))  as weighing_scale_infants,
            int(array_contains(split({self.get_column('gme_available')}, ' '), 'weighing_scale_mother_and_child'))  as weighing_scale_mother_and_child,
            int(array_contains(split({self.get_column('gme_available')}, ' '), 'infantometer'))  as infantometer,
            int(array_contains(split({self.get_column('gme_available')}, ' '), 'stadiometer'))  as stadiometer,
            int(array_contains(split({self.get_column('utility_items_list')}, ' '), 'medicine_kit'))  as medicine_kit
        FROM {self.database_name}.{self.source_tablename} as source_table
        WHERE add_months(month, 6) > '{self.month}' AND month < add_months('{self.month}', 1)
        """)
        return df

    def apply_transformation(self, df):

        df.createOrReplaceTempView("pass1")

        return SPARK.sql(f"""
        SELECT
            distinct
            '{self.domain}' as domain,
            to_date('{self.month}','yyyy-MM-dd') as month,
            flwc_id,
            supervisor_id,
            project_id,
            district_id,
            state_id,
            LAST_VALUE(safe_drinking_water) OVER safe_drinking_water as clean_drinking_water,
            LAST_VALUE(functional_toilet) OVER functional_toilet as functional_toilet,
            LAST_VALUE(weighing_scale_infants) OVER weighing_scale_infants as weighing_scale_infants,
            LAST_VALUE(weighing_scale_mother_and_child) OVER weighing_scale_mother_and_child as weighing_scale_mother_and_child,
            LAST_VALUE(infantometer) OVER infantometer as infantometer_available,
            LAST_VALUE(stadiometer) OVER stadiometer as stadiometer_available,
            LAST_VALUE(medicine_kit) OVER medicine_kit as medicine_kit_available
        FROM pass1
        WINDOW
            safe_drinking_water AS (
                PARTITION BY flwc_id
                ORDER BY
                    CASE WHEN safe_drinking_water is distinct from 1 THEN 0 ELSE 1 END ASC,
                    timeend RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ),
            functional_toilet AS (
                PARTITION BY flwc_id
                ORDER BY
                    CASE WHEN functional_toilet is distinct from 1 THEN 0 ELSE 1 END ASC,
                    timeend RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ),
            weighing_scale_infants AS (
                PARTITION BY flwc_id
                ORDER BY
                    CASE WHEN weighing_scale_infants is distinct from 1 THEN 0 ELSE 1 END ASC,
                    timeend RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ),
            weighing_scale_mother_and_child AS (
                PARTITION BY flwc_id
                ORDER BY
                    CASE WHEN weighing_scale_mother_and_child is distinct from 1 THEN 0 ELSE 1 END ASC,
                    timeend RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ),
            infantometer AS (
                PARTITION BY flwc_id
                ORDER BY
                    CASE WHEN infantometer is distinct from 1 THEN 0 ELSE 1 END ASC,
                    timeend RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ),
            stadiometer AS (
                PARTITION BY flwc_id
                ORDER BY
                    CASE WHEN stadiometer is distinct from 1 THEN 0 ELSE 1 END ASC,
                    timeend RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ),
            medicine_kit AS (
                PARTITION BY flwc_id
                ORDER BY
                    CASE WHEN medicine_kit is distinct from 1 THEN 0 ELSE 1 END ASC,
                    timeend RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            )
        """)

