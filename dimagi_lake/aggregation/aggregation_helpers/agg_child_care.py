from dimagi_lake.aggregation.aggregation_helpers.base_helper import \
    BaseAggregationHelper
from dimagi_lake.aggregation.utils import (get_location_column_rollup_calc,
                                           get_location_group_by_columns)
from spark_session_handler import SPARK
from consts import CHILD_CARE_MONTHLY_TABLE, FLWC_LOCATION_TABLE


class AggLocationHelper(BaseAggregationHelper):

    @property
    def child_care_monthly_table(self):
        return f"{self.database_name}.{CHILD_CARE_MONTHLY_TABLE}"

    @property
    def location_table(self):
        return f"{self.database_name}.{FLWC_LOCATION_TABLE}"

    def aggregate(self):
        df = self.agg_base_data()
        for i in range(4, 0, -1):
            df = df.unionByName(self.rollup_data(df, i))
        return self.enforce_schema(df)

    def agg_base_data(self):
        
        return SPARK.sql(f"""
        SELECT
            '{self.domain}' as domain,
            '{self.month}' as month,
            5 as location_level,
            location.state_id,
            location.district_id,
            location.project_id,
            location.supervisor_id,
            location.flwc_id,
            ccm.gender,
            ccm.age_group,
            COUNT(*) as children_in_month,
            SUM(alive_in_month) AS alive_in_month,
            SUM(thr_eligible) as thr_eligible,
            SUM(CASE WHEN days_thr_given>=25 THEN 1 ELSE 0 END) as thr_25_days,
            SUM(pse_eligible) as pse_eligible,
            SUM(CASE WHEN days_pse_attended>=25 THEN 1 ELSE 0 END) as pse_25_days,
            SUM(CASE WHEN days_snd_given>=25 THEN 1 ELSE 0 END) as snd_25_days,
            SUM(gm_eligible) as gm_eligible,
            SUM(CASE WHEN weight IS NOT NULL THEN 1 ELSE 0 END) as weight_measured_in_month,
            SUM(CASE WHEN height IS NOT NULL THEN 1 ELSE 0 END) as height_measured_in_month,
            SUM(CASE WHEN weight IS NOT NULL AND height IS NOT NULL THEN 1 ELSE 0 END) as weight_height_measured_in_month,
            SUM(CASE WHEN zscore_grading_hfa='severe' THEN 1 ELSE 0 END) as hfa_severe_in_month,
            SUM(CASE WHEN zscore_grading_hfa='moderate' THEN 1 ELSE 0 END) as hfa_moderate_in_month,
            SUM(CASE WHEN zscore_grading_hfa='normal' THEN 1 ELSE 0 END) as hfa_normal_in_month,

            SUM(CASE WHEN zscore_grading_wfh='severe' THEN 1 ELSE 0 END) as wfh_severe_in_month,
            SUM(CASE WHEN zscore_grading_wfh='moderate' THEN 1 ELSE 0 END) as wfh_moderate_in_month,
            SUM(CASE WHEN zscore_grading_wfh='normal' THEN 1 ELSE 0 END) as wfh_normal_in_month,

            SUM(CASE WHEN zscore_grading_wfh='severe' THEN 1 ELSE 0 END) as wfh_severe_in_month,
            SUM(CASE WHEN zscore_grading_wfh='moderate' THEN 1 ELSE 0 END) as wfh_moderate_in_month,
            SUM(CASE WHEN zscore_grading_wfh='normal' THEN 1 ELSE 0 END) as wfh_normal_in_month,
            SUM(born_in_month) as born_in_month,
            SUM(CASE WHEN born_in_month=1 AND immediate_bf=1 THEN 1 ELSE 0 END) as immediate_bf_in_month,
            SUM(CASE WHEN born_in_month=1 AND birth_weight is NOT NULL THEN 1 ELSE 0 END) as born_and_weighed_in_month,
            SUM(CASE WHEN born_in_month=1 AND birth_weight is NOT NULL AND low_birth_weight=1 THEN 1 ELSE 0 END) as low_birth_weight_in_month

        from '{self.location_table}' location
        LEFT JOIN '{self.child_care_monthly_table}' as ccm ON (
            location.flwc_id = ccm.flwc_id AND
            ccm.month = '{self.month}'
        )
        where location.location_level=5
        GROUP BY domain, month, state_id, district_id, project_id, supervisor_id, flwc_id, age_group, gender 

        """)

    def rollup_data(self, df, location_level):
        loc_info_suffix = ["id"]
        tmp_tablename = f"{clean_name(self.domain)}_agg_child_care_temp"

        df.createOrReplaceTempView(tmp_tablename)
        group_by = ['domain', 'month', 'gender', 'age_group'] + get_location_group_by_columns(location_level, loc_info_suffix)

        columns_calculation = get_location_column_rollup_calc(location_level, loc_info_suffix)
        columns_calculation += [
            ('location_level', f'{location_level}'),
            ('domain', f"'{self.domain}'"),
            ('month', f"'{self.month}'"),
            ('gender','gender'),
            ('age_group', 'age_group'),
            ('children_in_month',),
            ('alive_in_month',),
            ('thr_eligible',),
            ('thr_25_days',),
            ('pse_eligible',),
            ('pse_25_days',),
            ('snd_25_days',),
            ('gm_eligible',),
            ('weight_measured_in_month',),
            ('height_measured_in_month',),
            ('weight_height_measured_in_month',),
            ('hfa_severe_in_month',),
            ('hfa_moderate_in_month',),
            ('hfa_normal_in_month',),
            ('wfh_severe_in_month',),
            ('wfh_moderate_in_month',),
            ('wfh_normal_in_month',),
            ('born_in_month',),
            ('immediate_bf_in_month',),
            ('born_and_weighed_in_month',),
            ('low_birth_weight_in_month',),
        ]

        query = """
        SELECT
            {columns}
        FROM {table_name}
        WHERE location_level = {location_level}
        GROUP BY {group_by}
        """.format(
            columns=", ".join([f"{column[1]} AS {column[0]}" for column in columns_calculation]),
            table_name=tmp_tablename,
            group_by=", ".join(group_by),
            location_level=location_level+1
        )
        return SPARK.sql(query)
