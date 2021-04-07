from dimagi_lake.aggregation.aggregation_helpers.base_helper import \
    BaseAggregationHelper
from dimagi_lake.aggregation.utils import (get_location_column_rollup_calc,
                                           get_location_group_by_columns)
from dimagi_lake.utils import clean_name
from spark_session_handler import SPARK


class AggLocationHelper(BaseAggregationHelper):

    @property
    def source_tablename(self):
        return "raw_location"

    def aggregate(self):
        df = self.agg_base_data()
        for i in range(4, 0, -1):
            df = df.unionByName(self.rollup_data(df, i))
        return self.enforce_schema(df)

    def agg_base_data(self):
        df = SPARK.sql(f"""
        SELECT 
            flwc_id,
            flwc_site_code,
            flwc_name,
            supervisor_id,
            supervisor_site_code,
            supervisor_name,
            project_id,
            project_site_code,
            project_name,
            district_id,
            district_site_code,
            district_name,
            state_id,
            state_site_code,
            state_name,
            5 as location_level,
            '{self.domain}' as domain
        FROM {self.database_name}.{self.source_tablename} as source_table
        WHERE (
            state_is_archived is distinct from true AND
            district_is_archived is distinct from true AND
            project_is_archived is distinct from true AND
            supervisor_is_archived is distinct from true AND
            flwc_is_archived is distinct from true
        )
        """)
        return df

    def rollup_data(self, df, location_level):
        loc_info_suffix = ["id", "name", "site_code"]
        tmp_tablename = f"{clean_name(self.domain)}_flwc_location_temp"
        df.createOrReplaceTempView(tmp_tablename)
        columns_calculation = get_location_column_rollup_calc(location_level, loc_info_suffix)
        columns_calculation += [
            ('location_level', f'{location_level}'),
            ('domain', f"'{self.domain}'")
        ]

        group_by = ['domain'] + get_location_group_by_columns(location_level, loc_info_suffix)

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
