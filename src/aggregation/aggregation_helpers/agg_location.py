from src.aggregation.aggregation_helpers.base_helper import BaseAggregationHelper
from spark_session_handler import SPARK
from src.aggregation.utils import clean_tablename

class AggLocationHelper(BaseAggregationHelper):

    @property
    def source_tablename(self):
        return "raw_location"

    def aggregate(self):
        df = self.agg_base_data()
        for i in range(4, 0, -1):
            df = df.union(self.rollup_data(df, i))

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

        tmp_tablename = f"{clean_tablename(self.domain)}_flwc_location_temp"
        df.createOrReplaceTempView(tmp_tablename)

        columns_calculation = [
            ('flwc_id', 'flwc_id' if location_level > 4 else "'ALL'"),
            ('flwc_site_code', 'flwc_site_code' if location_level > 4 else "'ALL'"),
            ('flwc_name', 'flwc_name' if location_level > 4 else "'ALL'"),
            ('supervisor_id', 'supervisor_id' if location_level > 3 else "'ALL'"),
            ('supervisor_site_code', 'supervisor_site_code' if location_level > 3 else "'ALL'"),
            ('supervisor_name', 'supervisor_name' if location_level > 3 else "'ALL'"),
            ('project_id', 'project_id' if location_level > 2 else "'ALL'"),
            ('project_site_code', 'project_site_code' if location_level > 2 else "'ALL'"),
            ('project_name', 'project_name' if location_level > 2 else "'ALL'"),
            ('district_id', 'district_id' if location_level > 1 else "'ALL'"),
            ('district_site_code', 'district_site_code' if location_level > 1 else "'ALL'"),
            ('district_name', 'district_name' if location_level > 1 else "'ALL'"),
            ('state_id', 'state_id' if location_level > 0 else "'ALL'"),
            ('state_site_code', 'state_site_code' if location_level > 0 else "'ALL'"),
            ('state_name', 'state_name' if location_level > 0 else "'ALL'"),
            ('location_level', f'{location_level}'),
            ('domain', f"'{self.domain}'")
        ]

        loc_info_suffix = ["id", "name", "site_code"]
        group_by = ['domain'] + ["state_{}".format(name) for name in loc_info_suffix]

        if location_level > 1:
            group_by.extend(["district_{}".format(name) for name in loc_info_suffix])
        if location_level > 2:
            group_by.extend(["project_{}".format(name) for name in loc_info_suffix])
        if location_level > 3:
            group_by.extend(["supervisor_{}".format(name) for name in loc_info_suffix])

        query = """
        SELECT
            {columns}
        FROM {table_name}
        WHERE location_level={location_level}
        GROUP BY {group_by}
        """.format(
            columns=", ".join([f"{column[1]} AS {column[0]}" for column in columns_calculation]),
            table_name=tmp_tablename,
            group_by=", ".join(group_by),
            location_level=location_level+1
        )
        return SPARK.sql(query)
