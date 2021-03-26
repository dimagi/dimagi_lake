from spark_session_handler import SPARK


class BaseAggregationHelper:
    def __init__(self, database_name, domain, month, schema):
        self.database_name = database_name
        self.domain = domain
        self.month = month
        self.schema = schema

    def aggregate(self):
        pass

    def agg_base_data(self):
        pass

    def rollup_data(self):
        pass

    def enforce_schema(self, df):
        return SPARK.createDataFrame(df.collect(), self.schema)
