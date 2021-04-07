from abc import ABC
from spark_session_handler import SPARK


class BaseAggregationHelper(ABC):
    """
    Abstract Class defining the template for
    aggregation Helper class containing the aggregation logic.
    Each Aggregation helper is connected to different Table Schema class in aggregation/agg_table_schema
    with a class attribute `_aggregator`
    """
    def __init__(self, database_name, domain, month, schema):
        self.database_name = database_name
        self.domain = domain
        self.month = month
        self.schema = schema

    def aggregate(self):
        """
        Acts as an entry point to all the aggregation steps to follow for a particular table.
        Also responsible for calling each aggregation step in appropriate Order
        :return:
        """
        pass

    def agg_base_data(self):
        """
        calculates indicators for lowest level of location hierarchy
        """
        pass

    def rollup_data(self, df, location_level):
        """
        Aggregate the up in the location hierarchy from location_level+1  to location_level level
        """
        pass

    def enforce_schema(self, df):
        """
        Enforces table Schema defined by the corresponding Schema Class on the dataframe
        resulted after its aggregated.
        """
        for column in self.schema:
            df = df.withColumn(column.name, df[column.name].cast(column.dataType))

        return df
