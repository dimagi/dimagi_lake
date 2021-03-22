from corehq.form_processor.interfaces.dbaccessors import CaseAccessors, FormAccessors
from spark_session_handler import SPARK
from src.ingestion.transforms import (
    merge_location_information,
    flatten_json,
    json_dump,
    add_type_to_form,
    month_column
)


class Processor:
    pre_map_transformations = [  # Order Matters
        merge_location_information,
        month_column,
        flatten_json,
        json_dump
    ]
    record_accessor = None

    def __init__(self, domain, record_ids):
        self.domain = domain
        self.accessor = self.record_accessor(domain)
        self.record_ids = record_ids

    def get_processed_records(self):
        records = self.pull_records()
        records_rdd = self.pre_tranform(records)
        records_df = SPARK.read.json(records_rdd)
        processed_record_doc = self.post_transform(records_df)
        return processed_record_doc

    def pull_records(self):
        pass

    def pre_tranform(self, records):

        def map_transformations(record):
            for tranformation in self.pre_map_transformations:
                record = tranformation(record)
            return record

        records_rdd = SPARK.sparkContext.parallelize(records)

        return records_rdd.map(map_transformations)

    def post_transform(self, df):
        return df

    def filter(self):
        pass


class CaseProcessor(Processor):
    record_accessor = CaseAccessors

    def pull_records(self):
        return [case.to_json() for case in self.accessor.get_cases(list(self.record_ids))]


class FormProcessor(Processor):

    pre_map_transformations = [  # Order Matters
        add_type_to_form,
        merge_location_information,
        month_column,
        flatten_json,
        json_dump
    ]
    record_accessor = FormAccessors

    def pull_records(self):
        return [form.to_json() for form in self.accessor.get_forms(list(self.record_ids))]
