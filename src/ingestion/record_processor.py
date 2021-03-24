from corehq.form_processor.interfaces.dbaccessors import CaseAccessors, FormAccessors
from corehq.apps.locations.models import SQLLocation
from spark_session_handler import SPARK
from src.ingestion.transforms import (
    merge_location_information,
    flatten_json,
    json_dump,
    add_type_to_form,
    month_column,
    prepare_location_hierarchy
)


class Processor:
    partition_columns = ('owner_id',)
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
        processed_record_docs = self.post_transform(records_df)
        return processed_record_docs

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
    partition_columns = ('month', 'supervisor_id')
    record_accessor = CaseAccessors

    def pull_records(self):
        return [case.to_json() for case in self.accessor.get_cases(list(self.record_ids))]


class FormProcessor(Processor):
    partition_columns = ('month', 'supervisor_id')
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


class LocationProcessor(Processor):
    pre_map_transformations = [  # Order Matters
        prepare_location_hierarchy,
        flatten_json,
        json_dump
    ]
    partition_columns = ('supervisor_id',)

    def __init__(self, domain, record_ids):
        self.domain = domain
        self.record_ids = record_ids

    def pull_records(self):
        unique_loc_ids = set(self.record_ids)
        locations = SQLLocation.object.filter(location_id__in=unique_loc_ids)

        records = set()
        for location in locations:
            # Could not find a better way to find the lowest level descendants
            descendants = location.get_descendants(include_self=True).filter(location_type__name='flwc')
            records.update({loc.location_id for loc in descendants})

        return list(records)
