from src.aggregation.table_mapping import TABLE_MAPPING
from src.ingestion.kafka_sink import CaseKafkaSink, FormKafkaSink, LocationKafkaSink
from src.migration.domain_migration import migrate_domain_tables
from datalake_conts import (
    KAFKA_BOOTSTRAP_SERVER_HOST,
    KAFKA_BOOTSTRAP_SERVER_PORT,
    KAFKA_FORM_TOPIC,
    KAFKA_CASE_TOPIC,
    KAFKA_LOCATION_TOPIC

)


def start_kafka_sink(args):
    topic = args[1]
    if topic == KAFKA_FORM_TOPIC:
        kafka_sink_cls = FormKafkaSink
    elif topic == KAFKA_CASE_TOPIC:
        kafka_sink_cls = CaseKafkaSink
    elif topic == KAFKA_LOCATION_TOPIC:
        kafka_sink_cls = LocationKafkaSink

    bootstrap_server = f"{KAFKA_BOOTSTRAP_SERVER_HOST}:{KAFKA_BOOTSTRAP_SERVER_PORT}"
    kafka_sink = kafka_sink_cls(bootstrap_server=bootstrap_server)
    kafka_sink.sink_kafka()


def aggregate_table(args):
    table = args[1]
    domain = args[2]
    month = args[3]

    table_meta = TABLE_MAPPING[table](domain, month)
    table_meta.aggregate()


def migrate_db(args):
    domain_name = args[1]
    migrate_domain_tables(domain_name)
