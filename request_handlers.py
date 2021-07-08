from consts import KAFKA_CASE_TOPIC, KAFKA_FORM_TOPIC, KAFKA_LOCATION_TOPIC
from dimagi_lake.aggregation.nutrition_project.table_mapping import TABLE_MAPPING
from dimagi_lake.ingestion.kafka_sink import (CaseKafkaSink, FormKafkaSink,
                                              LocationKafkaSink)
from dimagi_lake.migration.domain_migration import migrate_domain_tables
from env_settings import (KAFKA_BOOTSTRAP_SERVER_HOST,
                          KAFKA_BOOTSTRAP_SERVER_PORT)


def start_kafka_sink(args):
    """
    Starts the Spark Kafka Stream process, subscribes given kafka topic and start pulling data from it.
    """
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
    """
    Aggregates a given table for a given domain.
    Domain, Table name, and month are to be passed as argument while submitting spark job.
    """
    table = args[1]
    domain = args[2]
    month = args[3]

    table_meta = TABLE_MAPPING[table](domain, month)
    table_meta.aggregate()


def migrate_db(args):
    """
    To Create meta tables in the metastore for a given domain.
    Migration configuration files are present in `dimagi_lake/migration/migration_config` directory.
    """
    domain_name = args[1]
    migrate_domain_tables(domain_name)
