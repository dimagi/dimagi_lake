from src.ingestion.kafka_sink import KafkaSink
from src.spark_session_handler import SPARK
from src.settings import (
    KAFKA_BOOTSTRAP_SERVER_HOST,
    KAFKA_BOOTSTRAP_SERVER_PORT

)


def start_kafka_sink(args):
    kafka_sink = KafkaSink(topic=args[0],
                           bootstrap_server=f"{KAFKA_BOOTSTRAP_SERVER_HOST}:{KAFKA_BOOTSTRAP_SERVER_PORT}"
                           )
    kafka_sink.sink_kafka()

