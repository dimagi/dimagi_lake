"""
Sudo code.


1. Create a spark session
2. pull the offsets
3. pull records from kafka using the offsets
4. Using the records pull:
    a. Cases/forms from ES
    b. Location information
5. combine Case/Form with location information for filtering by location
6. Merge the data into HDFS using Delta Lake
7. Save the new offsets to HDFS.
"""

from src.kafka_sink import KafkaSink
from src.spark_session_handler import SPARK
from src.settings import (
    KAFKA_BOOTSTRAP_SERVER_HOST,
    KAFKA_BOOTSTRAP_SERVER_PORT,
    KAFKA_CASE_TOPIC

)
import sys

kafka_sink = KafkaSink(spark_session=SPARK,
                       topic=sys.argv[1],
                       partition=0,
                       bootstrap_server=f"{KAFKA_BOOTSTRAP_SERVER_HOST}:{KAFKA_BOOTSTRAP_SERVER_PORT}"
                       )


if __name__ == '__main__':
    kafka_sink.sink_kafka()
