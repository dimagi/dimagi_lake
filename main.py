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
import sys
import os
import yaml
import request_handlers


def load_application_config():
    app_config = open(f"{os.getenv('DIMAGI_LAKE_DIR')}/application_config.yaml")
    return yaml.load(app_config, Loader=yaml.FullLoader)


if __name__ == '__main__':
    operation = sys.argv[1]
    app_config = load_application_config()
    if operation not in app_config['operations']:
        valid_ops = ' | '.join(app_config['operations'].keys())
        print(f"Invalid Operation for spark application. Valid operations are: {valid_ops}")

    handler = app_config['operations'][operation]['request_handler']

    request_handler = getattr(request_handlers, handler)
    request_handler(sys.argv[1:])
