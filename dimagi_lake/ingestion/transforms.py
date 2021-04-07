import json
from datetime import datetime

from corehq.apps.es import users
from corehq.apps.locations.models import SQLLocation
from corehq.util.json import CommCareJSONEncoder

from dimagi_lake.ingestion.utils import trim_xmlns_id


def flatten_json(data_dict, sep='__'):
    out = {}

    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + sep)
        else:
            out[name[:-len(sep)]] = x

    flatten(data_dict)

    return out


def add_type_to_form(data_dict):
    data_dict['type'] = trim_xmlns_id(data_dict['xmlns'])
    return data_dict


def month_column(data_dict):
    date_format = "%Y-%m-%dT%H:%M:%S.%fZ"

    if data_dict['doc_type'] == 'CommCareCase':
        record_date = data_dict['opened_on']
    else:
        record_date = data_dict['form']['meta']['timeEnd']

    data_dict['month'] = datetime.strptime(record_date, date_format).strftime('%Y-%m-1')

    return data_dict


def json_dump(data_dict):
    return json.dumps(data_dict, cls=CommCareJSONEncoder).replace(': []', ': [{}]')


def merge_location_information(record):
    def get_user_id(doc):
        return doc.get('user_id') or doc.get('meta', {}).get('userID')

    user_id = get_user_id(record)

    user_ids = list()
    if user_id is not None:
        user_ids = [user_id]

    user_with_loc = {user['_id']: user['location_id'] for user in (users.UserES()
                                                                   .user_ids(user_ids)
                                                                   .fields(['_id', 'location_id'])
                                                                   .run().hits)}

    if not (user_id and user_with_loc.get(user_id)):
        record['supervisor_id'] = None
    else:
        location_id = user_with_loc.get(user_id)
        ancestors = SQLLocation.by_location_id(location_id).get_ancestors(include_self=True)
        for loc in ancestors:
            location_type = loc.location_type.name
            record[f"{location_type}_id"] = loc.location_id

    return record


def prepare_location_hierarchy(loc_id):
    location = SQLLocation.by_location_id(loc_id)
    location_hierarchy = location.get_ancestors(include_self=True)

    location_record = dict()

    for loc in location_hierarchy:
        location_record[f"{loc.location_type.name}_id"] = loc.location_id
        location_record[f"{loc.location_type.name}_name"] = loc.name
        location_record[f"{loc.location_type.name}_site_code"] = loc.site_code
        location_record[f"{loc.location_type.name}_latitude"] = loc.latitude
        location_record[f"{loc.location_type.name}_longitude"] = loc.longitude
        location_record[f"{loc.location_type.name}_is_archived"] = loc.is_archived

    location_record['_id'] = location.location_id
    return location_record
