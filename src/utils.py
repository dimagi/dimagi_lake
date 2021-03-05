import json
from corehq.util.json import CommCareJSONEncoder


def flatten_json(data_dict, sep='__'):
    out = {}

    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + sep)
        else:
            out[name[:-len(sep)]] = x

    flatten(data_dict)
    return json.dumps(out, cls=CommCareJSONEncoder)


def custom_tranformation(record):
    return flatten_json(record)
