import re


def clean_name(table_name):
    # replace anything except a-z/A-Z/0-9/_ with `_`
    return re.sub('[^a-zA-Z0-9_\n]', '_', table_name)

