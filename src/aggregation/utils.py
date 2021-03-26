import re


def clean_tablename(table_name):
    # replace anything except a-z/A-Z/0-9/_ with `_`
    my_new_string = re.sub('[^a-zA-Z0-9_\n]', '_', table_name)

