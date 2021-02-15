
import psycopg2
from src.settings import DATABASE_SETTINGS
import json


def pull_cases_with_meta(cases_meta):
    case_types = {json.loads(meta.value).type for meta in cases_meta}
    case_ids = {json.loads(meta.value).document_id for meta in cases_meta}
    return pull_case_by_case_type(case_types, case_ids)


def pull_forms_with_meta(cases_meta):
    form_types = {json.loads(meta.value).type.replace('') for meta in cases_meta}
    form_ids = {json.loads(meta.value).document_id for meta in cases_meta}
    return pull_form_by_form_type(form_types, form_ids)


#TODO  This would be changed while integrating with HQ
def pull_case_by_case_type(case_types, case_ids):
    conn = psycopg2.connect(**DATABASE_SETTINGS)
    cur = conn.cursor()
    sql = """
    SELECT record_json from cases where type in %s and doc_id in %s;
    """

    cur.execute(sql, (tuple(case_types), tuple(case_ids)))
    data = [record[0] for record in cur.fetchall()]

    return data

#TODO  This would be changed while integrating with HQ
def pull_form_by_form_type(form_types, form_ids):
    conn = psycopg2.connect(**DATABASE_SETTINGS)
    cur = conn.cursor()
    sql = """
    SELECT record_json from forms where type in %s and doc_id in %s;
    """
    cur.execute(sql, (tuple(form_types), tuple(form_ids)))
    data = [record[0] for record in cur.fetchall()]

    return data


def pull_ledgers_with_meta(ledgers_meta):
    pass

