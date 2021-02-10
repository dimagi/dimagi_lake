
from collections import defaultdict
import psycopg2
from src.settings import DATABASE_SETTINGS


def pull_cases_with_meta(cases_meta):
    segregated_case_ids = segregate_by_doc_type(cases_meta)
    cases_by_casetypes = defaultdict(list)
    for case_type, case_ids in segregated_case_ids.items():
        cases_by_casetypes[case_type] = pull_case_by_case_type(case_type, case_ids)
    return cases_by_casetypes


def segregate_by_doc_type(docs_meta):
    doc_ids_by_doc_type = defaultdict(list)
    for meta in docs_meta:
        doc_info = meta['value']
        doc_ids_by_doc_type[doc_info['type']].append(doc_info['document_id'])
    return doc_ids_by_doc_type


#TODO  This would be changed while integrating with HQ
def pull_case_by_case_type(case_type, case_ids):
    conn = psycopg2.connect(**DATABASE_SETTINGS)
    cur = conn.cursor()
    sql = """
    SELECT record_json from cases where type=%s and doc_id in %s;
    """

    cur.execute(sql,(case_type,tuple(case_ids)))
    data = [record[0] for record in cur.fetchall()]

    return data


#TODO  This would be changed while integrating with HQ
def pull_form_by_form_type(form_type, form_ids):
    conn = psycopg2.connect(**DATABASE_SETTINGS)
    cur = conn.cursor()
    sql = """
    SELECT record_json from forms where type=%s and doc_id in %s;
    """

    cur.execute(sql,(form_type,tuple(form_ids)))
    data = [record[0] for record in cur.fetchall()]

    return data


def pull_ledgers_with_meta(ledgers_meta):
    pass


def pull_forms_with_meta(forms_meta):
    segregated_form_ids = segregate_by_doc_type(forms_meta)
    form_ids_by_type = defaultdict(list)
    for form_type, form_ids in segregated_form_ids.items():
        form_ids_by_type[form_type] = pull_case_by_case_type(form_type, form_ids)
    return form_ids_by_type
