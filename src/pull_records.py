
from collections import defaultdict
import psycopg2
from src.settings import DATABASE_SETTINGS


def pull_cases_with_meta(cases_meta):
    segregated_case_ids = segregate_by_case_type(cases_meta)
    cases_by_casetypes = defaultdict(list)
    for case_type, case_ids in segregated_case_ids.items():
        cases_by_casetypes[case_type] = pull_case_by_case_type(case_type, case_ids)
    return cases_by_casetypes


def segregate_by_case_type(cases_meta):
    casetype_wise_case_ids = defaultdict(list)
    for meta in cases_meta:
        case_info = meta['value']
        casetype_wise_case_ids[case_info['type']].append(case_info['document_id'])
    return casetype_wise_case_ids


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


def pull_ledgers_with_meta(ledgers_meta):
    pass


def pull_forms_with_meta(forms_meta):
    pass
