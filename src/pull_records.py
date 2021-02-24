
from src.settings import DATA_LAKE_DOMAIN
import json

from corehq.form_processor.interfaces.dbaccessors import CaseAccessors, FormAccessors


def pull_cases_with_meta(cases_meta):
    case_ids = {json.loads(meta.value)['document_id'] for meta in cases_meta}
    return pull_cases_by_ids(case_ids)


def pull_forms_with_meta(forms_meta):
    form_ids = {json.loads(meta.value)['document_id'] for meta in forms_meta}
    return pull_forms_by_ids(form_ids)


def pull_cases_by_ids(case_ids):
    case_accessor = CaseAccessors(DATA_LAKE_DOMAIN)
    return [case.to_json() for case in case_accessor.get_cases(list(case_ids))]


def pull_forms_by_ids(form_ids):
    form_accessor = FormAccessors(DATA_LAKE_DOMAIN)
    forms_json = [form.to_json() for form in form_accessor.get_forms(list(form_ids))]

    for form in forms_json:
        form['type'] = form['xmlns'].replace('http://openrosa.org/formdesigner/', '')
        form['_id'] = form['meta']['instanceID']
    return forms_json

