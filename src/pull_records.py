
from corehq.form_processor.interfaces.dbaccessors import CaseAccessors, FormAccessors



def pull_cases_by_ids(domain, case_ids):
    case_accessor = CaseAccessors(domain)
    return [case.to_json() for case in case_accessor.get_cases(list(case_ids))]


def pull_forms_by_ids(domain, form_ids):
    form_accessor = FormAccessors(domain)
    forms_json = [form.to_json() for form in form_accessor.get_forms(list(form_ids))]

    for form in forms_json:
        form['type'] = form['xmlns'].replace('http://openrosa.org/formdesigner/', '')
    return forms_json

