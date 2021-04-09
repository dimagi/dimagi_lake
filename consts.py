
# KAFKA TOPICS
KAFKA_CASE_TOPIC = 'case-sql'
KAFKA_FORM_TOPIC = 'form-sql'
KAFKA_LOCATION_TOPIC = 'location'


MAX_RECORDS_TO_PROCESS = 50000
DATA_LAKE_DOMAIN = ['inp-dashboard']

# TODO Split the different forms and cases types to be processed by separate process instances
ALLOWED_FORMS = [
    "http://openrosa.org/formdesigner/23145798-6570-4EEA-BF9F-17AA80D99103",
    "http://openrosa.org/formdesigner/EB3CBEB0-396E-438B-AA81-DCC3CCD4E55E",
    "http://openrosa.org/formdesigner/74524A74-FD52-4DCE-AE38-F68E680475C4",
    "http://openrosa.org/formdesigner/F33C77FE-3F75-4714-A9FA-ED88A5298C40",
    "http://openrosa.org/formdesigner/E50A1244-B92D-41EF-A647-FE2D797E1F08",
    "http://openrosa.org/formdesigner/09B1E7B3-BB57-4A51-8CD3-114F1054F18E",
    "http://openrosa.org/formdesigner/33EBE6E1-28FE-4F59-89D0-200E66ECA6F5",
    "http://openrosa.org/formdesigner/C598D396-C69A-4F1B-84FD-92B582DDC57D",
    "http://openrosa.org/formdesigner/EA556BA0-3F5D-4056-A49D-B847379EF8E2",
    "http://openrosa.org/formdesigner/F8AE9C85-38E3-4B84-BD16-0198C4907FD9",
    "http://openrosa.org/formdesigner/260F776D-6EB0-4118-AD15-72B0275747A4",
    "http://openrosa.org/formdesigner/87149AFE-1EE9-4C80-AE07-FD12FA808D83",
    "http://openrosa.org/formdesigner/109A04AF-409E-48B9-A2B0-C52B4CE72D91",
    "http://openrosa.org/formdesigner/4FAB81D4-BBAF-424E-8DB1-96478791DB01",
    "http://openrosa.org/formdesigner/116A78AD-E839-4BD4-BA13-A534A4A85B3B",
    "http://openrosa.org/formdesigner/5C97F80A-439D-45A1-AFDD-E46395A274F0",
    "http://openrosa.org/formdesigner/C94AEDD0-3EC8-4380-8272-9F16D217B53F",
    "http://openrosa.org/formdesigner/6595DEFD-60DA-4102-A704-F400D40F5F05",
    "http://openrosa.org/formdesigner/A6B42E30-FF2B-422F-ADB1-A34EFC23EF04",
    "http://openrosa.org/formdesigner/FC61CD97-2F3D-4299-B5FF-485B436AD1B1",
    "http://openrosa.org/formdesigner/6F3A860A-BB68-4446-BE88-7C6D2511D3FB",
    "http://openrosa.org/formdesigner/F4A55B36-A5CA-4003-959E-9D4FB446D09E",
    "http://openrosa.org/formdesigner/AF187CB9-961A-4717-8406-1DEA6F1524A2",
    "http://openrosa.org/formdesigner/C182EC9B-BFA4-4D3A-8A76-E9F0C32E9B2C",
    "http://openrosa.org/formdesigner/73399EB5-4493-489C-925D-09240329D785",
    "http://openrosa.org/formdesigner/78AADB0E-1123-478E-9810-81FEA2C41840",
    "http://openrosa.org/formdesigner/FB4D9AE6-4BB5-4589-BF4F-8259CF439636",
    "http://openrosa.org/formdesigner/3869F531-5D2C-4993-B01D-5EB94DE53BA3",
    "http://openrosa.org/formdesigner/D0317EF1-07AD-49A6-8D54-C6B5C12707DF"
]
ALLOWED_CASES = [
    "person_case",
    "flw",
    "adolescent_care",
    "woman_care",
    "child_care",
    "child_growth",
    "family",
    "maternal_care",
    "member",
    "commcare-case"
]

ORG_STRUCTURE = ['state', 'district', 'project', 'supervisor', 'flwc']

# Aggregation constants
# Following table names should match with the table name on dashboard db.
FLWC_LOCATION_TABLE = 'flwc_location'
CHILD_CARE_MONTHLY_TABLE = 'child_care_monthly'
SERVICE_ENROLLMENT_TABLE = 'service_enrollment_form'
CHILD_WEIGHT_HEIGHT_FORM_TABLE = 'child_weight_height_form'
CHILD_THR_FORM_TABLE = 'child_thr_form'
SUPPLEMENTARY_NUTRITION_FORM_TABLE = 'supplementary_nutrition_form'
AGG_CHILD_CARE_TABLE = 'agg_child_care'