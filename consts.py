
# KAFKA TOPICS
KAFKA_CASE_TOPIC = 'case-sql'
KAFKA_FORM_TOPIC = 'form-sql'
KAFKA_LOCATION_TOPIC = 'location'


MAX_RECORDS_TO_PROCESS = 50000
DATA_LAKE_DOMAIN = ['inp-dashboard']



# Datasource table names
RAW_LOCATION = 'raw_location'
RAW_AG_CASE = 'raw_ag_care_case'
RAW_CHILD_CARE_CASE = 'raw_child_care_case'
RAW_CHILD_GROWTH_CASE = 'raw_child_growth_case'
RAW_FAMILY_CASE = 'raw_family_case'
RAW_MATERNAL_CASE = 'raw_maternal_care_case'
RAW_MEMBER_CASE = 'raw_member_case'
RAW_WOMAN_CASE = 'raw_woman_care_case'
RAW_GROWTH_DEV_PW_LW_FORM = 'raw_gd_pw_lw'
RAW_PC_MW_FORM = 'raw_pc_mw'
RAW_DELIVERY_FORM = 'raw_delivery'
RAW_SND_AG_FORM = 'raw_snd_adol_ben'
RAW_MEM_REG_FORM = 'raw_member_reg'
RAW_ABORTION_FORM = 'raw_abortion'
RAW_EDIT_FAMILY_FORM = 'raw_pop_survey_edit_remove_family'
RAW_CONTACT_BOOK_FORM = 'raw_contact_book_form'
RAW_PREGNANCY_FORM = 'raw_pregnancy'
RAW_PC_LW_CHILD_0M_6M_FORM = 'raw_pc_lw_child_0m_6m'
RAW_PC_PW_FORM = 'raw_pc_pw'
RAW_SND_CHILD_3Y_6Y_FORM = 'raw_snd_child_3y_6y'
RAW_PC_AG_FORM = 'raw_pc_adol_girls'
RAW_FAMILY_REG_FORM = 'raw_family_reg'
RAW_PC_CHILD_6M_3Y_FORM = 'raw_pc_child_6m_3y'
RAW_GD_MW_FORM = 'raw_gd_mw'
RAW_GD_CHILD_0Y_5Y_FORM = 'raw_gd_child_0y_5y_w_h'
RAW_SND_MW_FORM = 'raw_snd_mw'
RAW_SND_PW_LW_FORM = 'raw_snd_pw_lw'
RAW_PC_CHILD_3Y_5Y_FORM = 'raw_pc_child_3y_5y'
RAW_SERVICE_ENROLLMENT_FORM = 'raw_service_enrollment'
RAW_SND_CHILD_6M_3Y_FORM = 'raw_snd_child_6m_3y'
RAW_GD_AG_FORM = 'raw_gd_adol_ben'
RAW_EDIT_REMOVE_MEMBER_FORM = 'raw_mem_mgt_edit_remove_member'

# TODO Split the different forms and cases types to be processed by separate process instances
ALLOWED_FORMS = [
    ("http://openrosa.org/formdesigner/23145798-6570-4EEA-BF9F-17AA80D99103",)
    ("http://openrosa.org/formdesigner/EB3CBEB0-396E-438B-AA81-DCC3CCD4E55E",)
    ("http://openrosa.org/formdesigner/74524A74-FD52-4DCE-AE38-F68E680475C4",RAW_FAMILY_REG)
    ("http://openrosa.org/formdesigner/F33C77FE-3F75-4714-A9FA-ED88A5298C40",RAW_MEM_REG)
    ("http://openrosa.org/formdesigner/E50A1244-B92D-41EF-A647-FE2D797E1F08",RAW_EDIT_FAMILY)
    ("http://openrosa.org/formdesigner/09B1E7B3-BB57-4A51-8CD3-114F1054F18E",RAW_EDIT_REMOVE_MEMBER)
    ("http://openrosa.org/formdesigner/33EBE6E1-28FE-4F59-89D0-200E66ECA6F5",RAW_SERVICE_ENROLLMENT)
    ("http://openrosa.org/formdesigner/C598D396-C69A-4F1B-84FD-92B582DDC57D",RAW_PREGNANCY)
    ("http://openrosa.org/formdesigner/EA556BA0-3F5D-4056-A49D-B847379EF8E2",RAW_ABORTION)
    ("http://openrosa.org/formdesigner/F8AE9C85-38E3-4B84-BD16-0198C4907FD9",RAW_DELIVERY)
    ("http://openrosa.org/formdesigner/260F776D-6EB0-4118-AD15-72B0275747A4",)
    ("http://openrosa.org/formdesigner/87149AFE-1EE9-4C80-AE07-FD12FA808D83",RAW_SND_CHILD_3Y_6Y)
    ("http://openrosa.org/formdesigner/109A04AF-409E-48B9-A2B0-C52B4CE72D91",RAW_GD_AG)
    ("http://openrosa.org/formdesigner/4FAB81D4-BBAF-424E-8DB1-96478791DB01",RAW_SND_PW_LW)
    ("http://openrosa.org/formdesigner/116A78AD-E839-4BD4-BA13-A534A4A85B3B",RAW_SND_CHILD_6M_3Y)
    ("http://openrosa.org/formdesigner/5C97F80A-439D-45A1-AFDD-E46395A274F0",RAW_SND_MW)
    ("http://openrosa.org/formdesigner/C94AEDD0-3EC8-4380-8272-9F16D217B53F",)
    ("http://openrosa.org/formdesigner/6595DEFD-60DA-4102-A704-F400D40F5F05",RAW_GD_CHILD_0Y_5Y)
    ("http://openrosa.org/formdesigner/A6B42E30-FF2B-422F-ADB1-A34EFC23EF04",)
    ("http://openrosa.org/formdesigner/FC61CD97-2F3D-4299-B5FF-485B436AD1B1",RAW_GROWTH_DEV_PW_LW_FORM)
    ("http://openrosa.org/formdesigner/6F3A860A-BB68-4446-BE88-7C6D2511D3FB",RAW_GD_MW)
    ("http://openrosa.org/formdesigner/F4A55B36-A5CA-4003-959E-9D4FB446D09E",RAW_SND_AG)
    ("http://openrosa.org/formdesigner/AF187CB9-961A-4717-8406-1DEA6F1524A2",RAW_PC_PW)
    ("http://openrosa.org/formdesigner/C182EC9B-BFA4-4D3A-8A76-E9F0C32E9B2C",RAW_PC_LW_CHILD_0M_6M)
    ("http://openrosa.org/formdesigner/73399EB5-4493-489C-925D-09240329D785",RAW_PC_CHILD_6M_3Y)
    ("http://openrosa.org/formdesigner/78AADB0E-1123-478E-9810-81FEA2C41840",RAW_PC_AG)
    ("http://openrosa.org/formdesigner/FB4D9AE6-4BB5-4589-BF4F-8259CF439636",RAW_PC_MW)
    ("http://openrosa.org/formdesigner/3869F531-5D2C-4993-B01D-5EB94DE53BA3",RAW_PC_CHILD_3Y_5Y)
    ("http://openrosa.org/formdesigner/D0317EF1-07AD-49A6-8D54-C6B5C12707DF",RAW_CONTACT_BOOK_FORM)
]
ALLOWED_CASES = [
    ("adolescent_care", RAW_AG_CASE),
    ("woman_care", RAW_WOMAN_CASE),
    ("child_care", RAW_CHILD_CARE_CASE),
    ("child_growth", RAW_CHILD_GROWTH_CASE),
    ("family", RAW_FAMILY_CASE),
    ("maternal_care", RAW_MATERNAL_CASE),
    ("member", RAW_MEMBER_CASE)
]

ORG_STRUCTURE = ['state', 'district', 'project', 'supervisor', 'flwc']



# Aggregation constants
# Following table names should match with the table name on dashboard db.
FLWC_LOCATION_TABLE = 'flwc_location'
