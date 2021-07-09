from dimagi_lake.aggregation.agg_table_schema import (ChildCareMonthly,
                                                      FlwcLocation,
                                                      ServiceEnrollment,
                                                      ChildWeightHeightForm,
                                                      SupplementaryNutritionForm,
                                                      ChildTHRForm)

TABLE_MAPPING = {
    'location': FlwcLocation,
    'child_care': ChildCareMonthly,
    'service_enrollment': ServiceEnrollment,
    'child_weight_height': ChildWeightHeightForm,
    'supplementary_nutrition': SupplementaryNutritionForm,
    'child_thr': ChildTHRForm
}
