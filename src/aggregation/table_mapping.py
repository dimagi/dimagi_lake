from src.aggregation.agg_table_schema import (
    FlwcLocation,
    ChildCareMonthly,
    ServiceEnrollment,
    SupplementaryNutritionForm,
    GrowthMonitoring,
    ChildTHRForm
)

TABLE_MAPPING = {
    'location': FlwcLocation,
    'child_care': ChildCareMonthly,
    'service_enrollment': ServiceEnrollment,
    'supplementary_nutrition': SupplementaryNutritionForm,
    'growth_monitoring': GrowthMonitoring,
    'child_thr': ChildTHRForm
}