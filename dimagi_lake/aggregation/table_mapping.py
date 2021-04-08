from dimagi_lake.aggregation.agg_table_schema import (ChildCareMonthly,
                                                      FlwcLocation,
                                                      ServiceEnrollment)

TABLE_MAPPING = {
    'location': FlwcLocation,
    'child_care': ChildCareMonthly,
    'service_enrollment': ServiceEnrollment,
}
