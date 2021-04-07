from consts import ORG_STRUCTURE


def get_location_group_by_columns(location_level, loc_info_suffix):
    group_by = ["state_{}".format(name) for name in loc_info_suffix]

    if location_level > 1:
        group_by.extend(["district_{}".format(name) for name in loc_info_suffix])
    if location_level > 2:
        group_by.extend(["project_{}".format(name) for name in loc_info_suffix])
    if location_level > 3:
        group_by.extend(["supervisor_{}".format(name) for name in loc_info_suffix])
    return group_by


def get_location_column_rollup_calc(location_level, loc_info_suffix):
    column_and_calculation = list()

    for index, loc_level in enumerate(ORG_STRUCTURE):
        column_and_calculation.extend([
            (f"{loc_level}_{suffix}", f"{loc_level}_{suffix}" if location_level > index else "'ALL'")
            for suffix in loc_info_suffix
        ])
    return column_and_calculation
