
def create_domain_table_query(parent_table, table_name, domain):
    return f"""
    CREATE TABLE IF NOT EXISTS {table_name}
    (
        CHECK (domain='{domain}'),
        LIKE {parent_table} INCLUDING INDEXES INCLUDING CONSTRAINTS INCLUDING DEFAULTS
    )
    """


def drop_table_query(table_name):
    return f"DROP TABLE IF EXISTS {table_name}"


def detach_partition_query(parent_table, partition_table):
    return f"""
    ALTER TABLE IF EXISTS "{partition_table}" NO INHERIT "{parent_table}"
    """


def rename_table_query(table_name, new_table_name):
    return f"""
    ALTER TABLE IF EXISTS "{table_name}" RENAME TO "{new_table_name}"
    """


def attach_partition_query(parent_table, partition_table):
    return f"""
    ALTER TABLE IF EXISTS "{partition_table}" INHERIT "{parent_table}"
    """

