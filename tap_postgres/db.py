def canonicalize_identifier(identifier):
    return identifier

def fully_qualified_column_name(schema, table, column):
    return '"{}"."{}"."{}"'.format(canonicalize_identifier(schema), canonicalize_identifier(table), canonicalize_identifier(column))


def fully_qualified_table_name(schema, table):
    return '"{}"."{}"."{}"'.format(canonicalize_identifier(schema), canonicalize_identifier(table))
