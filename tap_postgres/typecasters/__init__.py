from tap_postgres.typecasters.invalid_timestamp_caster import register_type as register_invalid_timestamp_type

def register_type_casters(connection):
    register_invalid_timestamp_type(connection)