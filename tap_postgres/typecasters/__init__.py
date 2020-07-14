from tap_postgres.typecasters import invalid_timestamp_caster

def register_type_casters(connection):
    invalid_timestamp_caster.register_type(connection)