import tap_postgres.typecasters.invalid_timestamp_caster as invalid_timestamp_caster

def register_type_casters(connection):
    invalid_timestamp_caster.register_type(connection)