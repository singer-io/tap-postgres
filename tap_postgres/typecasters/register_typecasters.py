import tap_postgres.typecasters.invalid_timestamp_caster as invalid_timestamp

def register_typecasters(connection):
    invalid_timestamp.register_type(connection)