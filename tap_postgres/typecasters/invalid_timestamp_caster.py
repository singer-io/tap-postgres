import psycopg2

InvalidDate = None

 # This function defines types cast, and as returned database literal is already a string, no  additional logic required
def cast_invalid_timestamp(value, cursor):
    return value

def register_type(connection):
    global InvalidDate
    if not InvalidDate:
        cursor = connection.cursor()
        # some databases have timestamp defaults of 0001-01-01... instead of NULL defaults
        cursor.execute("select to_timestamp('0001-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS')::timestamp without time zone")
        psql_timestamp_oid = cursor.description[0][1]

        InvalidDate = psycopg2.extensions.new_type((psql_timestamp_oid,), 'TIMESTAMP WITHOUT TIMEZONE', cast_invalid_timestamp)
        psycopg2.extensions.register_type(InvalidDate)

