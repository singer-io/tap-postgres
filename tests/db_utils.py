import os
import psycopg2
from psycopg2.extensions import quote_ident

def ensure_environment_variables_set():
    missing_envs = [x for x in [os.getenv('TAP_POSTGRES_HOST'),
                                os.getenv('TAP_POSTGRES_USER'),
                                os.getenv('TAP_POSTGRES_PASSWORD'),
                                os.getenv('TAP_POSTGRES_PORT'),
                                os.getenv('TAP_POSTGRES_DBNAME')] if x == None]
    if len(missing_envs) != 0:
        #pylint: disable=line-too-long
        raise Exception("set TAP_POSTGRES_HOST, TAP_POSTGRES_DBNAME, TAP_POSTGRES_USER, TAP_POSTGRES_PASSWORD, TAP_POSTGRES_PORT")

def ensure_db(dbname=os.getenv('TAP_POSTGRES_DBNAME')):
    # Create database dev if not exists
    with get_test_connection() as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = '{}'".format(dbname))
            exists = cur.fetchone()
            if not exists:
                print("Creating database {}".format(dbname))
                cur.execute("CREATE DATABASE {}".format(dbname))

def get_test_connection(dbname=os.getenv('TAP_POSTGRES_DBNAME'), logical_replication=False):
    conn_string = "host='{}' dbname='{}' user='{}' password='{}' port='{}'".format(os.getenv('TAP_POSTGRES_HOST'),
                                                                                   dbname,
                                                                                   os.getenv('TAP_POSTGRES_USER'),
                                                                                   os.getenv('TAP_POSTGRES_PASSWORD'),
                                                                                   os.getenv('TAP_POSTGRES_PORT'))
    if logical_replication:
        return psycopg2.connect(conn_string, connection_factory=psycopg2.extras.LogicalReplicationConnection)
    else:
        return psycopg2.connect(conn_string)

def canonicalized_table_name(conn_cursor, schema, table):
    return "{}.{}".format(quote_ident(schema, conn_cursor), quote_ident(table, conn_cursor))

def ensure_replication_slot(conn_cursor, slot_name='stitch'):
    conn_cursor.execute("""SELECT EXISTS (
                  SELECT 1
                  FROM  pg_replication_slots
                  WHERE  slot_name = '{}') """, slot_name)

    old_slot = conn_cursor.fetchone()[0]

    with get_test_connection('dev', True) as conn2:
        with conn2.cursor() as conn_2_cursor:
            if old_slot:
                conn_2_cursor.drop_replication_slot(slot_name)
            conn_2_cursor.create_replication_slot(slot_name, output_plugin='wal2json')

def ensure_table(conn, conn_cursor, schema_name, table_name):
    ctable_name = canonicalized_table_name(conn_cursor, schema_name, table_name)

    old_table = conn_cursor.execute("""SELECT EXISTS (
                              SELECT 1
                              FROM  information_schema.tables
                              WHERE  table_schema = %s
                              AND  table_name =   %s);""",
                            [schema_name, table_name])
    old_table = conn_cursor.fetchone()[0]
    if old_table:
        conn_cursor.execute("DROP TABLE {}".format(ctable_name))


    conn_cursor2 = conn.cursor()
    conn_cursor2.execute(""" SELECT installed_version FROM pg_available_extensions WHERE name = 'hstore' """)
    if conn_cursor2.fetchone()[0] is None:
        conn_cursor2.execute(""" CREATE EXTENSION hstore; """)

    conn_cursor2.execute(""" CREATE EXTENSION IF NOT EXISTS citext WITH SCHEMA public;""")
    conn_cursor2.execute(""" DROP TYPE IF EXISTS ALIGNMENT CASCADE """)
    conn_cursor2.execute(""" CREATE TYPE ALIGNMENT AS ENUM ('good', 'bad', 'ugly') """)

    return conn_cursor2

def insert_record(conn_cursor, table_name, data):
    our_keys = list(data.keys())
    our_keys.sort()
    our_values = list(map( lambda k: data.get(k), our_keys))


    columns_sql = ", \n ".join(our_keys)
    value_sql = ",".join(["%s" for i in range(len(our_keys))])

    insert_sql = """ INSERT INTO {}
                            ( {} )
                     VALUES ( {} )""".format(quote_ident(table_name, conn_cursor), columns_sql, value_sql)
    conn_cursor.execute(insert_sql, our_values)
