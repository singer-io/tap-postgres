from singer import get_logger, metadata
from nose.tools import nottest
import psycopg2
import singer
import os
import decimal
import math
import datetime
import pdb
from psycopg2.extensions import quote_ident

LOGGER = get_logger()

def get_test_connection():
    creds = {}
    missing_envs = [x for x in [os.getenv('TAP_POSTGRES_HOST'),
                                os.getenv('TAP_POSTGRES_USER'),
                                os.getenv('TAP_POSTGRES_PASSWORD'),
                                os.getenv('TAP_POSTGRES_PORT')
                                # os.getenv('TAP_POSTGRES_DATABASE')
    ] if x == None]
    if len(missing_envs) != 0:
        #pylint: disable=line-too-long
        raise Exception("set TAP_POSTGRES_HOST, TAP_POSTGRES_USER, TAP_POSTGRES_PASSWORD, TAP_POSTGRES_PORT, TAP_POSTGRES_DB")

    creds['host'] = os.environ.get('TAP_POSTGRES_HOST')
    creds['user'] = os.environ.get('TAP_POSTGRES_USER')
    creds['password'] = os.environ.get('TAP_POSTGRES_PASSWORD')
    creds['port'] = os.environ.get('TAP_POSTGRES_PORT')
    # creds['DB'] = os.environ.get('TAP_POSTGRES_DB')
    conn_string = "host='{}'  user='{}' password='{}' port='{}'".format(creds['host'],
                                                                        creds['user'],
                                                                        creds['password'],
                                                                        creds['port'])

    conn = psycopg2.connect(conn_string)

    return conn

def build_col_sql( col):
    col_sql = "{} {}".format(col['name'], col['type'])

    return col_sql

def build_table(table, cur):
    create_sql = "CREATE TABLE {}\n".format(quote_ident(table['name'], cur))
    col_sql = map(build_col_sql, table['columns'])
    pks = [c['name'] for c in table['columns'] if c.get('primary_key')]
    if len(pks) != 0:
        pk_sql = ",\n CONSTRAINT {}  PRIMARY KEY({})".format(quote_ident(table['name'] + "_pk", cur), " ,".join(pks))
    else:
       pk_sql = ""

    sql = "{} ( {} {})".format(create_sql, ",\n".join(col_sql), pk_sql)

    return sql

@nottest
def ensure_test_table(table_spec):


    with get_test_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            sql = """SELECT *
                       FROM information_schema.tables
                      WHERE table_schema = 'public'
                        AND table_name = %s"""

            cur.execute(sql,
                        [table_spec['name']])
            old_table = cur.fetchall()

            if len(old_table) != 0:
                drop_sql = "DROP TABLE {}".format(quote_ident(table_spec['name'], cur))
                cur.execute(drop_sql)

            sql = build_table(table_spec, cur)
            LOGGER.info("create table sql: %s", sql)
            cur.execute(sql)

def unselect_column(our_stream, col):
    md = metadata.to_map(our_stream.metadata)
    md.get(('properties', col))['selected'] = False
    our_stream.metadata = metadata.to_list(md)
    return our_stream

def set_replication_method_for_stream(stream, method):
    new_md = metadata.to_map(stream.metadata)
    old_md = new_md.get(())
    old_md.update({'replication-method': method})

    stream.metadata = metadata.to_list(new_md)
    return stream

def select_all_of_stream(stream):
    new_md = metadata.to_map(stream.metadata)

    old_md = new_md.get(())
    old_md.update({'selected': True})
    for col_name, col_schema in stream.schema.properties.items():
        #explicitly select column if it is not automatic
        if new_md.get(('properties', col_name)).get('inclusion') != 'automatic' and new_md.get(('properties', col_name)).get('inclusion') != 'unsupported':
            old_md = new_md.get(('properties', col_name))
            old_md.update({'selected' : True})

    stream.metadata = metadata.to_list(new_md)
    return stream


def crud_up_value(value):
    if isinstance(value, str):
        return "'" + value + "'"
    elif isinstance(value, int):
        return str(value)
    elif isinstance(value, float):
        if (value == float('+inf')):
            return "'+Inf'"
        elif (value == float('-inf')):
            return "'-Inf'"
        elif (math.isnan(value)):
            return "'NaN'"
        else:
            return "{:f}".format(value)
    elif isinstance(value, decimal.Decimal):
        return "{:f}".format(value)

    elif value is None:
        return 'NULL'
    elif isinstance(value, datetime.datetime) and value.tzinfo is None:
        return "TIMESTAMP '{}'".format(str(value))
    elif isinstance(value, datetime.datetime):
        return "TIMESTAMP '{}'".format(str(value))
    elif isinstance(value, datetime.date):
        return "Date  '{}'".format(str(value))
    else:
        raise Exception("crud_up_value does not yet support {}".format(value.__class__))

def insert_record(cursor, table_name, data):
    our_keys = list(data.keys())
    our_keys.sort()
    our_values = list(map( lambda k: data.get(k), our_keys))

    columns_sql = ", \n".join(our_keys)
    value_sql   = ", \n".join(map(crud_up_value, our_values))
    insert_sql = """ INSERT INTO {}
                            ( {} )
                     VALUES ( {} )""".format(table_name, columns_sql, value_sql)
    #LOGGER.info("INSERT: {}".format(insert_sql))
    cursor.execute(insert_sql)

def crud_up_log_miner_fixtures(cursor, table_name, data, update_munger_fn):
    #initial insert
    insert_record(cursor, table_name, data)

    our_keys = list(data.keys())
    our_keys.sort()
    our_values = list(map( lambda k: data.get(k), our_keys))

    our_update_values = list(map(lambda v: crud_up_value(update_munger_fn(v)) , our_values))
    set_fragments =  ["{} = {}".format(i,j) for i, j in list(zip(our_keys, our_update_values))]
    set_clause = ", \n".join(set_fragments)


    #insert another row for fun
    insert_record(cursor, table_name, data)

    #update both rows
    update_sql = """UPDATE {}
                       SET {}""".format(table_name, set_clause)

    #now update
    #LOGGER.info("crud_up_log_miner_fixtures UPDATE: {}".format(update_sql))
    cursor.execute(update_sql)


    #delete both rows
    cursor.execute(""" DELETE FROM {}""".format(table_name))

    return True

def verify_crud_messages(that, caught_messages, pks):

    that.assertEqual(14, len(caught_messages))
    that.assertTrue(isinstance(caught_messages[0], singer.SchemaMessage))
    that.assertTrue(isinstance(caught_messages[1], singer.RecordMessage))
    that.assertTrue(isinstance(caught_messages[2], singer.StateMessage))
    that.assertTrue(isinstance(caught_messages[3], singer.RecordMessage))
    that.assertTrue(isinstance(caught_messages[4], singer.StateMessage))
    that.assertTrue(isinstance(caught_messages[5], singer.RecordMessage))
    that.assertTrue(isinstance(caught_messages[6], singer.StateMessage))
    that.assertTrue(isinstance(caught_messages[7], singer.RecordMessage))
    that.assertTrue(isinstance(caught_messages[8], singer.StateMessage))
    that.assertTrue(isinstance(caught_messages[9], singer.RecordMessage))
    that.assertTrue(isinstance(caught_messages[10], singer.StateMessage))
    that.assertTrue(isinstance(caught_messages[11], singer.RecordMessage))
    that.assertTrue(isinstance(caught_messages[12], singer.StateMessage))
    that.assertTrue(isinstance(caught_messages[13], singer.StateMessage))

    #schema includes scn && _sdc_deleted_at because we selected logminer as our replication method
    that.assertEqual({"type" : ['integer']}, caught_messages[0].schema.get('properties').get('scn') )
    that.assertEqual({"type" : ['null', 'string'], "format" : "date-time"}, caught_messages[0].schema.get('properties').get('_sdc_deleted_at') )

    that.assertEqual(pks, caught_messages[0].key_properties)

    #verify first STATE message
    bookmarks_1 = caught_messages[2].value.get('bookmarks')['ROOT-CHICKEN']
    that.assertIsNotNone(bookmarks_1)
    bookmarks_1_scn = bookmarks_1.get('scn')
    bookmarks_1_version = bookmarks_1.get('version')
    that.assertIsNotNone(bookmarks_1_scn)
    that.assertIsNotNone(bookmarks_1_version)

    #verify STATE message after UPDATE
    bookmarks_2 = caught_messages[6].value.get('bookmarks')['ROOT-CHICKEN']
    that.assertIsNotNone(bookmarks_2)
    bookmarks_2_scn = bookmarks_2.get('scn')
    bookmarks_2_version = bookmarks_2.get('version')
    that.assertIsNotNone(bookmarks_2_scn)
    that.assertIsNotNone(bookmarks_2_version)
    that.assertGreater(bookmarks_2_scn, bookmarks_1_scn)
    that.assertEqual(bookmarks_2_version, bookmarks_1_version)
