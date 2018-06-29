import datetime
import decimal
import json
import psycopg2
import psycopg2.extras
import singer

cursor_iter_size = 20000

#from the postgres docs:
#Quoted identifiers can contain any character, except the character with code zero. (To include a double #quote, write two double quotes.)
def canonicalize_identifier(identifier):
    return identifier.replace('"', '""')

def fully_qualified_column_name(schema, table, column):
    return '"{}"."{}"."{}"'.format(canonicalize_identifier(schema), canonicalize_identifier(table), canonicalize_identifier(column))

def fully_qualified_table_name(schema, table):
    return '"{}"."{}"'.format(canonicalize_identifier(schema), canonicalize_identifier(table))

def open_connection(conn_config, logical_replication=False):
    conn_string = "host='{}' dbname='{}' user='{}' password='{}' port='{}'".format(conn_config['host'],
                                                                                   conn_config['dbname'],
                                                                                   conn_config['user'],
                                                                                   conn_config['password'],
                                                                                   conn_config['port'])
    if logical_replication:
        conn = psycopg2.connect(conn_string, connection_factory=psycopg2.extras.LogicalReplicationConnection)
    else:
        conn = psycopg2.connect(conn_string)

    return conn

def prepare_columns_sql(c):
    column_name = """ "{}" """.format(canonicalize_identifier(c))
    return column_name

def filter_dbs_sql_clause(sql, filter_dbs):
    in_clause = " AND datname in (" + ",".join(["'{}'".format(b.strip(' ')) for b in filter_dbs.split(',')]) + ")"
    return sql + in_clause

#pylint: disable=too-many-branches,too-many-nested-blocks
def selected_value_to_singer_value_impl(elem, sql_datatype):
    sql_datatype = sql_datatype.replace('[]', '')
    if elem is None:
        cleaned_elem = elem
    elif sql_datatype == 'money':
        cleaned_elem = elem
    elif isinstance(elem, datetime.datetime):
        if sql_datatype == 'timestamp with time zone':
            cleaned_elem = elem.isoformat()
        else: #timestamp WITH OUT time zone
            cleaned_elem = elem.isoformat() + '+00:00'
    elif isinstance(elem, datetime.date):
        cleaned_elem = elem.isoformat() + 'T00:00:00+00:00'
    elif sql_datatype == 'bit':
        cleaned_elem = elem == '1'
    elif sql_datatype == 'boolean':
        cleaned_elem = elem
    elif isinstance(elem, int):
        cleaned_elem = elem
    elif isinstance(elem, datetime.time):
        cleaned_elem = str(elem)
    elif isinstance(elem, str):
        cleaned_elem = elem
    elif isinstance(elem, decimal.Decimal):
        cleaned_elem = elem
    elif isinstance(elem, float):
        cleaned_elem = elem
    elif isinstance(elem, dict):
        if sql_datatype == 'hstore':
            cleaned_elem = elem
        elif sql_datatype in {'json', 'jsonb'}:
            cleaned_elem = json.dumps(elem)
        else:
            raise Exception("do not know how to marshall a dict if its not an hstore or json: {}".format(sql_datatype))
    elif isinstance(elem, list) and sql_datatype in {'json', 'jsonb'}:
        cleaned_elem = json.dumps(elem)
    else:
        raise Exception("do not know how to marshall value of class( {} ) and sql_datatype ( {} )".format(elem.__class__, sql_datatype))

    return cleaned_elem

def selected_array_to_singer_value(elem, sql_datatype):
    if isinstance(elem, list):
        return list(map(lambda elem: selected_array_to_singer_value(elem, sql_datatype), elem))

    return selected_value_to_singer_value_impl(elem, sql_datatype)

def selected_value_to_singer_value(elem, sql_datatype):
    #are we dealing with an array?
    if sql_datatype.find('[]') > 0:
        return list(map(lambda elem: selected_array_to_singer_value(elem, sql_datatype), (elem or [])))

    return selected_value_to_singer_value_impl(elem, sql_datatype)

#pylint: disable=too-many-arguments
def selected_row_to_singer_message(stream, row, version, columns, time_extracted, md_map):
    row_to_persist = ()
    for idx, elem in enumerate(row):
        sql_datatype = md_map.get(('properties', columns[idx]))['sql-datatype']
        cleaned_elem = selected_value_to_singer_value(elem, sql_datatype)
        row_to_persist += (cleaned_elem,)

    rec = dict(zip(columns, row_to_persist))

    return singer.RecordMessage(
        stream=stream.stream,
        record=rec,
        version=version,
        time_extracted=time_extracted)

def hstore_available(conn_info):
    with open_connection(conn_info) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor, name='stitch_cursor') as cur:
            cur.execute(""" SELECT installed_version FROM pg_available_extensions WHERE name = 'hstore' """)
            return cur.fetchone()[0] is not None


def compute_tap_stream_id(database_name, schema_name, table_name):
    return database_name + '-' + schema_name + '-' + table_name
