#!/usr/bin/env python3
# pylint: disable=missing-docstring,not-an-iterable,too-many-locals,too-many-arguments,invalid-name,too-many-return-statements,too-many-branches,len-as-condition,too-many-statements

import datetime
import pdb
import json
import os
import sys
import time
import collections
import itertools
from itertools import dropwhile
import copy
import ssl
import psycopg2
import psycopg2.extras
import singer
import singer.metrics as metrics
import singer.schema
from singer import utils, metadata, get_bookmark
from singer.schema import Schema
from singer.catalog import Catalog, CatalogEntry

import tap_postgres.sync_strategies.logical_replication as logical_replication
import tap_postgres.sync_strategies.full_table as full_table
import tap_postgres.sync_strategies.incremental as incremental
import tap_postgres.db as post_db
import tap_postgres.sync_strategies.common as sync_common
LOGGER = singer.get_logger()

#LogMiner do not support LONG, LONG RAW, CLOB, BLOB, NCLOB, ADT, or COLLECTION datatypes.
Column = collections.namedtuple('Column', [
    "column_name",
    "is_primary_key",
    "sql_data_type",
    "character_maximum_length",
    "numeric_precision",
    "numeric_scale",
    "array_dimensions",
    "is_enum"

])


REQUIRED_CONFIG_KEYS = [
    'dbname',
    'host',
    'port',
    'user',
    'password'
]


INTEGER_TYPES = {'integer', 'smallint', 'bigint'}
FLOAT_TYPES = {'real', 'double precision'}
JSON_TYPES = {'json', 'jsonb'}

#NB> numeric/decimal columns in postgres without a specified scale && precision
#default to 'up to 131072 digits before the decimal point; up to 16383
#digits after the decimal point'. For practical reasons, we are capping this at 74/38
#  https://www.postgresql.org/docs/10/static/datatype-numeric.html#DATATYPE-NUMERIC-TABLE
MAX_SCALE = 38
MAX_PRECISION = 100

def nullable_column(col_type, pk):
    if pk:
        return  [col_type]
    return ['null', col_type]


def schema_for_column_datatype(c):
    schema = {}
    #remove any array notation from type information as we use a separate field for that
    data_type = c.sql_data_type.lower().replace('[]', '')

    if data_type in INTEGER_TYPES:
        schema['type'] = nullable_column('integer', c.is_primary_key)
        schema['minimum'] = -1 * (2**(c.numeric_precision - 1))
        schema['maximum'] = 2**(c.numeric_precision - 1) - 1
        return schema
    elif data_type == 'money':
        schema['type'] = nullable_column('string', c.is_primary_key)
        return schema
    elif c.is_enum:
        schema['type'] = nullable_column('string', c.is_primary_key)
        return schema

    elif data_type == 'bit' and c.character_maximum_length == 1:
        schema['type'] = nullable_column('boolean', c.is_primary_key)
        return schema

    elif data_type == 'boolean':
        schema['type'] = nullable_column('boolean', c.is_primary_key)
        return schema

    elif data_type == 'uuid':
        schema['type'] = nullable_column('string', c.is_primary_key)
        return schema

    elif data_type == 'hstore':
        schema['type'] = nullable_column('object', c.is_primary_key)
        schema['properties'] = {}
        return schema

    elif data_type == 'citext':
        schema['type'] = nullable_column('string', c.is_primary_key)
        return schema

    elif data_type in JSON_TYPES:
        schema['type'] = nullable_column('string', c.is_primary_key)
        return schema

    elif data_type == 'numeric':
        schema['type'] = nullable_column('number', c.is_primary_key)
        if c.numeric_scale is None or c.numeric_scale > MAX_SCALE:
            LOGGER.warning('capping decimal scale to 38.  THIS MAY CAUSE TRUNCATION')
            scale = MAX_SCALE
        else:
            scale = c.numeric_scale

        if c.numeric_precision is None or c.numeric_precision > MAX_PRECISION:
            LOGGER.warning('capping decimal precision to 100.  THIS MAY CAUSE TRUNCATION')
            precision = MAX_PRECISION
        else:
            precision = c.numeric_precision

        schema['exclusiveMaximum'] = True
        schema['maximum'] = 10 ** (precision - scale)
        schema['multipleOf'] = 10 ** (0 - scale)
        schema['exclusiveMinimum'] = True
        schema['minimum'] = -10 ** (precision - scale)
        return schema

    elif data_type in {'time without time zone', 'time with time zone'}:
        #times are treated as ordinary strings as they can not possible match RFC3339
        schema['type'] = nullable_column('string', c.is_primary_key)
        return schema

    elif data_type in ('date', 'timestamp without time zone', 'timestamp with time zone'):
        schema['type'] = nullable_column('string', c.is_primary_key)

        schema['format'] = 'date-time'
        return schema

    elif data_type in FLOAT_TYPES:
        schema['type'] = nullable_column('number', c.is_primary_key)
        return schema

    elif data_type == 'text':
        schema['type'] = nullable_column('string', c.is_primary_key)
        return schema

    elif data_type == 'character varying':
        schema['type'] = nullable_column('string', c.is_primary_key)
        schema['maxLength'] = c.character_maximum_length
        return schema

    elif data_type == 'character':
        schema['type'] = nullable_column('string', c.is_primary_key)
        schema['maxLength'] = c.character_maximum_length
        return schema

    elif data_type in {'cidr', 'inet', 'macaddr'}:
        schema['type'] = nullable_column('string', c.is_primary_key)
        return schema

    return schema

def schema_for_column(c):
    column_schema = {}

    #NB> from the post postgres docs: The current implementation does not enforce the declared number of dimensions either.
    #these means we can say nothing about an array column. its items may be more arrays or primitive types like integers
    #and this can vary on a row by row basis
    if c.array_dimensions > 0:
        column_schema["type"] = ["null", "array"]
        column_schema["items"] = {}
        return Schema.from_dict(column_schema)

    return Schema.from_dict(schema_for_column_datatype(c))

#typlen  -1  == variable length arrays
#typelem != 0 points to subtypes. 23 in the case of arrays
# so integer arrays are typlen = -1, typelem = 23 because integer types are oid 23

#this seems to identify all arrays:
#select typname from pg_attribute  as pga join pg_type as pgt on pgt.oid = pga.atttypid  where typlen = -1 and typelem != 0 and pga.attndims > 0;
def produce_table_info(conn):
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor, name='stitch_cursor') as cur:
        cur.itersize = post_db.cursor_iter_size
        table_info = {}
          # SELECT CASE WHEN $2.typtype = 'd' THEN $2.typbasetype ELSE $1.atttypid END
        cur.execute("""
SELECT
  pg_class.reltuples::BIGINT             AS approximate_row_count,
  pg_class.relkind = 'v'                 AS is_view,
  n.nspname                              AS schema_name,
  pg_class.relname                       AS table_name,
  attname                                AS column_name,
  i.indisprimary                         AS primary_key,

  format_type(a.atttypid, NULL::integer) AS data_type,
  information_schema._pg_char_max_length(CASE WHEN COALESCE(subpgt.typtype, pgt.typtype) = 'd'
                                              THEN COALESCE(subpgt.typbasetype, pgt.typbasetype) ELSE COALESCE(subpgt.oid, pgt.oid)
                                          END,
                                          information_schema._pg_truetypmod(a.*, pgt.*))::information_schema.cardinal_number AS character_maximum_length,
  information_schema._pg_numeric_precision(CASE WHEN COALESCE(subpgt.typtype, pgt.typtype) = 'd'
                                                THEN COALESCE(subpgt.typbasetype, pgt.typbasetype) ELSE COALESCE(subpgt.oid, pgt.oid)
                                            END,
                                           information_schema._pg_truetypmod(a.*, pgt.*))::information_schema.cardinal_number AS numeric_precision,
  information_schema._pg_numeric_scale(CASE WHEN COALESCE(subpgt.typtype, pgt.typtype) = 'd'
                                                THEN COALESCE(subpgt.typbasetype, pgt.typbasetype) ELSE COALESCE(subpgt.oid, pgt.oid)
                                        END,
                                       information_schema._pg_truetypmod(a.*, pgt.*))::information_schema.cardinal_number AS numeric_scale,
  a.attndims                           AS array_dimensions,
  COALESCE(subpgt.typtype, pgt.typtype) = 'e' AS is_enum
FROM pg_attribute a
LEFT JOIN pg_type AS pgt ON a.atttypid = pgt.oid
JOIN pg_class
  ON pg_class.oid = a.attrelid
JOIN pg_catalog.pg_namespace n
  ON n.oid = pg_class.relnamespace
LEFT OUTER JOIN pg_index as i
  ON a.attrelid = i.indrelid
 AND a.attnum = ANY(i.indkey)
LEFT OUTER JOIN pg_type AS subpgt
  ON pgt.typelem = subpgt.oid
 AND pgt.typelem != 0
WHERE attnum > 0
AND NOT a.attisdropped
AND pg_class.relkind IN ('r', 'v', 'm')
AND n.nspname NOT in ('pg_toast', 'pg_catalog', 'information_schema')
AND has_table_privilege(pg_class.oid, 'SELECT') = true """)
        for row in cur.fetchall():
            row_count, is_view, schema_name, table_name, *col_info = row

            if table_info.get(schema_name) is None:
                table_info[schema_name] = {}

            if table_info[schema_name].get(table_name) is None:
                table_info[schema_name][table_name] = {'is_view': is_view, 'row_count' : row_count, 'columns' : {}}

            col_name = col_info[0]

            table_info[schema_name][table_name]['columns'][col_name] = Column(*col_info)

        return table_info

def get_database_name(connection):
    cur = connection.cursor()

    rows = cur.execute("SELECT name FROM v$database").fetchall()
    return rows[0][0]

def write_sql_data_type_md(mdata, col_info):
    c_name = col_info.column_name
    if col_info.sql_data_type == 'bit' and col_info.character_maximum_length > 1:
        mdata = metadata.write(mdata, ('properties', c_name), 'sql-datatype', "bit({})".format(col_info.character_maximum_length))
    else:
        mdata = metadata.write(mdata, ('properties', c_name), 'sql-datatype', col_info.sql_data_type)

    return mdata

def discover_columns(connection, table_info):
    entries = []
    for schema_name in table_info.keys():
        for table_name in table_info[schema_name].keys():
            mdata = {}
            columns = table_info[schema_name][table_name]['columns']
            table_pks = [col_name for col_name, col_info in columns.items() if col_info.is_primary_key]
            with connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute(" SELECT current_database()")
                database_name = cur.fetchone()[0]

            metadata.write(mdata, (), 'table-key-properties', table_pks)
            metadata.write(mdata, (), 'schema-name', schema_name)
            metadata.write(mdata, (), 'database-name', database_name)
            metadata.write(mdata, (), 'row-count', table_info[schema_name][table_name]['row_count'])
            metadata.write(mdata, (), 'is-view', table_info[schema_name][table_name].get('is_view'))

            column_schemas = {col_name : schema_for_column(col_info) for col_name, col_info in columns.items()}

            schema = Schema(type='object', properties=column_schemas)
            for c_name in column_schemas.keys():
                mdata = write_sql_data_type_md(mdata, columns[c_name])
                if column_schemas[c_name].type is None:
                    mdata = metadata.write(mdata, ('properties', c_name), 'inclusion', 'unsupported')
                    mdata = metadata.write(mdata, ('properties', c_name), 'selected-by-default', False)
                elif table_info[schema_name][table_name]['columns'][c_name].is_primary_key:
                    mdata = metadata.write(mdata, ('properties', c_name), 'inclusion', 'automatic')
                    mdata = metadata.write(mdata, ('properties', c_name), 'selected-by-default', True)
                else:
                    mdata = metadata.write(mdata, ('properties', c_name), 'inclusion', 'available')
                    mdata = metadata.write(mdata, ('properties', c_name), 'selected-by-default', True)

            entry = CatalogEntry(
                table=table_name,
                stream=table_name,
                metadata=metadata.to_list(mdata),
                tap_stream_id=post_db.compute_tap_stream_id(database_name, schema_name, table_name),
                schema=schema)

            entries.append(entry)

    return entries

def dump_catalog(catalog):
    catalog.dump()

def discover_db(connection):
    table_info = produce_table_info(connection)
    db_streams = discover_columns(connection, table_info)
    return db_streams

def do_discovery(conn_config):
    all_streams = []

    with post_db.open_connection(conn_config) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor, name='stitch_cursor') as cur:
            cur.itersize = post_db.cursor_iter_size
            sql = """SELECT datname
            FROM pg_database
            WHERE datistemplate = false
                AND CASE WHEN version() LIKE '%Redshift%' THEN true
                        ELSE has_database_privilege(datname,'CONNECT')
                    END = true """

            if conn_config.get('filter_dbs'):
                sql = post_db.filter_dbs_sql_clause(sql, conn_config['filter_dbs'])


            LOGGER.info("Running DB discovery: %s", sql)

            cur.execute(sql)
            filter_dbs = (row[0] for row in cur.fetchall())

    for db_row in filter_dbs:
        dbname = db_row
        LOGGER.info("Discovering db %s", dbname)
        conn_config['dbname'] = dbname
        with post_db.open_connection(conn_config) as conn:
            db_streams = discover_db(conn)
            all_streams = all_streams + db_streams

    cluster_catalog = Catalog(all_streams)
    dump_catalog(cluster_catalog)
    return cluster_catalog

def should_sync_column(md_map, field_name):
    #always sync replication_keys
    if md_map.get((), {}).get('replication-key') == field_name:
        return True

    if md_map.get(('properties', field_name), {}).get('inclusion') == 'unsupported':
        return False

    if md_map.get(('properties', field_name), {}).get('selected'):
        return True

    if md_map.get(('properties', field_name), {}).get('inclusion') == 'automatic':
        return True

    return False

def is_selected_via_metadata(stream):
    table_md = metadata.to_map(stream.metadata).get((), {})
    return table_md.get('selected')

def do_sync_full_table(conn_config, stream, state, desired_columns, md_map):
    LOGGER.info("Stream %s is using full_table replication", stream.tap_stream_id)
    sync_common.send_schema_message(stream, [])
    if md_map.get((), {}).get('is-view'):
        state = full_table.sync_view(conn_config, stream, state, desired_columns, md_map)
    else:
        state = full_table.sync_table(conn_config, stream, state, desired_columns, md_map)
    return state

#Possible state keys: replication_key, replication_key_value, version
def do_sync_incremental(conn_config, stream, state, desired_columns, md_map):
    replication_key = md_map.get((), {}).get('replication-key')
    LOGGER.info("Stream %s is using incremental replication with replication key %s", stream.tap_stream_id, replication_key)

    stream_state = state.get('bookmarks', {}).get(stream.tap_stream_id)
    illegal_bk_keys = set(stream_state.keys()).difference(set(['replication_key', 'replication_key_value', 'version', 'last_replication_method']))
    if len(illegal_bk_keys) != 0:
        raise Exception("invalid keys found in state: {}".format(illegal_bk_keys))

    state = singer.write_bookmark(state, stream.tap_stream_id, 'replication_key', replication_key)
    if md_map.get((), {}).get('is-view'):
        pks = md_map.get(()).get('table-key-properties')
    else:
        pks = md_map.get(()).get('table-key-properties')

    sync_common.send_schema_message(stream, pks)
    state = incremental.sync_table(conn_config, stream, state, desired_columns, md_map)

    return state

def clear_state_on_replication_change(state, tap_stream_id, replication_key, replication_method):
    #user changed replication, nuke state
    last_replication_method = singer.get_bookmark(state, tap_stream_id, 'last_replication_method')
    if last_replication_method is not None and (replication_method != last_replication_method):
        state = singer.reset_stream(state, tap_stream_id)

    #key changed
    if replication_method == 'INCREMENTAL':
        if replication_key != singer.get_bookmark(state, tap_stream_id, 'replication_key'):
            state = singer.reset_stream(state, tap_stream_id)

    state = singer.write_bookmark(state, tap_stream_id, 'last_replication_method', replication_method)
    return state

def sync_method_for_streams(streams, state, default_replication_method):
    lookup = {}
    traditional_steams = []
    logical_streams = []

    for stream in streams:
        stream_metadata = metadata.to_map(stream.metadata)
        replication_method = stream_metadata.get((), {}).get('replication-method', default_replication_method)
        replication_key = stream_metadata.get((), {}).get('replication-key')

        state = clear_state_on_replication_change(state, stream.tap_stream_id, replication_key, replication_method)

        if replication_method not in set(['LOG_BASED', 'FULL_TABLE', 'INCREMENTAL']):
            raise Exception("Unrecognized replication_method {}".format(replication_method))

        md_map = metadata.to_map(stream.metadata)
        desired_columns = [c for c in stream.schema.properties.keys() if should_sync_column(md_map, c)]
        desired_columns.sort()

        if len(desired_columns) == 0:
            LOGGER.warning('There are no columns selected for stream %s, skipping it', stream.tap_stream_id)
            continue

        if replication_method == 'LOG_BASED' and stream_metadata.get((), {}).get('is-view'):
            raise Exception('Logical Replication is NOT supported for views. Please change the replication method for {}'.format(stream.tap_stream_id))

        if replication_method == 'FULL_TABLE':
            lookup[stream.tap_stream_id] = 'full'
            traditional_steams.append(stream)
        elif replication_method == 'INCREMENTAL':
            lookup[stream.tap_stream_id] = 'incremental'
            traditional_steams.append(stream)

        elif get_bookmark(state, stream.tap_stream_id, 'xmin') and get_bookmark(state, stream.tap_stream_id, 'lsn'):
            #finishing previously interrupted full-table (first stage of logical replication)
            lookup[stream.tap_stream_id] = 'logical_initial_interrupted'
            traditional_steams.append(stream)

        #inconsistent state
        elif get_bookmark(state, stream.tap_stream_id, 'xmin') and not get_bookmark(state, stream.tap_stream_id, 'lsn'):
            raise Exception("Xmin found(%s) in state implying full-table replication but no lsn is present")

        elif not get_bookmark(state, stream.tap_stream_id, 'xmin') and not get_bookmark(state, stream.tap_stream_id, 'lsn'):
            #initial full-table phase of logical replication
            lookup[stream.tap_stream_id] = 'logical_initial'
            traditional_steams.append(stream)

        else: #no xmin but we have an lsn
            #initial stage of logical replication(full-table) has been completed. moving onto pure logical replication
            lookup[stream.tap_stream_id] = 'pure_logical'
            logical_streams.append(stream)

    return lookup, traditional_steams, logical_streams

def sync_traditional_stream(conn_config, stream, state, sync_method):
    LOGGER.info("Beginning sync of stream(%s) with sync method(%s)", stream.tap_stream_id, sync_method)
    md_map = metadata.to_map(stream.metadata)
    conn_config['dbname'] = md_map.get(()).get('database-name')
    desired_columns = [c for c in stream.schema.properties.keys() if should_sync_column(md_map, c)]
    desired_columns.sort()

    if len(desired_columns) == 0:
        LOGGER.warning('There are no columns selected for stream %s, skipping it', stream.tap_stream_id)
        return state

    if sync_method == 'full':
        state = singer.set_currently_syncing(state, stream.tap_stream_id)
        state = do_sync_full_table(conn_config, stream, state, desired_columns, md_map)
    elif sync_method == 'incremental':
        state = singer.set_currently_syncing(state, stream.tap_stream_id)
        state = do_sync_incremental(conn_config, stream, state, desired_columns, md_map)
    elif sync_method == 'logical_initial':
        state = singer.set_currently_syncing(state, stream.tap_stream_id)
        end_lsn = logical_replication.fetch_current_lsn(conn_config)
        LOGGER.info("Performing initial full table sync")
        state = singer.write_bookmark(state, stream.tap_stream_id, 'lsn', end_lsn)

        sync_common.send_schema_message(stream, [])
        state = full_table.sync_table(conn_config, stream, state, desired_columns, md_map)
        state = singer.write_bookmark(state, stream.tap_stream_id, 'xmin', None)
    elif sync_method == 'logical_initial_interrupted':
        state = singer.set_currently_syncing(state, stream.tap_stream_id)
        LOGGER.info("Initial stage of full table sync was interrupted. resuming...")
        sync_common.send_schema_message(stream, [])
        state = full_table.sync_table(conn_config, stream, state, desired_columns, md_map)
    else:
        raise Exception("unknown sync method {} for stream {}".format(sync_method, stream.tap_stream_id))

    state = singer.set_currently_syncing(state, None)
    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))
    return state

def sync_logical_streams(conn_config, logical_streams, state):
    if logical_streams:
        LOGGER.info("Pure Logical Replication upto lsn %s for (%s)", logical_replication.fetch_current_lsn(conn_config), list(map(lambda s: s.tap_stream_id, logical_streams)))
        logical_streams = list(map(logical_replication.add_automatic_properties, logical_streams))

        state = logical_replication.sync_tables(conn_config, logical_streams, state)

    return state

def do_sync(conn_config, catalog, default_replication_method, state):
    currently_syncing = singer.get_currently_syncing(state)
    streams = list(filter(is_selected_via_metadata, catalog.streams))
    streams.sort(key=lambda s: s.tap_stream_id)
    LOGGER.info("Selected streams: %s ", list(map(lambda s: s.tap_stream_id, streams)))

    sync_method_lookup, traditional_streams, logical_streams = sync_method_for_streams(streams, state, default_replication_method)
    #{"chickens" : "full_stream", "cows" : "logical_initial_interrupted_streams", "turkeys": "logical_replication"}
    #{"logical_streams" : ["turkeys"], "traditional_streams" : ["chickens", "cows"]}

    if currently_syncing:
        LOGGER.info("found currently_syncing: %s", currently_syncing)
        currently_syncing_stream = list(filter(lambda s: s.tap_stream_id == currently_syncing, traditional_streams))
        if currently_syncing_stream is None:
            LOGGER.warning("unable to locate currently_syncing(%s) amongst selected traditional streams(%s). will ignore", currently_syncing, list(map(lambda s: s.tap_stream_id, traditional_streams)))
        other_streams = list(filter(lambda s: s.tap_stream_id != currently_syncing, traditional_streams))
        traditional_streams = currently_syncing_stream + other_streams
    else:
        LOGGER.info("No currently_syncing found")

    for stream in traditional_streams:
        state = sync_traditional_stream(conn_config, stream, state, sync_method_lookup[stream.tap_stream_id])

    logical_streams.sort(key=lambda s: metadata.to_map(s.metadata).get(()).get('database-name'))
    for dbname, streams in itertools.groupby(logical_streams, lambda s: metadata.to_map(s.metadata).get(()).get('database-name')):
        conn_config['dbname'] = dbname
        state = sync_logical_streams(conn_config, list(streams), state)
    return state

def main_impl():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    conn_config = {'host'     : args.config['host'],
                   'user'     : args.config['user'],
                   'password' : args.config['password'],
                   'port'     : args.config['port'],
                   'dbname'   : args.config['dbname'],
                   'filter_dbs' : args.config.get('filter_dbs')}

    if args.config.get('ssl') == 'true':
        conn_config['sslmode'] = 'require'

    post_db.cursor_iter_size = int(args.config.get('itersize', '20000'))

    if args.discover:
        do_discovery(conn_config)
    elif args.catalog:
        state = args.state
        do_sync(conn_config, args.catalog, args.config.get('default_replication_method'), state)
    else:
        LOGGER.info("No properties were selected")

def main():
    try:
        main_impl()
    except Exception as exc:
        LOGGER.critical(exc)
        raise exc
