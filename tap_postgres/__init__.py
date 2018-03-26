#!/usr/bin/env python3
# pylint: disable=missing-docstring,not-an-iterable,too-many-locals,too-many-arguments,invalid-name

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
import singer
import singer.metrics as metrics
import singer.schema
from singer import utils, metadata, get_bookmark
from singer.schema import Schema
from singer.catalog import Catalog, CatalogEntry
import psycopg2
import psycopg2.extras
import tap_postgres.sync_strategies.logical_replication as logical_replication
import tap_postgres.sync_strategies.full_table as full_table
import tap_postgres.db as post_db
LOGGER = singer.get_logger()

#LogMiner do not support LONG, LONG RAW, CLOB, BLOB, NCLOB, ADT, or COLLECTION datatypes.
Column = collections.namedtuple('Column', [
   "column_name",
   "is_primary_key",
   "sql_data_type",
   "character_maximum_length",
   "numeric_precision",
   "numeric_scale"
])


REQUIRED_CONFIG_KEYS = [
    'database',
    'host',
    'port',
    'user',
    'password',
    'default_replication_method'
]


INTEGER_TYPES= {'integer', 'smallint', 'bigint'}

def open_connection(config, logical_replication=False):
    conn_string = "host='{}' dbname='{}' user='{}' password='{}' port='{}'".format(config['host'],
                                                                                   config['database'],
                                                                                   config['user'],
                                                                                   config['password'],
                                                                                   config['port'])
    if logical_replication:
        conn = psycopg2.connect(conn_string, connection_factory=psycopg2.extras.LogicalReplicationConnection)
    else:
        conn = psycopg2.connect(conn_string)

    return conn

DEFAULT_NUMERIC_PRECISION=38
DEFAULT_NUMERIC_SCALE=0

def nullable_column(col_name, col_type, pk):
   if pk:
      return  [col_type]
   else:
      return ['null', col_type]

def schema_for_column(c):
   data_type = c.sql_data_type.lower()
   result = Schema()

   numeric_scale = c.numeric_scale or DEFAULT_NUMERIC_SCALE
   numeric_precision = c.numeric_precision or DEFAULT_NUMERIC_PRECISION

   if data_type in INTEGER_TYPES:
      result.type = nullable_column(c.column_name, 'integer', c.is_primary_key)
      result.minimum = -1 * (2**(c.numeric_precision - 1))
      result.maximum = 2**(c.numeric_precision - 1) - 1

      return result

   elif data_type == 'numeric':
      result.type = nullable_column(c.column_name, 'number', c.is_primary_key)

      result.exclusiveMaximum = True
      result.maximum = 10 ** (c.numeric_precision - c.numeric_scale)
      result.multipleOf = 10 ** (0 - c.numeric_scale)
      result.exclusiveMinimum = True
      result.minimum = -10 ** (c.numeric_precision - c.numeric_scale)
      return result

   # elif data_type == 'date' or data_type.startswith("timestamp"):
   #    result.type = nullable_column(c.column_name, 'string', is_primary_key)

   #    result.format = 'date-time'
   #    return result

   # elif data_type in FLOAT_TYPES:
   #    result.type = nullable_column(c.column_name, 'number', is_primary_key)
   #    return result

   elif data_type == 'text':
      result.type = nullable_column(c.column_name, 'string', c.is_primary_key)

      return result

   elif data_type == 'character varying':
       result.type = nullable_column(c.column_name, 'string', c.is_primary_key)
       result.maxLength = c.character_maximum_length
       return result


   return Schema(None)

def produce_table_info(conn):
   with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
      table_info = {}
      cur.execute("""
SELECT
  pg_class.reltuples::BIGINT AS approximate_row_count,
  pg_class.relkind = 'v' AS is_view,
  n.nspname as schema_name,
  pg_class.relname as table_name,
  attname as column_name,
  i.indisprimary AS primary_key,
  format_type(a.atttypid, NULL::integer) as data_type,
  information_schema._pg_char_max_length(information_schema._pg_truetypid(a.*, t.*),
                                         information_schema._pg_truetypmod(a.*, t.*))::information_schema.cardinal_number AS character_maximum_length,
  information_schema._pg_numeric_precision(information_schema._pg_truetypid(a.*, t.*),
                                           information_schema._pg_truetypmod(a.*, t.*))::information_schema.cardinal_number AS numeric_precision,
 information_schema._pg_numeric_scale(information_schema._pg_truetypid(a.*, t.*),
                                      information_schema._pg_truetypmod(a.*, t.*))::information_schema.cardinal_number AS numeric_scale
FROM   pg_attribute a
LEFT JOIN pg_type t on a.atttypid = t.oid
JOIN pg_class
  ON pg_class.oid = a.attrelid
JOIN pg_catalog.pg_namespace n
  ON n.oid = pg_class.relnamespace
left outer join  pg_index as i
  on a.attrelid = i.indrelid
 and a.attnum = ANY(i.indkey)
WHERE attnum > 0
AND NOT a.attisdropped
AND pg_class.relkind IN ('r', 'v')
AND n.nspname NOT in ('pg_toast', 'pg_catalog', 'information_schema')
AND has_table_privilege(pg_class.oid, 'SELECT') = true """)
      for row in cur.fetchall():
         row_count, is_view, schema_name, table_name, *col_info = row

         if table_info.get(schema_name) is None:
            table_info[schema_name] = {}

         if table_info[schema_name].get(table_name) is None:
            table_info[schema_name][table_name] = {'is_view': is_view, 'row_count' : row_count, 'columns' : {}}

         col_name  = col_info[0]
         table_info[schema_name][table_name]['columns'][col_name] = Column(*col_info)

      return table_info

def get_database_name(connection):
   cur = connection.cursor()

   rows = cur.execute("SELECT name FROM v$database").fetchall()
   return rows[0][0]

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

         metadata.write(mdata, (), 'key-properties', table_pks)
         metadata.write(mdata, (), 'schema-name', schema_name)
         metadata.write(mdata, (), 'database-name', database_name)
         metadata.write(mdata, (), 'row-count', table_info[schema_name][table_name]['row_count'])
         metadata.write(mdata, (), 'is-view', table_info[schema_name][table_name].get('is_view'))

         # if table_name == 'CHICKEN TIMES':
         #     pdb.set_trace()
         column_schemas = {col_name : schema_for_column(col_info) for col_name, col_info in columns.items()}
         schema = Schema(type='object', properties=column_schemas)
         for c_name in column_schemas.keys():

             mdata = metadata.write(mdata, ('properties', c_name), 'sql-datatype', columns[c_name].sql_data_type)
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
            tap_stream_id=schema_name + '-' + table_name,
            schema=schema)

         entries.append(entry)

   return Catalog(entries)

def dump_catalog(catalog):
   catalog.dump()

def do_discovery(connection):
   table_info = produce_table_info(connection)
   catalog = discover_columns(connection, table_info)
   dump_catalog(catalog)
   return catalog

def should_sync_column(metadata, field_name):
   if metadata.get(('properties', field_name), {}).get('inclusion') == 'unsupported':
      return False

   if metadata.get(('properties', field_name), {}).get('selected'):
      return True

   if metadata.get(('properties', field_name), {}).get('inclusion') == 'automatic':
      return True

   return False

def send_schema_message(stream, bookmark_properties):
   schema_message = singer.SchemaMessage(stream=stream.stream,
                                         schema=stream.schema.to_dict(),
                                         key_properties=metadata.to_map(stream.metadata).get((), {}).get('key-properties'),
                                         bookmark_properties=bookmark_properties)
   singer.write_message(schema_message)

def is_selected_via_metadata(stream):
   table_md = metadata.to_map(stream.metadata).get((), {})
   return table_md.get('selected')

def do_sync(connection, catalog, default_replication_method, state):
   streams = list(filter(lambda stream: is_selected_via_metadata(stream), catalog.streams))
   streams.sort(key=lambda s: s.tap_stream_id)

   currently_syncing = singer.get_currently_syncing(state)

   if currently_syncing:
      streams = dropwhile(lambda s: s.tap_stream_id != currently_syncing, streams)

   for stream in streams:
      state = singer.set_currently_syncing(state, stream.tap_stream_id)
      stream_metadata = metadata.to_map(stream.metadata)

      desired_columns =  [c for c in stream.schema.properties.keys() if should_sync_column(stream_metadata, c)]
      desired_columns.sort()

      replication_method = stream_metadata.get((), {}).get('replication-method', default_replication_method)
      if replication_method == 'LOG_BASED':
         if get_bookmark(state, stream.tap_stream_id, 'lsn'):
            logical_replication.add_automatic_properties(stream)
            send_schema_message(stream, ['lsn'])
            args = utils.parse_args(REQUIRED_CONFIG_KEYS)
            rep_connection = open_connection(args.config, True)
            state = logical_replication.sync_table(rep_connection, connection, stream, state, desired_columns)
         else:
            #start off with full-table replication
            end_lsn = logical_replication.fetch_current_lsn(connection)
            # LOGGER.info('RING3: lsn %s', end_lsn)
            send_schema_message(stream, [])
            state = full_table.sync_table(connection, stream, state, desired_columns)
            state = singer.write_bookmark(state,
                                          stream.tap_stream_id,
                                          'xmin',
                                          None)
            #once we are done with full table, write the lsn to the state
            state = singer.write_bookmark(state, stream.tap_stream_id, 'lsn', end_lsn)

      elif replication_method == 'FULL_TABLE':
         send_schema_message(stream, [])
         state = full_table.sync_table(connection, stream, state, desired_columns)
      else:
         raise Exception("only LOG_BASED and FULL_TABLE are supported right now :)")

      state = singer.set_currently_syncing(state, None)
      singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))


def main_impl():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    connection = open_connection(args.config)

    if args.discover:
        do_discovery(connection)
    elif args.catalog:
       state = args.state
       do_sync(connection, args.catalog, args.config.get('default_replication_method'), state)
    else:
        LOGGER.info("No properties were selected")

def main():
    try:
        main_impl()
    except Exception as exc:
        LOGGER.critical(exc)
        raise exc
