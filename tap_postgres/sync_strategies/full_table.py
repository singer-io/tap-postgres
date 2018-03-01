#!/usr/bin/env python3
import singer
from singer import utils, write_message, get_bookmark
import singer.metadata as metadata
from singer.schema import Schema
import singer.metrics as metrics
import copy
import pdb
import time
import decimal
import psycopg2

LOGGER = singer.get_logger()

UPDATE_BOOKMARK_PERIOD = 1000

def row_to_singer_message(stream, row, version, columns, time_extracted):
   row_to_persist = ()
   for idx, elem in enumerate(row):
      property_type = stream.schema.properties[columns[idx]].type
      if elem is None:
            row_to_persist += (elem,)
      elif 'integer' in property_type or property_type == 'integer':
         integer_representation = int(elem)
         row_to_persist += (integer_representation,)
      else:
         row_to_persist += (elem,)

   rec = dict(zip(columns, row_to_persist))

   return singer.RecordMessage(
        stream=stream.stream,
        record=rec,
        version=version,
        time_extracted=time_extracted)

def OutputTypeHandler(cursor, name, defaultType, size, precision, scale):
    if defaultType == cx_Oracle.NUMBER:
        return cursor.var(decimal.Decimal, arraysize = cursor.arraysize)


def prepare_columns_sql(stream, c):
   column_name = """ "{}" """.format(c)
   if 'string' in stream.schema.properties[c].type and stream.schema.properties[c].format == 'date-time':
      return "to_char({})".format(column_name)

   return column_name

def sync_table(connection, stream, state, desired_columns):
   connection.outputtypehandler = OutputTypeHandler

   stream_metadata = metadata.to_map(stream.metadata)

   cur = connection.cursor()
   cur.execute("""ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD"T00:00:00.00+00:00"'""")
   cur.execute("""ALTER SESSION SET NLS_TIMESTAMP_FORMAT='YYYY-MM-DD"T"HH24:MI:SSXFF"+00:00"'""")
   cur.execute("""ALTER SESSION SET NLS_TIMESTAMP_TZ_FORMAT  = 'YYYY-MM-DD"T"HH24:MI:SS.FFTZH:TZM'""")
   time_extracted = utils.now()

   #before writing the table version to state, check if we had one to begin with
   first_run = singer.get_bookmark(state, stream.tap_stream_id, 'version') is None

   #pick a new table version
   nascent_stream_version = int(time.time() * 1000)
   state = singer.write_bookmark(state,
                                 stream.tap_stream_id,
                                 'version',
                                 nascent_stream_version)
   singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

   cur = connection.cursor()
   md = metadata.to_map(stream.metadata)
   schema_name = md.get(()).get('schema-name')

   escaped_columns = map(lambda c: prepare_columns_sql(stream, c), desired_columns)
   escaped_schema  = schema_name
   escaped_table   = stream.table

   select_sql      = 'SELECT {} FROM {}.{}'.format(','.join(escaped_columns),
                                                   escaped_schema,
                                                   escaped_table)


   #LOGGER.info("select: %s", select_sql)
   activate_version_message = singer.ActivateVersionMessage(
      stream=stream.stream,
      version=nascent_stream_version)

   if first_run:
      singer.write_message(activate_version_message)

   with metrics.record_counter(None) as counter:
      for row in cur.execute(select_sql):
         record_message = row_to_singer_message(stream, row, nascent_stream_version, desired_columns, time_extracted)
         singer.write_message(record_message)
         counter.increment()

   #always send the activate version whether first run or subsequent
   singer.write_message(activate_version_message)

   return state
