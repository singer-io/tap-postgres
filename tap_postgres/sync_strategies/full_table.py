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
import psycopg2.extras
import tap_postgres.db as post_db

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
   column_name = """ "{}" """.format(post_db.canonicalize_identifier(c))

   return column_name

def sync_table(connection, stream, state, desired_columns):
   stream_metadata = metadata.to_map(stream.metadata)

   # cur = connection.cursor()
   # cur.execute("""ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD"T00:00:00.00+00:00"'""")
   # cur.execute("""ALTER SESSION SET NLS_TIMESTAMP_FORMAT='YYYY-MM-DD"T"HH24:MI:SSXFF"+00:00"'""")
   # cur.execute("""ALTER SESSION SET NLS_TIMESTAMP_TZ_FORMAT  = 'YYYY-MM-DD"T"HH24:MI:SS.FFTZH:TZM'""")
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

   md = metadata.to_map(stream.metadata)
   schema_name = md.get(()).get('schema-name')

   escaped_columns = map(lambda c: prepare_columns_sql(stream, c), desired_columns)

   activate_version_message = singer.ActivateVersionMessage(
      stream=stream.stream,
      version=nascent_stream_version)

   if first_run:
      singer.write_message(activate_version_message)

   with metrics.record_counter(None) as counter:
      with connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:

         xmin = singer.get_bookmark(state, stream.tap_stream_id, 'xmin')
         if xmin:
            select_sql      = 'SELECT {}, xmin::text::bigint FROM {} where xmin::text::bigint > {} order by xmin::text::bigint'.format(','.join(escaped_columns),
                                                                                                                                       post_db.fully_qualified_table_name(schema_name, stream.table),
                                                                                                                                       xmin)
            cur.execute(select_sql)
         else:
            select_sql      = 'SELECT {}, xmin::text::bigint FROM {} order by xmin::text::bigint'.format(','.join(escaped_columns),
                                                                                                         post_db.fully_qualified_table_name(schema_name, stream.table))
            cur.execute(select_sql)



         rows_saved = 0
         rec = cur.fetchone()
         while rec is not None:
            xmin = rec['xmin']
            rec = rec[:-1]
            record_message = row_to_singer_message(stream, rec, nascent_stream_version, desired_columns, time_extracted)
            singer.write_message(record_message)
            state = singer.write_bookmark(state,
                                          stream.tap_stream_id,
                                          'xmin',
                                          xmin)
            rows_saved = rows_saved + 1
            if rows_saved % UPDATE_BOOKMARK_PERIOD == 0:
               singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

            counter.increment()
            rec = cur.fetchone()


   singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))
   #always send the activate version whether first run or subsequent
   singer.write_message(activate_version_message)


   return state
