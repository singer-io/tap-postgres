#!/usr/bin/env python3
# pylint: disable=missing-docstring,not-an-iterable,too-many-locals,too-many-arguments,invalid-name,too-many-return-statements,too-many-branches,len-as-condition,too-many-nested-blocks,wrong-import-order,duplicate-code

import singer
import datetime
from singer import utils, get_bookmark
import singer.metadata as metadata
from singer.schema import Schema
import tap_postgres.db as post_db
from dateutil.parser import parse
import psycopg2
import copy
from select import select
import json

LOGGER = singer.get_logger()

UPDATE_BOOKMARK_PERIOD = 1000

def fetch_current_lsn(conn_config):
    with post_db.open_connection(conn_config, False) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_current_xlog_location()")
            current_lsn = cur.fetchone()[0]
            file, index = current_lsn.split('/')
            return (int(file, 16)  << 32) + int(index, 16)

def add_automatic_properties(stream):
    stream.schema.properties['_sdc_deleted_at'] = Schema(
        type=['null', 'string'], format='date-time')

def get_stream_version(tap_stream_id, state):
    stream_version = singer.get_bookmark(state, tap_stream_id, 'version')

    if stream_version is None:
        raise Exception("version not found for log miner {}".format(tap_stream_id))

    return stream_version

def row_to_singer_message(stream, row, version, columns, time_extracted, md_map):
    row_to_persist = ()
    md_map[('properties', '_sdc_deleted_at')] = {'sql-datatype' : 'timestamp with time zone'}

    for idx, elem in enumerate(row):
        sql_datatype = md_map.get(('properties', columns[idx]))['sql-datatype']


        if elem is None:
            row_to_persist += (elem,)
        elif sql_datatype == 'timestamp without time zone':
            row_to_persist += (parse(elem).isoformat() + '+00:00',)
        elif sql_datatype == 'timestamp with time zone':
            row_to_persist += (parse(elem).isoformat(),)
        elif sql_datatype == 'date':
            row_to_persist += (parse(elem).isoformat() + "+00:00",)
        elif sql_datatype == 'time with time zone':
            row_to_persist += (parse(elem).isoformat().split('T')[1], )
        elif sql_datatype == 'bit':
            row_to_persist += (elem == '1',)
        elif sql_datatype == 'boolean':
            row_to_persist += (elem,)
        elif isinstance(elem, int):
            row_to_persist += (elem,)
        elif isinstance(elem, float):
            row_to_persist += (elem,)
        elif isinstance(elem, str):
            row_to_persist += (elem,)
        else:
            raise Exception("do not know how to marshall value of type {}".format(elem.__class__))

    rec = dict(zip(columns, row_to_persist))
    return singer.RecordMessage(
        stream=stream.stream,
        record=rec,
        version=version,
        time_extracted=time_extracted)

def consume_message(stream, state, msg, time_extracted, md_map):
    payload = json.loads(msg.payload)
    lsn = msg.data_start
    stream_version = get_stream_version(stream.tap_stream_id, state)

    for c in payload['change']:
        if c['schema'] != metadata.to_map(stream.metadata).get(())['schema-name'] or c['table'] != stream.table:
            continue

        if c['kind'] == 'insert':
            col_vals = c['columnvalues'] + [None]
            col_names = c['columnnames'] + ['_sdc_deleted_at']
            record_message = row_to_singer_message(stream, col_vals, stream_version, col_names, time_extracted, md_map)
        elif c['kind'] == 'update':
            col_vals = c['columnvalues'] + [None]
            col_names = c['columnnames'] + ['_sdc_deleted_at']
            record_message = row_to_singer_message(stream, col_vals, stream_version, col_names, time_extracted, md_map)
        elif c['kind'] == 'delete':
            col_names = c['oldkeys']['keynames'] + ['_sdc_deleted_at']
            col_vals = c['oldkeys']['keyvalues']  + [singer.utils.strftime(time_extracted)]
            record_message = row_to_singer_message(stream, col_vals, stream_version, col_names, time_extracted, md_map)
        else:
            raise Exception("unrecognized replication operation: {}".format(c['kind']))

        singer.write_message(record_message)


        state = singer.write_bookmark(state,
                                      stream.tap_stream_id,
                                      'lsn',
                                      lsn)
        #LOGGER.info("Flushing log up to LSN  %s", msg.data_start)
        msg.cursor.send_feedback(flush_lsn=msg.data_start)

    return state

# pylint: disable=unused-argument
def sync_table(conn_info, stream, state, desired_columns, md_map):
    start_lsn = get_bookmark(state, stream.tap_stream_id, 'lsn')
    end_lsn = fetch_current_lsn(conn_info)
    time_extracted = utils.now()

    with post_db.open_connection(conn_info, True) as conn:
        with conn.cursor() as cur:

            LOGGER.info("Starting Logical Replication: %s -> %s", start_lsn, end_lsn)
            try:
                cur.start_replication(slot_name='stitch', decode=True, start_lsn=start_lsn)
            except psycopg2.ProgrammingError:
                raise Exception("unable to start replication with logical replication slot 'stitch'")

            cur.send_feedback(flush_lsn=start_lsn)
            keepalive_interval = 10.0
            rows_saved = 0

            while True:
                msg = cur.read_message()
                if msg:
                    state = consume_message(stream, state, msg, time_extracted, md_map)
                    rows_saved = rows_saved + 1

                    if rows_saved % UPDATE_BOOKMARK_PERIOD == 0:
                        singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

                else:
                    now = datetime.datetime.now()
                    timeout = keepalive_interval - (now - cur.io_timestamp).total_seconds()
                    try:
                        sel = select([cur], [], [], max(0, timeout))
                        if not any(sel):
                            break
                    except InterruptedError:
                        pass  # recalculate timeout and continue

    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))
    return state
