#!/usr/bin/env python3
# pylint: disable=missing-docstring,not-an-iterable,too-many-locals,too-many-arguments,invalid-name,too-many-return-statements,too-many-branches,len-as-condition,too-many-nested-blocks,wrong-import-order,duplicate-code, anomalous-backslash-in-string

import singer
import datetime
from singer import utils, get_bookmark
import singer.metadata as metadata
from singer.schema import Schema
import tap_postgres.db as post_db
import tap_postgres.sync_strategies.common as sync_common
from dateutil.parser import parse
import psycopg2
import copy
from select import select
from functools import reduce
import json
import re

LOGGER = singer.get_logger()

UPDATE_BOOKMARK_PERIOD = 1000

def get_pg_version(cur):
    cur.execute("SELECT version()")
    res = cur.fetchone()[0]
    version_match = re.match('PostgreSQL (\d+)', res)
    if not version_match:
        raise Exception('unable to determine PostgreSQL version from {}'.format(res))

    version = int(version_match.group(1))
    LOGGER.info("Detected PostgresSQL version: %s", version)
    return version

def fetch_current_lsn(conn_config):
    with post_db.open_connection(conn_config, False) as conn:
        with conn.cursor() as cur:
            version = get_pg_version(cur)
            if version == 9:
                cur.execute("SELECT pg_current_xlog_location()")
            elif version > 9:
                cur.execute("SELECT pg_current_wal_lsn()")
            else:
                raise Exception('unable to fetch current lsn for PostgresQL version {}'.format(version))

            current_lsn = cur.fetchone()[0]
            file, index = current_lsn.split('/')
            return (int(file, 16)  << 32) + int(index, 16)

def add_automatic_properties(stream):
    stream.schema.properties['_sdc_deleted_at'] = Schema(
        type=['null', 'string'], format='date-time')

    return stream

def get_stream_version(tap_stream_id, state):
    stream_version = singer.get_bookmark(state, tap_stream_id, 'version')

    if stream_version is None:
        raise Exception("version not found for log miner {}".format(tap_stream_id))

    return stream_version

def tuples_to_map(accum, t):
    accum[t[0]] = t[1]
    return accum

def create_hstore_elem(conn_info, elem):
    with post_db.open_connection(conn_info) as conn:
        with conn.cursor() as cur:
            sql = """SELECT hstore_to_array('{}')""".format(elem)
            cur.execute(sql)
            res = cur.fetchone()[0]
            hstore_elem = reduce(tuples_to_map, [res[i:i + 2] for i in range(0, len(res), 2)], {})
            return hstore_elem

def create_array_elem(elem, sql_datatype, conn_info):
    if elem is None:
        return None

    with post_db.open_connection(conn_info) as conn:
        with conn.cursor() as cur:
            sql = """SELECT '{}'::{}""".format(elem, sql_datatype)
            cur.execute(sql)
            res = cur.fetchone()[0]
            return res

#pylint: disable=too-many-branches,too-many-nested-blocks
def selected_value_to_singer_value_impl(elem, og_sql_datatype, conn_info):
    sql_datatype = og_sql_datatype.replace('[]', '')
    if elem is None:
        return elem
    if sql_datatype == 'timestamp without time zone':
        return parse(elem).isoformat() + '+00:00'
    if sql_datatype == 'timestamp with time zone':
        if isinstance(elem, datetime.datetime):
            return elem.isoformat()

        return parse(elem).isoformat()
    if sql_datatype == 'date':
        if  isinstance(elem, datetime.date):
            #logical replication gives us dates as strings UNLESS they from an array
            return elem.isoformat() + 'T00:00:00+00:00'

        return parse(elem).isoformat() + "+00:00"
    if sql_datatype == 'time with time zone':
        return parse(elem).isoformat().split('T')[1]
    if sql_datatype == 'bit':
        return elem == '1'
    if sql_datatype == 'boolean':
        return elem
    if sql_datatype == 'hstore':
        return create_hstore_elem(conn_info, elem)
    if isinstance(elem, int):
        return elem
    if isinstance(elem, float):
        return elem
    if isinstance(elem, str):
        return elem

    raise Exception("do not know how to marshall value of type {}".format(elem.__class__))

def selected_array_to_singer_value(elem, sql_datatype, conn_info):
    if isinstance(elem, list):
        return list(map(lambda elem: selected_array_to_singer_value(elem, sql_datatype, conn_info), elem))

    return selected_value_to_singer_value_impl(elem, sql_datatype, conn_info)

def selected_value_to_singer_value(elem, sql_datatype, conn_info):
    #are we dealing with an array?
    if sql_datatype.find('[]') > 0:
        cleaned_elem = create_array_elem(elem, sql_datatype, conn_info)
        return list(map(lambda elem: selected_array_to_singer_value(elem, sql_datatype, conn_info), (cleaned_elem or [])))

    return selected_value_to_singer_value_impl(elem, sql_datatype, conn_info)

def row_to_singer_message(stream, row, version, columns, time_extracted, md_map, conn_info):
    row_to_persist = ()
    md_map[('properties', '_sdc_deleted_at')] = {'sql-datatype' : 'timestamp with time zone'}

    for idx, elem in enumerate(row):
        sql_datatype = md_map.get(('properties', columns[idx]))['sql-datatype']
        cleaned_elem = selected_value_to_singer_value(elem, sql_datatype, conn_info)
        row_to_persist += (cleaned_elem,)

    rec = dict(zip(columns, row_to_persist))

    return singer.RecordMessage(
        stream=post_db.calculate_destination_stream_name(stream, md_map),
        record=rec,
        version=version,
        time_extracted=time_extracted)

def consume_message(streams, state, msg, time_extracted, conn_info):
    payload = json.loads(msg.payload)
    lsn = msg.data_start

    streams_lookup = {}
    for s in streams:
        streams_lookup[s.tap_stream_id] = s

    for c in payload['change']:
        tap_stream_id = post_db.compute_tap_stream_id(conn_info['dbname'], c['schema'], c['table'])
        if streams_lookup.get(tap_stream_id) is None:
            continue

        target_stream = streams_lookup[tap_stream_id]
        stream_version = get_stream_version(target_stream.tap_stream_id, state)
        stream_md_map = metadata.to_map(target_stream.metadata)

        if c['kind'] == 'insert':
            col_vals = c['columnvalues'] + [None]
            col_names = c['columnnames'] + ['_sdc_deleted_at']
            record_message = row_to_singer_message(target_stream, col_vals, stream_version, col_names, time_extracted, stream_md_map, conn_info)
        elif c['kind'] == 'update':
            col_vals = c['columnvalues'] + [None]
            col_names = c['columnnames'] + ['_sdc_deleted_at']
            record_message = row_to_singer_message(target_stream, col_vals, stream_version, col_names, time_extracted, stream_md_map, conn_info)
        elif c['kind'] == 'delete':
            col_names = c['oldkeys']['keynames'] + ['_sdc_deleted_at']
            col_vals = c['oldkeys']['keyvalues']  + [singer.utils.strftime(time_extracted)]
            record_message = row_to_singer_message(target_stream, col_vals, stream_version, col_names, time_extracted, stream_md_map, conn_info)
        else:
            raise Exception("unrecognized replication operation: {}".format(c['kind']))

        sync_common.send_schema_message(target_stream, ['lsn'])

        singer.write_message(record_message)
        state = singer.write_bookmark(state,
                                      target_stream.tap_stream_id,
                                      'lsn',
                                      lsn)

    LOGGER.info("sending feedback to server. flush_lsn = %s", msg.data_start)
    msg.cursor.send_feedback(flush_lsn=msg.data_start)


    return state

def locate_replication_slot(conn_info):
    with post_db.open_connection(conn_info, False) as conn:
        with conn.cursor() as cur:
            db_specific_slot = "stitch_{}".format(conn_info['dbname'])
            cur.execute("SELECT * FROM pg_replication_slots WHERE slot_name = %s AND plugin = %s", (db_specific_slot, 'wal2json'))
            if len(cur.fetchall()) == 1:
                LOGGER.info("using pg_replication_slot %s", db_specific_slot)
                return db_specific_slot


            cur.execute("SELECT * FROM pg_replication_slots WHERE slot_name = 'stitch' AND plugin = 'wal2json'")
            if len(cur.fetchall()) == 1:
                LOGGER.info("using pg_replication_slot 'stitch'")
                return 'stitch'

            raise Exception("Unable to find replication slot (stitch || {} with wal2json".format(db_specific_slot))


def sync_tables(conn_info, logical_streams, state):
    start_lsn = min([get_bookmark(state, s.tap_stream_id, 'lsn') for s in logical_streams])
    end_lsn = fetch_current_lsn(conn_info)
    time_extracted = utils.now()
    slot = locate_replication_slot(conn_info)

    with post_db.open_connection(conn_info, True) as conn:
        with conn.cursor() as cur:

            LOGGER.info("Starting Logical Replication for %s(%s): %s -> %s", list(map(lambda s: s.tap_stream_id, logical_streams)), slot, start_lsn, end_lsn)
            try:
                cur.start_replication(slot_name=slot, decode=True, start_lsn=start_lsn)
            except psycopg2.ProgrammingError:
                raise Exception("unable to start replication with logical replication slot {}".format(slot))

            cur.send_feedback(flush_lsn=start_lsn)
            keepalive_interval = 10.0
            rows_saved = 0
            while True:
                msg = cur.read_message()
                if msg:
                    state = consume_message(logical_streams, state, msg, time_extracted, conn_info)

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

    for s in logical_streams:
        LOGGER.info("updating bookmark for stream %s to end_lsn %s", s.tap_stream_id, end_lsn)
        state = singer.write_bookmark(state, s.tap_stream_id, 'lsn', end_lsn)

    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))
    return state
