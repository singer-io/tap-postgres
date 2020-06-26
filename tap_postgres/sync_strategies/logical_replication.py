#!/usr/bin/env python3
# pylint: disable=missing-docstring,not-an-iterable,too-many-locals,too-many-arguments,invalid-name,too-many-return-statements,too-many-branches,len-as-condition,too-many-nested-blocks,wrong-import-order,duplicate-code, anomalous-backslash-in-string, too-many-statements, singleton-comparison, consider-using-in

import singer
import datetime
import decimal
from singer import utils, get_bookmark
import singer.metadata as metadata
import tap_postgres.db as post_db
import tap_postgres.sync_strategies.common as sync_common
from dateutil.parser import parse
import psycopg2
from psycopg2 import sql
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

def add_automatic_properties(stream, conn_config):
    stream['schema']['properties']['_sdc_deleted_at'] = {'type' : ['null', 'string'], 'format' :'date-time'}
    if conn_config.get('debug_lsn'):
        LOGGER.info('debug_lsn is ON')
        stream['schema']['properties']['_sdc_lsn'] = {'type' : ['null', 'string']}
    else:
        LOGGER.info('debug_lsn is OFF')

    return stream

def get_stream_version(tap_stream_id, state):
    stream_version = singer.get_bookmark(state, tap_stream_id, 'version')

    if stream_version is None:
        raise Exception("version not found for log miner {}".format(tap_stream_id))

    return stream_version

def tuples_to_map(accum, t):
    accum[t[0]] = t[1]
    return accum

def create_hstore_elem_query(elem):
    return sql.SQL("SELECT hstore_to_array({})").format(sql.Literal(elem))

def create_hstore_elem(conn_info, elem):
    with post_db.open_connection(conn_info) as conn:
        with conn.cursor() as cur:
            query = create_hstore_elem_query(elem)
            cur.execute(query)
            res = cur.fetchone()[0]
            hstore_elem = reduce(tuples_to_map, [res[i:i + 2] for i in range(0, len(res), 2)], {})
            return hstore_elem

def create_array_elem(elem, sql_datatype, conn_info):
    if elem is None:
        return None

    with post_db.open_connection(conn_info) as conn:
        with conn.cursor() as cur:
            if sql_datatype == 'bit[]':
                cast_datatype = 'boolean[]'
            elif sql_datatype == 'boolean[]':
                cast_datatype = 'boolean[]'
            elif sql_datatype == 'character varying[]':
                cast_datatype = 'character varying[]'
            elif sql_datatype == 'cidr[]':
                cast_datatype = 'cidr[]'
            elif sql_datatype == 'citext[]':
                cast_datatype = 'text[]'
            elif sql_datatype == 'date[]':
                cast_datatype = 'text[]'
            elif sql_datatype == 'double precision[]':
                cast_datatype = 'double precision[]'
            elif sql_datatype == 'hstore[]':
                cast_datatype = 'text[]'
            elif sql_datatype == 'integer[]':
                cast_datatype = 'integer[]'
            elif sql_datatype == 'bigint[]':
                cast_datatype = 'bigint[]'
            elif sql_datatype == 'inet[]':
                cast_datatype = 'inet[]'
            elif sql_datatype == 'json[]':
                cast_datatype = 'text[]'
            elif sql_datatype == 'jsonb[]':
                cast_datatype = 'text[]'
            elif sql_datatype == 'macaddr[]':
                cast_datatype = 'macaddr[]'
            elif sql_datatype == 'money[]':
                cast_datatype = 'text[]'
            elif sql_datatype == 'numeric[]':
                cast_datatype = 'text[]'
            elif sql_datatype == 'real[]':
                cast_datatype = 'real[]'
            elif sql_datatype == 'smallint[]':
                cast_datatype = 'smallint[]'
            elif sql_datatype == 'text[]':
                cast_datatype = 'text[]'
            elif sql_datatype in ('time without time zone[]', 'time with time zone[]'):
                cast_datatype = 'text[]'
            elif sql_datatype in ('timestamp with time zone[]', 'timestamp without time zone[]'):
                cast_datatype = 'text[]'
            elif sql_datatype == 'uuid[]':
                cast_datatype = 'text[]'

            else:
                #custom datatypes like enums
                cast_datatype = 'text[]'

            sql_stmt = """SELECT $stitch_quote${}$stitch_quote$::{}""".format(elem, cast_datatype)
            cur.execute(sql_stmt)
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
        #for arrays, elem will == True
        #for ordinary bits, elem will == '1'
        return elem == '1' or elem == True
    if sql_datatype == 'boolean':
        return elem
    if sql_datatype == 'hstore':
        return create_hstore_elem(conn_info, elem)
    if 'numeric' in sql_datatype:
        return decimal.Decimal(str(elem))
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
    md_map[('properties', '_sdc_lsn')] = {'sql-datatype' : "character varying"}

    for idx, elem in enumerate(row):
        sql_datatype = md_map.get(('properties', columns[idx])).get('sql-datatype')

        if not sql_datatype:
            LOGGER.info("No sql-datatype found for stream %s: %s", stream, columns[idx])
            raise Exception("Unable to find sql-datatype for stream {}".format(stream))

        cleaned_elem = selected_value_to_singer_value(elem, sql_datatype, conn_info)
        row_to_persist += (cleaned_elem,)

    rec = dict(zip(columns, row_to_persist))

    return singer.RecordMessage(
        stream=post_db.calculate_destination_stream_name(stream, md_map),
        record=rec,
        version=version,
        time_extracted=time_extracted)

def consume_message_format_2(payload, conn_info, streams_lookup, state, time_extracted, lsn):
    ## Action Types:
    # I = Insert
    # U = Update
    # D = Delete
    # B = Begin Transaction
    # C = Commit Transaction
    # M = Message
    # T = Truncate
    action = payload['action']
    if action not in ['U', 'I', 'D']:
        LOGGER.debug("Skipping message of type %s", action)
        yield None
    else:
        tap_stream_id = post_db.compute_tap_stream_id(conn_info['dbname'], payload['schema'], payload['table'])
        if streams_lookup.get(tap_stream_id) is None:
            yield None
        else:
            target_stream = streams_lookup[tap_stream_id]
            stream_version = get_stream_version(target_stream['tap_stream_id'], state)
            stream_md_map = metadata.to_map(target_stream['metadata'])

            desired_columns = [col for col in target_stream['schema']['properties'].keys() if sync_common.should_sync_column(stream_md_map, col)]

            col_names = []
            col_vals = []
            if payload['action'] in ['I', 'U']:
                for column in payload['columns']:
                    if column['name'] in set(desired_columns):
                        col_names.append(column['name'])
                        col_vals.append(column['value'])

                col_names = col_names + ['_sdc_deleted_at']
                col_vals = col_vals + [None]

                if conn_info.get('debug_lsn'):
                    col_names = col_names + ['_sdc_lsn']
                    col_vals = col_vals + [str(lsn)]

            elif payload['action'] == 'D':
                for column in payload['identity']:
                    if column['name'] in set(desired_columns):
                        col_names.append(column['name'])
                        col_vals.append(column['value'])

                col_names = col_names + ['_sdc_deleted_at']
                col_vals = col_vals + [singer.utils.strftime(singer.utils.strptime_to_utc(payload['timestamp']))]

                if conn_info.get('debug_lsn'):
                    col_vals = col_vals + [str(lsn)]
                    col_names = col_names + ['_sdc_lsn']

            # Yield 1 record to match the API of V1
            yield row_to_singer_message(target_stream, col_vals, stream_version, col_names, time_extracted, stream_md_map, conn_info)

            state = singer.write_bookmark(state,
                                          target_stream['tap_stream_id'],
                                          'lsn',
                                          lsn)

# message-format v1
def consume_message_format_1(payload, conn_info, streams_lookup, state, time_extracted, lsn):
    for c in payload['change']:
        tap_stream_id = post_db.compute_tap_stream_id(conn_info['dbname'], c['schema'], c['table'])
        if streams_lookup.get(tap_stream_id) is None:
            continue

        target_stream = streams_lookup[tap_stream_id]
        stream_version = get_stream_version(target_stream['tap_stream_id'], state)
        stream_md_map = metadata.to_map(target_stream['metadata'])


        desired_columns = [c for c in target_stream['schema']['properties'].keys() if sync_common.should_sync_column(stream_md_map, c)]

        if c['kind'] == 'insert':
            col_names = []
            col_vals = []
            for idx, col in enumerate(c['columnnames']):
                if col in set(desired_columns):
                    col_names.append(col)
                    col_vals.append(c['columnvalues'][idx])

            col_names = col_names + ['_sdc_deleted_at']
            col_vals = col_vals + [None]
            if conn_info.get('debug_lsn'):
                col_names = col_names + ['_sdc_lsn']
                col_vals = col_vals + [str(lsn)]
            record_message = row_to_singer_message(target_stream, col_vals, stream_version, col_names, time_extracted, stream_md_map, conn_info)

        elif c['kind'] == 'update':
            col_names = []
            col_vals = []
            for idx, col in enumerate(c['columnnames']):
                if col in set(desired_columns):
                    col_names.append(col)
                    col_vals.append(c['columnvalues'][idx])

            col_names = col_names + ['_sdc_deleted_at']
            col_vals = col_vals + [None]

            if conn_info.get('debug_lsn'):
                col_vals = col_vals + [str(lsn)]
                col_names = col_names + ['_sdc_lsn']
            record_message = row_to_singer_message(target_stream, col_vals, stream_version, col_names, time_extracted, stream_md_map, conn_info)

        elif c['kind'] == 'delete':
            col_names = []
            col_vals = []
            for idx, col in enumerate(c['oldkeys']['keynames']):
                if col in set(desired_columns):
                    col_names.append(col)
                    col_vals.append(c['oldkeys']['keyvalues'][idx])


            col_names = col_names + ['_sdc_deleted_at']
            col_vals = col_vals  + [singer.utils.strftime(time_extracted)]
            if conn_info.get('debug_lsn'):
                col_vals = col_vals + [str(lsn)]
                col_names = col_names + ['_sdc_lsn']
            record_message = row_to_singer_message(target_stream, col_vals, stream_version, col_names, time_extracted, stream_md_map, conn_info)

        else:
            raise Exception("unrecognized replication operation: {}".format(c['kind']))


        yield record_message
        state = singer.write_bookmark(state,
                                      target_stream['tap_stream_id'],
                                      'lsn',
                                      lsn)


def consume_message(streams, state, msg, time_extracted, conn_info, end_lsn, message_format="1"):
    payload = json.loads(msg.payload)
    lsn = msg.data_start

    streams_lookup = {s['tap_stream_id']: s for s in streams}

    if message_format == "1":
        records = consume_message_format_1(payload, conn_info, streams_lookup, state, time_extracted, lsn)
    elif message_format == "2":
        records = consume_message_format_2(payload, conn_info, streams_lookup, state, time_extracted, lsn)
    else:
        raise Exception("Unknown wal2json message format version: {}".format(message_format))

    for record_message in records:
        if record_message:
            singer.write_message(record_message)
        # Pulled out of refactor so we send a keep-alive per-record
        LOGGER.debug("sending feedback to server with NO flush_lsn. just a keep-alive")
        msg.cursor.send_feedback()

    LOGGER.debug("sending feedback to server. flush_lsn = %s", msg.data_start)
    if msg.data_start > end_lsn:
        raise Exception("incorrectly attempting to flush an lsn({}) > end_lsn({})".format(msg.data_start, end_lsn))

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


def sync_tables(conn_info, logical_streams, state, end_lsn):
    start_lsn = min([get_bookmark(state, s['tap_stream_id'], 'lsn') for s in logical_streams])
    time_extracted = utils.now()
    slot = locate_replication_slot(conn_info)
    last_lsn_processed = None
    poll_total_seconds = conn_info['logical_poll_total_seconds'] or 60 * 30  #we are willing to poll for a total of 30 minutes without finding a record
    keep_alive_time = 10.0
    begin_ts = datetime.datetime.now()

    for s in logical_streams:
        sync_common.send_schema_message(s, ['lsn'])

    with post_db.open_connection(conn_info, True) as conn:
        with conn.cursor() as cur:
            LOGGER.info("Starting Logical Replication for %s(%s): %s -> %s. poll_total_seconds: %s", list(map(lambda s: s['tap_stream_id'], logical_streams)), slot, start_lsn, end_lsn, poll_total_seconds)

            replication_params = {"slot_name": slot,
                                  "decode": True,
                                  "start_lsn": start_lsn}
            message_format = conn_info.get("wal2json_message_format") or "1"
            if message_format == "2":
                LOGGER.info("Using wal2json format-version 2")
                replication_params["options"] = {"format-version": 2, "include-timestamp": True}

            try:
                cur.start_replication(**replication_params)
            except psycopg2.ProgrammingError:
                raise Exception("unable to start replication with logical replication slot {}".format(slot))

            rows_saved = 0
            while True:
                poll_duration = (datetime.datetime.now() - begin_ts).total_seconds()
                if poll_duration > poll_total_seconds:
                    LOGGER.info("breaking after %s seconds of polling with no data", poll_duration)
                    break

                msg = cur.read_message()
                if msg:
                    begin_ts = datetime.datetime.now()
                    if msg.data_start > end_lsn:
                        LOGGER.info("gone past end_lsn %s for run. breaking", end_lsn)
                        break

                    state = consume_message(logical_streams, state, msg, time_extracted,
                                            conn_info, end_lsn, message_format=message_format)
                    #msg has been consumed. it has been processed
                    last_lsn_processed = msg.data_start
                    rows_saved = rows_saved + 1
                    if rows_saved % UPDATE_BOOKMARK_PERIOD == 0:
                        singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))
                else:
                    now = datetime.datetime.now()
                    timeout = keep_alive_time - (now - cur.io_timestamp).total_seconds()
                    try:
                        sel = select([cur], [], [], max(0, timeout))
                        if not any(sel):
                            LOGGER.info("no data for %s seconds. sending feedback to server with NO flush_lsn. just a keep-alive", timeout)
                            cur.send_feedback()

                    except InterruptedError:
                        pass  # recalculate timeout and continue

    if last_lsn_processed:
        for s in logical_streams:
            LOGGER.info("updating bookmark for stream %s to last_lsn_processed %s", s['tap_stream_id'], last_lsn_processed)
            state = singer.write_bookmark(state, s['tap_stream_id'], 'lsn', last_lsn_processed)

    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))
    return state
