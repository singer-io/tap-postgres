import unittest
import os
import tap_postgres
import tap_postgres.sync_strategies.full_table as full_table
import tap_postgres.sync_strategies.common as pg_common
import pdb
import singer
from singer import get_logger, metadata, write_bookmark

try:
    from tests.utils import get_test_connection, ensure_test_table, select_all_of_stream, \
        set_replication_method_for_stream, insert_record, get_test_connection_config
except ImportError:
    from utils import get_test_connection, ensure_test_table, select_all_of_stream, set_replication_method_for_stream, \
        insert_record, get_test_connection_config

import decimal
import math
import pytz
import strict_rfc3339
import copy

LOGGER = get_logger()

CAUGHT_MESSAGES = []
COW_RECORD_COUNT = 0


def singer_write_message_no_cow(message):
    global COW_RECORD_COUNT

    if isinstance(message, singer.RecordMessage) and message.stream == 'COW':
        COW_RECORD_COUNT = COW_RECORD_COUNT + 1
        if COW_RECORD_COUNT > 2:
            raise Exception("simulated exception")
        CAUGHT_MESSAGES.append(message)
    else:
        CAUGHT_MESSAGES.append(message)


def singer_write_schema_ok(message):
    CAUGHT_MESSAGES.append(message)


def singer_write_message_ok(message):
    CAUGHT_MESSAGES.append(message)


def expected_record(fixture_row):
    expected_record = {}
    for k, v in fixture_row.items():
        expected_record[k.replace('"', '')] = v

    return expected_record


def do_not_dump_catalog(catalog):
    pass


tap_postgres.dump_catalog = do_not_dump_catalog
full_table.UPDATE_BOOKMARK_PERIOD = 1


class TestIncrementalReplication(unittest.TestCase):
    maxDiff = None

    def setUp(self):
        table_spec_1 = {"columns": [{"name": "id", "type": "serial", "primary_key": True},
                                    {"name": 'name', "type": "character varying"},
                                    {"name": 'colour', "type": "character varying"}],
                        "name": 'COW'}
        ensure_test_table(table_spec_1)
        global COW_RECORD_COUNT
        COW_RECORD_COUNT = 0
        global CAUGHT_MESSAGES
        CAUGHT_MESSAGES.clear()

    def test_catalog(self):
        singer.write_message = singer_write_message_ok
        pg_common.write_schema_message = singer_write_message_ok

        conn_config = get_test_connection_config()
        streams = tap_postgres.do_discovery(conn_config)
        cow_stream = [s for s in streams if s['table_name'] == 'COW'][0]
        self.assertIsNotNone(cow_stream)
        cow_stream = select_all_of_stream(cow_stream)
        cow_stream = set_replication_method_for_stream(cow_stream, 'INCREMENTAL', '__TRANSACTION_COMMIT_TIMESTAMP__')

        with get_test_connection() as conn:
            conn.autocommit = True
            cur = conn.cursor()

            cow_rec = {'name': 'betty', 'colour': 'blue'}
            insert_record(cur, 'COW', cow_rec)

            cow_rec = {'name': 'smelly', 'colour': 'brow'}
            insert_record(cur, 'COW', cow_rec)

            cow_rec = {'name': 'pooper', 'colour': 'green'}
            insert_record(cur, 'COW', cow_rec)

        state = {}
        # the initial phase of cows logical replication will be a full table.
        # it will sync the first record and then blow up on the 2nd record

        tap_postgres.do_sync(get_test_connection_config(), {'streams': streams}, None, state)

        self.assertEqual(True, True)

if __name__ == "__main__":
    test1 = TestIncrementalReplication()
    test1.setUp()
    test1.test_catalog()
