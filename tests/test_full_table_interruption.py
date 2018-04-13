import unittest
import os
import tap_postgres
import tap_postgres.sync_strategies.full_table as full_table
import pdb
import singer
from singer import get_logger, metadata, write_bookmark
try:
    from tests.utils import get_test_connection, ensure_test_table, select_all_of_stream, set_replication_method_for_stream, insert_record, get_test_connection_config
except ImportError:
    from utils import get_test_connection, ensure_test_table, select_all_of_stream, set_replication_method_for_stream, insert_record, get_test_connection_config

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
        if COW_RECORD_COUNT > 1:
            raise Exception("simulated exception")
        CAUGHT_MESSAGES.append(message)
    else:
        CAUGHT_MESSAGES.append(message)


def singer_write_message_ok(message):
    CAUGHT_MESSAGES.append(message)

def expected_record(fixture_row):
    expected_record = {}
    for k,v in fixture_row.items():
        expected_record[k.replace('"', '')] = v

    return expected_record

def do_not_dump_catalog(catalog):
    pass

tap_postgres.dump_catalog = do_not_dump_catalog
full_table.UPDATE_BOOKMARK_PERIOD = 1

class LogicalInterruption(unittest.TestCase):
    maxDiff = None

    def setUp(self):
        table_spec_1 = {"columns": [{"name": "id", "type" : "serial",       "primary_key" : True},
                                    {"name" : 'name', "type": "character varying"},
                                    {"name" : 'colour', "type": "character varying"}],
                        "name" : 'COW'}
        ensure_test_table(table_spec_1)
        global COW_RECORD_COUNT
        COW_RECORD_COUNT = 0
        global CAUGHT_MESSAGES
        CAUGHT_MESSAGES.clear()

    def test_catalog(self):
        singer.write_message = singer_write_message_no_cow

        conn_config = get_test_connection_config()
        catalog = tap_postgres.do_discovery(conn_config)
        cow_stream = [s for s in catalog.streams if s.table == 'COW'][0]
        self.assertIsNotNone(cow_stream)
        cow_stream = select_all_of_stream(cow_stream)
        cow_stream = set_replication_method_for_stream(cow_stream, 'LOG_BASED')

        with get_test_connection() as conn:
            conn.autocommit = True
            cur = conn.cursor()

            cow_rec = {'name' : 'betty', 'colour' : 'blue'}
            insert_record(cur, 'COW', cow_rec)
            cow_rec = {'name' : 'smelly', 'colour' : 'brow'}
            insert_record(cur, 'COW', cow_rec)

        state = {}
        #the initial phase of cows logical replication will be a full table.
        #it will sync the first record and then blow up on the 2nd record
        try:
            tap_postgres.do_sync(get_test_connection_config(), catalog, None, state)
        except Exception as ex:
            # LOGGER.exception(ex)
            blew_up_on_cow = True

        self.assertTrue(blew_up_on_cow)
        self.assertEqual(5, len(CAUGHT_MESSAGES))

        self.assertTrue(isinstance(CAUGHT_MESSAGES[0], singer.SchemaMessage))
        self.assertTrue(isinstance(CAUGHT_MESSAGES[1], singer.StateMessage))
        self.assertIsNone(CAUGHT_MESSAGES[1].value['bookmarks']['postgres-public-COW'].get('xmin'))
        self.assertIsNotNone(CAUGHT_MESSAGES[1].value['bookmarks']['postgres-public-COW'].get('lsn'))
        end_lsn = CAUGHT_MESSAGES[1].value['bookmarks']['postgres-public-COW'].get('lsn')

        self.assertTrue(isinstance(CAUGHT_MESSAGES[2], singer.ActivateVersionMessage))
        new_version = CAUGHT_MESSAGES[2].version

        self.assertTrue(isinstance(CAUGHT_MESSAGES[3], singer.RecordMessage))
        self.assertEqual(CAUGHT_MESSAGES[3].record, {'colour': 'blue', 'id': 1, 'name': 'betty'})
        self.assertEqual('COW', CAUGHT_MESSAGES[3].stream)

        self.assertTrue(isinstance(CAUGHT_MESSAGES[4], singer.StateMessage))
        #xmin is set while we are processing the full table replication
        self.assertIsNotNone(CAUGHT_MESSAGES[4].value['bookmarks']['postgres-public-COW']['xmin'])
        last_xmin = CAUGHT_MESSAGES[4].value['bookmarks']['postgres-public-COW']['xmin']

        self.assertEqual(CAUGHT_MESSAGES[4].value['bookmarks']['postgres-public-COW']['lsn'], end_lsn)
        old_state = CAUGHT_MESSAGES[4].value

        #run another do_sync, should get the remaining record which effectively finishes the initial full_table
        #replication portion of the logical replication
        singer.write_message = singer_write_message_ok
        global COW_RECORD_COUNT
        COW_RECORD_COUNT = 0
        CAUGHT_MESSAGES.clear()
        tap_postgres.do_sync(get_test_connection_config(), catalog, None, old_state)

        # pdb.set_trace()
        self.assertEqual(6, len(CAUGHT_MESSAGES))

        self.assertTrue(isinstance(CAUGHT_MESSAGES[0], singer.SchemaMessage))
        self.assertTrue(isinstance(CAUGHT_MESSAGES[1], singer.StateMessage))
        self.assertEqual(CAUGHT_MESSAGES[1].value['bookmarks']['postgres-public-COW'].get('xmin'), last_xmin)
        self.assertEqual(CAUGHT_MESSAGES[1].value['bookmarks']['postgres-public-COW'].get('lsn'), end_lsn)
        self.assertEqual(CAUGHT_MESSAGES[1].value['bookmarks']['postgres-public-COW'].get('version'), new_version)

        self.assertTrue(isinstance(CAUGHT_MESSAGES[2], singer.RecordMessage))
        self.assertEqual(CAUGHT_MESSAGES[2].record, {'colour': 'brow', 'id': 2, 'name': 'smelly'})
        self.assertEqual('COW', CAUGHT_MESSAGES[2].stream)

        self.assertTrue(isinstance(CAUGHT_MESSAGES[3], singer.StateMessage))
        self.assertTrue(CAUGHT_MESSAGES[3].value['bookmarks']['postgres-public-COW'].get('xmin') > last_xmin)
        self.assertEqual(CAUGHT_MESSAGES[3].value['bookmarks']['postgres-public-COW'].get('lsn'), end_lsn)
        self.assertEqual(CAUGHT_MESSAGES[3].value['bookmarks']['postgres-public-COW'].get('version'), new_version)


        self.assertTrue(isinstance(CAUGHT_MESSAGES[4], singer.ActivateVersionMessage))
        self.assertEqual(CAUGHT_MESSAGES[4].version, new_version)

        self.assertTrue(isinstance(CAUGHT_MESSAGES[5], singer.StateMessage))
        self.assertIsNone(CAUGHT_MESSAGES[5].value['bookmarks']['postgres-public-COW'].get('xmin'))
        self.assertEqual(CAUGHT_MESSAGES[5].value['bookmarks']['postgres-public-COW'].get('lsn'), end_lsn)
        self.assertEqual(CAUGHT_MESSAGES[5].value['bookmarks']['postgres-public-COW'].get('version'), new_version)

class FullTableInterruption(unittest.TestCase):
    maxDiff = None
    def setUp(self):
        table_spec_1 = {"columns": [{"name": "id", "type" : "serial",       "primary_key" : True},
                                    {"name" : 'name', "type": "character varying"},
                                    {"name" : 'colour', "type": "character varying"}],
                        "name" : 'COW'}
        ensure_test_table(table_spec_1)

        table_spec_2 = {"columns": [{"name": "id", "type" : "serial",       "primary_key" : True},
                                    {"name" : 'name', "type": "character varying"},
                                    {"name" : 'colour', "type": "character varying"}],
                        "name" : 'CHICKEN'}
        ensure_test_table(table_spec_2)

        global COW_RECORD_COUNT
        COW_RECORD_COUNT = 0
        global CAUGHT_MESSAGES
        CAUGHT_MESSAGES.clear()

    def test_catalog(self):
        singer.write_message = singer_write_message_no_cow

        conn_config = get_test_connection_config()
        catalog = tap_postgres.do_discovery(conn_config)
        cow_stream = [s for s in catalog.streams if s.table == 'COW'][0]
        self.assertIsNotNone(cow_stream)
        cow_stream = select_all_of_stream(cow_stream)
        cow_stream = set_replication_method_for_stream(cow_stream, 'FULL_TABLE')

        chicken_stream = [s for s in catalog.streams if s.table == 'CHICKEN'][0]
        self.assertIsNotNone(chicken_stream)
        chicken_stream = select_all_of_stream(chicken_stream)
        chicken_stream = set_replication_method_for_stream(chicken_stream, 'FULL_TABLE')
        with get_test_connection() as conn:
            conn.autocommit = True
            cur = conn.cursor()

            cow_rec = {'name' : 'betty', 'colour' : 'blue'}
            insert_record(cur, 'COW', cow_rec)
            cow_rec = {'name' : 'smelly', 'colour' : 'brow'}
            insert_record(cur, 'COW', cow_rec)

            chicken_rec = {'name' : 'fred', 'colour' : 'red'}
            insert_record(cur, 'CHICKEN', chicken_rec)

        state = {}
        #this will sync the CHICKEN but then blow up on the COW
        try:
            tap_postgres.do_sync(get_test_connection_config(), catalog, None, state)
        except Exception as ex:
            # LOGGER.exception(ex)
            blew_up_on_cow = True

        self.assertTrue(blew_up_on_cow)

        self.assertEqual(12, len(CAUGHT_MESSAGES))
        self.assertTrue(isinstance(CAUGHT_MESSAGES[0], singer.SchemaMessage))
        self.assertTrue(isinstance(CAUGHT_MESSAGES[1], singer.StateMessage))
        self.assertIsNone(CAUGHT_MESSAGES[1].value['bookmarks']['postgres-public-CHICKEN'].get('xmin'))

        self.assertTrue(isinstance(CAUGHT_MESSAGES[2], singer.ActivateVersionMessage))
        new_version = CAUGHT_MESSAGES[2].version

        self.assertTrue(isinstance(CAUGHT_MESSAGES[3], singer.RecordMessage))
        self.assertEqual('CHICKEN', CAUGHT_MESSAGES[3].stream)

        self.assertTrue(isinstance(CAUGHT_MESSAGES[4], singer.StateMessage))
        #xmin is set while we are processing the full table replication
        self.assertIsNotNone(CAUGHT_MESSAGES[4].value['bookmarks']['postgres-public-CHICKEN']['xmin'])

        self.assertTrue(isinstance(CAUGHT_MESSAGES[5], singer.ActivateVersionMessage))
        self.assertEqual(CAUGHT_MESSAGES[5].version, new_version)

        self.assertTrue(isinstance(CAUGHT_MESSAGES[6], singer.StateMessage))
        self.assertEqual(None, singer.get_currently_syncing( CAUGHT_MESSAGES[6].value))
        #xmin is cleared at the end of the full table replication
        self.assertIsNone(CAUGHT_MESSAGES[6].value['bookmarks']['postgres-public-CHICKEN']['xmin'])


        #cow messages
        self.assertTrue(isinstance(CAUGHT_MESSAGES[7], singer.SchemaMessage))
        self.assertEqual("COW", CAUGHT_MESSAGES[7].stream)
        self.assertTrue(isinstance(CAUGHT_MESSAGES[8], singer.StateMessage))
        self.assertIsNone(CAUGHT_MESSAGES[8].value['bookmarks']['postgres-public-COW'].get('xmin'))
        self.assertEqual("postgres-public-COW", CAUGHT_MESSAGES[8].value['currently_syncing'])
        self.assertTrue(isinstance(CAUGHT_MESSAGES[9], singer.ActivateVersionMessage))
        cow_version = CAUGHT_MESSAGES[9].version
        self.assertTrue(isinstance(CAUGHT_MESSAGES[10], singer.RecordMessage))
        self.assertEqual(CAUGHT_MESSAGES[10].record['name'], 'betty')
        self.assertEqual('COW', CAUGHT_MESSAGES[10].stream)
        self.assertTrue(isinstance(CAUGHT_MESSAGES[11], singer.StateMessage))
        #xmin is set while we are processing the full table replication
        self.assertIsNotNone(CAUGHT_MESSAGES[11].value['bookmarks']['postgres-public-COW']['xmin'])
        old_state = CAUGHT_MESSAGES[11].value


        #run another do_sync
        singer.write_message = singer_write_message_ok
        CAUGHT_MESSAGES.clear()
        global COW_RECORD_COUNT
        COW_RECORD_COUNT = 0
        tap_postgres.do_sync(get_test_connection_config(), catalog, None, old_state)


        # self.assertEqual(5, len(CAUGHT_MESSAGES))
        self.assertTrue(isinstance(CAUGHT_MESSAGES[0], singer.SchemaMessage))
        self.assertTrue(isinstance(CAUGHT_MESSAGES[1], singer.StateMessage))

        # because we were interrupted, we do not switch versions
        self.assertEqual(CAUGHT_MESSAGES[1].value['bookmarks']['postgres-public-COW']['version'], cow_version)
        self.assertIsNotNone(CAUGHT_MESSAGES[1].value['bookmarks']['postgres-public-COW']['xmin'])
        self.assertEqual("postgres-public-COW", singer.get_currently_syncing(CAUGHT_MESSAGES[1].value))

        self.assertTrue(isinstance(CAUGHT_MESSAGES[2], singer.RecordMessage))
        self.assertEqual(CAUGHT_MESSAGES[2].record['name'], 'smelly')
        self.assertEqual('COW', CAUGHT_MESSAGES[2].stream)


        #after record: activate version, state with no xmin or currently syncing
        self.assertTrue(isinstance(CAUGHT_MESSAGES[3], singer.StateMessage))
        #xmin is cleared because we are finished the full table replication
        self.assertIsNone(CAUGHT_MESSAGES[3].value['bookmarks']['postgres-public-CHICKEN']['xmin'])
        self.assertEqual(singer.get_currently_syncing( CAUGHT_MESSAGES[3].value), 'postgres-public-COW')

        self.assertTrue(isinstance(CAUGHT_MESSAGES[4], singer.ActivateVersionMessage))
        self.assertEqual(CAUGHT_MESSAGES[4].version, cow_version)

        self.assertTrue(isinstance(CAUGHT_MESSAGES[5], singer.StateMessage))
        self.assertIsNone(singer.get_currently_syncing( CAUGHT_MESSAGES[5].value))
        self.assertIsNone(CAUGHT_MESSAGES[5].value['bookmarks']['postgres-public-CHICKEN']['xmin'])
        self.assertIsNone(singer.get_currently_syncing( CAUGHT_MESSAGES[5].value))


if __name__== "__main__":
    test1 = LogicalInterruption()
    test1.setUp()
    test1.test_catalog()
