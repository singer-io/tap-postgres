import unittest
import os
import tap_postgres
import tap_postgres.sync_strategies.full_table as full_table
import pdb
import singer
from singer import get_logger, metadata, write_bookmark
from tests.utils import get_test_connection, ensure_test_table, select_all_of_stream, set_replication_method_for_stream, crud_up_log_miner_fixtures, verify_crud_messages, insert_record, unselect_column
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

class CurrentlySyncing(unittest.TestCase):
    maxDiff = None
    def setUp(self):
        with get_test_connection() as conn:
            cur = conn.cursor()
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

    def test_catalog(self):
        singer.write_message = singer_write_message_no_cow

        with get_test_connection() as conn:
            conn.autocommit = True

            catalog = tap_postgres.do_discovery(conn)

            cow_stream = [s for s in catalog.streams if s.table == 'COW'][0]
            cow_stream = select_all_of_stream(cow_stream)
            cow_stream = set_replication_method_for_stream(cow_stream, 'FULL_TABLE')

            chicken_stream = [s for s in catalog.streams if s.table == 'CHICKEN'][0]
            chicken_stream = select_all_of_stream(chicken_stream)
            chicken_stream = set_replication_method_for_stream(chicken_stream, 'FULL_TABLE')

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
                tap_postgres.do_sync(conn, catalog, None, state)
            except Exception:
                blew_up_on_cow = True

            self.assertTrue(blew_up_on_cow)

            self.assertEqual(12, len(CAUGHT_MESSAGES))
            self.assertTrue(isinstance(CAUGHT_MESSAGES[0], singer.SchemaMessage))
            self.assertTrue(isinstance(CAUGHT_MESSAGES[1], singer.StateMessage))
            self.assertIsNone(CAUGHT_MESSAGES[1].value['bookmarks']['public-CHICKEN'].get('xmin'))

            self.assertTrue(isinstance(CAUGHT_MESSAGES[2], singer.ActivateVersionMessage))
            new_version = CAUGHT_MESSAGES[2].version

            self.assertTrue(isinstance(CAUGHT_MESSAGES[3], singer.RecordMessage))
            self.assertEqual('CHICKEN', CAUGHT_MESSAGES[3].stream)

            self.assertTrue(isinstance(CAUGHT_MESSAGES[4], singer.StateMessage))
            #xmin is set while we are processing the full table replication
            self.assertIsNotNone(CAUGHT_MESSAGES[4].value['bookmarks']['public-CHICKEN']['xmin'])

            self.assertTrue(isinstance(CAUGHT_MESSAGES[5], singer.ActivateVersionMessage))
            self.assertEqual(CAUGHT_MESSAGES[5].version, new_version)


            self.assertTrue(isinstance(CAUGHT_MESSAGES[6], singer.StateMessage))
            self.assertEqual(None, singer.get_currently_syncing( CAUGHT_MESSAGES[6].value))
            #xmin is cleared at the end of the full table replication
            self.assertIsNone(CAUGHT_MESSAGES[6].value['bookmarks']['public-CHICKEN']['xmin'])


            #cow messages
            self.assertTrue(isinstance(CAUGHT_MESSAGES[7], singer.SchemaMessage))
            self.assertEqual("COW", CAUGHT_MESSAGES[7].stream)
            self.assertTrue(isinstance(CAUGHT_MESSAGES[8], singer.StateMessage))
            self.assertIsNone(CAUGHT_MESSAGES[8].value['bookmarks']['public-COW'].get('xmin'))
            self.assertEqual("public-COW", CAUGHT_MESSAGES[8].value['currently_syncing'])
            self.assertTrue(isinstance(CAUGHT_MESSAGES[9], singer.ActivateVersionMessage))
            cow_version = CAUGHT_MESSAGES[9].version
            self.assertTrue(isinstance(CAUGHT_MESSAGES[10], singer.RecordMessage))
            self.assertEqual(CAUGHT_MESSAGES[10].record['name'], 'betty')
            self.assertEqual('COW', CAUGHT_MESSAGES[10].stream)
            self.assertTrue(isinstance(CAUGHT_MESSAGES[11], singer.StateMessage))
            #xmin is set while we are processing the full table replication
            self.assertIsNotNone(CAUGHT_MESSAGES[11].value['bookmarks']['public-COW']['xmin'])
            old_state = CAUGHT_MESSAGES[11].value


            #run another do_sync
            singer.write_message = singer_write_message_ok
            CAUGHT_MESSAGES.clear()
            global COW_RECORD_COUNT
            COW_RECORD_COUNT = 0
            tap_postgres.do_sync(conn, catalog, None, old_state)


            # self.assertEqual(5, len(CAUGHT_MESSAGES))
            self.assertTrue(isinstance(CAUGHT_MESSAGES[0], singer.SchemaMessage))
            self.assertTrue(isinstance(CAUGHT_MESSAGES[1], singer.StateMessage))

            # because we were interrupted, we do not switch versions
            self.assertEqual(CAUGHT_MESSAGES[1].value['bookmarks']['public-COW']['version'], cow_version)
            self.assertIsNotNone(CAUGHT_MESSAGES[1].value['bookmarks']['public-COW']['xmin'])
            self.assertEqual("public-COW", singer.get_currently_syncing(CAUGHT_MESSAGES[1].value))

            self.assertTrue(isinstance(CAUGHT_MESSAGES[2], singer.RecordMessage))
            self.assertEqual(CAUGHT_MESSAGES[2].record['name'], 'smelly')
            self.assertEqual('COW', CAUGHT_MESSAGES[2].stream)


            #after record: activate version, state with no xmin or currently syncing
            self.assertTrue(isinstance(CAUGHT_MESSAGES[3], singer.StateMessage))
            #xmin is cleared because we are finished the full table replication
            self.assertIsNone(CAUGHT_MESSAGES[3].value['bookmarks']['public-CHICKEN']['xmin'])
            self.assertEqual(singer.get_currently_syncing( CAUGHT_MESSAGES[3].value), 'public-COW')

            self.assertTrue(isinstance(CAUGHT_MESSAGES[4], singer.ActivateVersionMessage))
            self.assertEqual(CAUGHT_MESSAGES[4].version, cow_version)

            self.assertTrue(isinstance(CAUGHT_MESSAGES[5], singer.StateMessage))
            self.assertIsNone(singer.get_currently_syncing( CAUGHT_MESSAGES[5].value))
            self.assertIsNone(CAUGHT_MESSAGES[5].value['bookmarks']['public-CHICKEN']['xmin'])
            self.assertIsNone(singer.get_currently_syncing( CAUGHT_MESSAGES[5].value))






if __name__== "__main__":
    test1 = CurrentlySyncing()
    test1.setUp()
    test1.test_catalog()
