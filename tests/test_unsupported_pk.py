import unittest
import os
import cx_Oracle, sys, string, datetime
import tap_oracle
import pdb
import singer
from singer import get_logger, metadata, write_bookmark
from tests.utils import get_test_connection, ensure_test_table, select_all_of_stream, set_replication_method_for_stream, crud_up_log_miner_fixtures, verify_crud_messages, insert_record, unselect_column
import tap_oracle.sync_strategies.log_miner as log_miner
import decimal
import math
import pytz
import strict_rfc3339
import copy

LOGGER = get_logger()

CAUGHT_MESSAGES = []

def do_not_dump_catalog(catalog):
    pass

tap_oracle.dump_catalog = do_not_dump_catalog

def singer_write_message(message):
    CAUGHT_MESSAGES.append(message)

def expected_record(fixture_row):
    expected_record = {}
    for k,v in fixture_row.items():
        expected_record[k.replace('"', '')] = v

    return expected_record

class UnsupportedPK(unittest.TestCase):
    maxDiff = None
    def setUp(self):
        with get_test_connection() as conn:
            cur = conn.cursor()
            table_spec = {"columns": [{"name": "interval_column", "type": "INTERVAL DAY TO SECOND",
                                       "primary_key": True },
                                      {"name": "age", "type": "integer"}            ],
                          "name": "CHICKEN"}


            ensure_test_table(table_spec)

    def test_catalog(self):
        singer.write_message = singer_write_message

        with get_test_connection() as conn:
            conn.autocommit = True

            catalog = tap_oracle.do_discovery(conn)
            chicken_stream = [s for s in catalog.streams if s.table == 'CHICKEN'][0]
            key_properties = metadata.to_map(chicken_stream.metadata).get(()).get('key-properties')

            #interval_column SHOULD BE unsupported
            self.assertEqual([], key_properties)

            chicken_stream = select_all_of_stream(chicken_stream)

            chicken_stream = set_replication_method_for_stream(chicken_stream, 'FULL_TABLE')
            cur = conn.cursor()

            cur.execute("""
               INSERT INTO CHICKEN (AGE, INTERVAL_COLUMN) values (3,
                   TIMESTAMP '2001-09-04 17:00:00.000000' - TIMESTAMP '2001-09-03 17:00:00.000000'
               )""")

            state = {}
            tap_oracle.do_sync(conn, catalog, None, state)


            #messages: ActivateVersion, SchemaMessage, Record, Record, State, ActivateVersion
            self.assertEqual(6, len(CAUGHT_MESSAGES))
            self.assertTrue(isinstance(CAUGHT_MESSAGES[0], singer.SchemaMessage))

            self.assertEqual([], CAUGHT_MESSAGES[0].key_properties)
            self.assertTrue(isinstance(CAUGHT_MESSAGES[1], singer.StateMessage))
            self.assertTrue(isinstance(CAUGHT_MESSAGES[2], singer.ActivateVersionMessage))
            self.assertTrue(isinstance(CAUGHT_MESSAGES[3], singer.RecordMessage))
            self.assertEqual({'AGE': 3}, CAUGHT_MESSAGES[3].record)
            self.assertTrue(isinstance(CAUGHT_MESSAGES[4], singer.ActivateVersionMessage))
            self.assertTrue(isinstance(CAUGHT_MESSAGES[5], singer.StateMessage))




if __name__== "__main__":
    test1 = UnsupportedPK()
    test1.setUp()
    test1.test_catalog()
