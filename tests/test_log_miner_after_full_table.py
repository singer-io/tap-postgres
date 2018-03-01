import unittest
import os
import cx_Oracle, sys, string, datetime
import tap_oracle
import pdb
import singer
from singer import get_logger, metadata, write_bookmark
from tests.utils import get_test_connection, ensure_test_table, select_all_of_stream, set_replication_method_for_stream, insert_record, unselect_column
import tap_oracle.sync_strategies.log_miner as log_miner
import decimal
import math
import pytz
import strict_rfc3339
import copy

LOGGER = get_logger()

CAUGHT_MESSAGES = []

def singer_write_message(message):
    CAUGHT_MESSAGES.append(message)

def expected_record(fixture_row):
    expected_record = {}
    for k,v in fixture_row.items():
        expected_record[k.replace('"', '')] = v

    return expected_record

def do_not_dump_catalog(catalog):
    pass

tap_oracle.dump_catalog = do_not_dump_catalog

class FullTable(unittest.TestCase):
    maxDiff = None
    def setUp(self):
        with get_test_connection() as conn:
            cur = conn.cursor()
            table_spec = {"columns": [{"name": "id", "type" : "integer",       "primary_key" : True, "identity" : True},
                                      {"name": '"none_column"',                "type" : "integer"},
                                      {"name": "no_sync",                      "type" : "integer"},
                                      {"name": "bad_column",                   "type" : "long"},

                                      {"name" : '"size_number_4_0"',      "type" : "number(4,0)"},
                                      {"name" : '"size_number_*_0"',      "type" : "number(*,0)"},
                                      {"name" : '"size_number_10_-1"',    "type" : "number(10,-1)"},
                                      {"name" : '"size_number_integer"',  "type" : "integer"},
                                      {"name" : '"size_number_int"',      "type" : "int"},
                                      {"name" : '"size_number_smallint"', "type" : "smallint"},

                                      {"name" : '"our_number_10_2"',           "type" : "number(10,2)"},
                                      {"name" : '"our_number_38_4"',           "type" : "number(38,4)"},

                                      {"name" : '"our_float"',                 "type" : "float" },
                                      {"name" : '"our_double_precision"',      "type" : "double precision"},
                                      {"name" : '"our_real"',                  "type" : "real"},
                                      {"name" : '"our_nan"',                   "type" : "binary_float"},
                                      {"name" : '"our_+_infinity"',            "type" : "binary_float"},
                                      {"name" : '"our_-_infinity"',            "type" : "binary_float"},
                                      {"name" : '"our_binary_float"',          "type" : "binary_float"},
                                      {"name" : '"our_binary_double"',         "type" : "binary_double"},

                                      {"name" : '"our_date"',                   "type" : "DATE"},
                                      {"name" : '"our_ts"',                     "type" : "TIMESTAMP"},
                                      {"name" : '"our_ts_tz_edt"',              "type" : "TIMESTAMP WITH TIME ZONE"},
                                      {"name" : '"our_ts_tz_utc"',              "type" : "TIMESTAMP WITH TIME ZONE"},
                                      {"name" : '"our_ts_tz_local"',            "type" : "TIMESTAMP WITH LOCAL TIME ZONE"},

                                      {"name" : '"name-char-explicit-byte"',     "type": "char(250 byte)"},
                                      {"name" : '"name-char-explicit-char"',     "type": "char(250 char)"},
                                      {"name" : 'name_nchar',                   "type": "nchar(123)"},
                                      {"name" : '"name-nvarchar2"',              "type": "nvarchar2(234)"},

                                      {"name" : '"name-varchar-explicit-byte"',  "type": "varchar(250 byte)"},
                                      {"name" : '"name-varchar-explicit-char"',  "type": "varchar(251 char)"},

                                      {"name" : '"name-varchar2-explicit-byte"', "type": "varchar2(250 byte)"},
                                      {"name" : '"name-varchar2-explicit-char"', "type": "varchar2(251 char)"}
            ],
                          "name" : "CHICKEN"}
            ensure_test_table(table_spec)

    def test_catalog(self):
        singer.write_message = singer_write_message

        with get_test_connection() as conn:
            conn.autocommit = True


            catalog = tap_oracle.do_discovery(conn)
            chicken_stream = [s for s in catalog.streams if s.table == 'CHICKEN'][0]
            chicken_stream = select_all_of_stream(chicken_stream)

            #unselect the NO_SYNC column
            chicken_stream = unselect_column(chicken_stream, 'NO_SYNC')

            #select logminer
            chicken_stream = set_replication_method_for_stream(chicken_stream, 'LOG_BASED')
            cur = conn.cursor()

            rec_1 = {
                '"none_column"'         : None,
                '"our_number_10_2"'     : decimal.Decimal('100.11'),
                '"our_binary_float"'    : 1234567.8901234,
                '"our_date"'            : datetime.date(1996, 6, 6),
                '"name-char-explicit-byte"'    :'name-char-explicit-byte I',
                }

            insert_record(cur, 'CHICKEN', rec_1)
            rec_2 = copy.deepcopy(rec_1)
            rec_2.update({'"size_number_4_0"' : 101,
                          '"our_number_10_2"' : decimal.Decimal('101.11') + 1,
                          '"our_binary_float"' : 1234567.8901234 + 1,
                          '"our_date"' : datetime.date(1996, 6, 6) + datetime.timedelta(days=1)
            })

            insert_record(cur, 'CHICKEN', rec_2)

            original_state = {}
            #initial run should be full_table
            tap_oracle.do_sync(conn, catalog, None, original_state)

            #messages for initial full table replication: ActivateVersion, SchemaMessage, Record, Record, State, ActivateVersion
            self.assertEqual(7, len(CAUGHT_MESSAGES))
            self.assertTrue(isinstance(CAUGHT_MESSAGES[0], singer.SchemaMessage))
            self.assertTrue(isinstance(CAUGHT_MESSAGES[1], singer.StateMessage))
            self.assertTrue(isinstance(CAUGHT_MESSAGES[2], singer.ActivateVersionMessage))
            self.assertTrue(isinstance(CAUGHT_MESSAGES[3], singer.RecordMessage))
            self.assertTrue(isinstance(CAUGHT_MESSAGES[4], singer.RecordMessage))
            self.assertTrue(isinstance(CAUGHT_MESSAGES[5], singer.ActivateVersionMessage))
            self.assertTrue(isinstance(CAUGHT_MESSAGES[6], singer.StateMessage))

            state = CAUGHT_MESSAGES[6].value
            version = state.get('bookmarks', {}).get(chicken_stream.tap_stream_id, {}).get('version')
            scn = state.get('bookmarks', {}).get(chicken_stream.tap_stream_id, {}).get('scn')

            self.assertIsNotNone(version)
            self.assertIsNotNone(scn)
            self.assertEqual(CAUGHT_MESSAGES[2].version, version)
            self.assertEqual(CAUGHT_MESSAGES[5].version, version)

            #run another do_sync
            CAUGHT_MESSAGES.clear()
            rec_3 = copy.deepcopy(rec_2)
            rec_3.update({'"size_number_4_0"' : 102,
                          '"our_number_10_2"' : decimal.Decimal('101.11') + 3,
                          '"our_binary_float"' : 1234567.8901234 + 2,
                          '"our_date"' : datetime.date(1996, 6, 6) + datetime.timedelta(days=2)
            })

            insert_record(cur, 'CHICKEN', rec_3)

            #this sync should activate logminer because of the scn in state
            tap_oracle.do_sync(conn, catalog, None, state)

            #TODO: assert new scn
            self.assertEqual(3, len(CAUGHT_MESSAGES))

            self.assertTrue(isinstance(CAUGHT_MESSAGES[0], singer.SchemaMessage))
            self.assertTrue(isinstance(CAUGHT_MESSAGES[1], singer.RecordMessage))
            self.assertTrue(isinstance(CAUGHT_MESSAGES[2], singer.StateMessage))

            new_scn = CAUGHT_MESSAGES[2].value.get('bookmarks', {}).get(chicken_stream.tap_stream_id, {}).get('scn')
            new_version = CAUGHT_MESSAGES[2].value.get('bookmarks', {}).get(chicken_stream.tap_stream_id, {}).get('version')

            self.assertTrue(new_scn > scn)
            self.assertTrue(version == new_version)


if __name__== "__main__":
    test1 = FullTable()
    test1.setUp()
    test1.test_catalog()
