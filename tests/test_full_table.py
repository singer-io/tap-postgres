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
            chicken_stream = set_replication_method_for_stream(chicken_stream, 'FULL_TABLE')
            cur = conn.cursor()

            our_date = datetime.date(1996, 6, 6)
            our_ts    = datetime.datetime(1997, 2, 2, 2, 2, 2, 722184)
            nyc_tz = pytz.timezone('America/New_York')
            our_ts_tz_edt = nyc_tz.localize(datetime.datetime(1997, 3, 3, 3, 3, 3, 722184))
            our_ts_tz_utc = datetime.datetime(1997, 3, 3, 3, 3, 3, 722184, pytz.UTC)
            auckland_tz = pytz.timezone('Pacific/Auckland')
            our_ts_local  = auckland_tz.localize(datetime.datetime(1997, 3, 3, 18, 3, 3, 722184))
            our_float = decimal.Decimal('1234567.890123456789012345678901234567890123456789')
            our_real = our_float
            our_double_precision = our_float

            rec_1 = {
                '"none_column"'         : None,
                'NO_SYNC'               : 666, #should not sync this column
                '"size_number_4_0"'     : 100,
                '"size_number_*_0"'     : 200,
                '"size_number_10_-1"'   : 311,
                '"size_number_integer"' : 400,
                '"size_number_int"'     : 500,
                '"size_number_smallint"': 50000,

                '"our_number_10_2"'     : decimal.Decimal('100.11'),
                '"our_number_38_4"'     : decimal.Decimal('99999999999999999.99991'),

                '"our_double_precision"': our_double_precision,
                '"our_real"'            : our_real,
                '"our_float"'           : our_float,

                '"our_binary_float"'    : 1234567.8901234,
                '"our_binary_double"'   : 1234567.8901234,
                '"our_nan"'             : float('nan'),
                '"our_+_infinity"'      : float('+inf'),
                '"our_-_infinity"'      : float('-inf'),

                '"our_date"'            :  our_date,
                '"our_ts"'              :  our_ts,
                '"our_ts_tz_edt"'       :  our_ts_tz_edt,
                '"our_ts_tz_utc"'       :  our_ts_tz_utc,
                '"our_ts_tz_local"'     :  our_ts_local,

                '"name-char-explicit-byte"'    :'name-char-explicit-byte I',
                '"name-char-explicit-char"'    :'name-char-explicit-char I',
                'NAME_NCHAR'                   : 'name-nchar I',
                '"name-nvarchar2"'             : 'name-nvarchar2 I',
                '"name-varchar-explicit-byte"' : 'name-varchar-explicit-byte I',
                '"name-varchar-explicit-char"' : 'name-varchar-explicit-char I',
                '"name-varchar2-explicit-byte"': 'name-varchar2-explicit-byte I',
                '"name-varchar2-explicit-char"': 'name-varchar2-explicit-char I'
                }

            insert_record(cur, 'CHICKEN', rec_1)
            rec_2 = copy.deepcopy(rec_1)
            rec_2.update({'"size_number_4_0"' : 101,
                          '"our_number_10_2"' : decimal.Decimal('101.11') + 1,
                          '"our_double_precision"' : our_double_precision + 1,
                          '"our_date"' : our_date + datetime.timedelta(days=1),
                          'NAME_NCHAR' :  'name-nchar II'})

            insert_record(cur, 'CHICKEN', rec_2)

            state = {}
            tap_oracle.do_sync(conn, catalog, None, state)

            #messages: ActivateVersion, SchemaMessage, Record, Record, State, ActivateVersion
            self.assertEqual(7, len(CAUGHT_MESSAGES))
            self.assertTrue(isinstance(CAUGHT_MESSAGES[0], singer.SchemaMessage))
            self.assertTrue(isinstance(CAUGHT_MESSAGES[1], singer.StateMessage))
            self.assertTrue(isinstance(CAUGHT_MESSAGES[2], singer.ActivateVersionMessage))
            self.assertTrue(isinstance(CAUGHT_MESSAGES[3], singer.RecordMessage))
            self.assertTrue(isinstance(CAUGHT_MESSAGES[4], singer.RecordMessage))
            self.assertTrue(isinstance(CAUGHT_MESSAGES[5], singer.ActivateVersionMessage))
            self.assertTrue(isinstance(CAUGHT_MESSAGES[6], singer.StateMessage))

            state = CAUGHT_MESSAGES[1].value
            version = state.get('bookmarks', {}).get(chicken_stream.tap_stream_id, {}).get('version')

            self.assertIsNotNone(version)
            self.assertEqual(CAUGHT_MESSAGES[2].version, version)
            self.assertEqual(CAUGHT_MESSAGES[5].version, version)
            edt = pytz.timezone('America/New_York')

            expected_rec_1 = {'ID'                  : 1,
                              'none_column'         : None,
                              'size_number_4_0'     : 100,
                              'size_number_*_0'     : 200,
                              'size_number_10_-1'   : 310,
                              'size_number_integer' : 400,
                              'size_number_int'     : 500,
                              'size_number_smallint': 50000,

                              'our_number_10_2'     : decimal.Decimal('100.11'),
                              'our_number_38_4'     : decimal.Decimal('99999999999999999.9999'),

                              'our_double_precision': decimal.Decimal('1234567.8901234567890123456789012345679'),
                              'our_float'           : decimal.Decimal('1234567.8901234567890123456789012345679'),
                              'our_real'            : decimal.Decimal('1234567.890123456789'),



                              'our_binary_float'    : 1234567.875,
                              'our_binary_double'   : 1234567.890123,
                              'our_+_infinity'      : float('+inf'),
                              'our_-_infinity'      : float('-inf'),

                              'our_date'            : '1996-06-06T00:00:00.00+00:00',
                              'our_ts'              : '1997-02-02T02:02:02.722184+00:00',
                              'our_ts_tz_edt'       : '1997-03-03T03:03:03.722184-05:00',
                              'our_ts_tz_utc'       : '1997-03-03T03:03:03.722184+00:00',
                              'our_ts_tz_local'     : '1997-03-03T00:03:03.722184+00:00',

                              'name-char-explicit-byte'    :'name-char-explicit-byte I                                                                                                                                                                                                                                 ',
                              'name-char-explicit-char'    :'name-char-explicit-char I                                                                                                                                                                                                                                 ',
                              'NAME_NCHAR'                 : 'name-nchar I                                                                                                               ',
                              'name-nvarchar2'             : 'name-nvarchar2 I',
                              'name-varchar-explicit-byte' : 'name-varchar-explicit-byte I',
                              'name-varchar-explicit-char' : 'name-varchar-explicit-char I',
                              'name-varchar2-explicit-byte': 'name-varchar2-explicit-byte I',
                              'name-varchar2-explicit-char': 'name-varchar2-explicit-char I'
                }

            self.assertTrue(math.isnan(CAUGHT_MESSAGES[3].record.get('our_nan')))
            CAUGHT_MESSAGES[3].record.pop('our_nan')

            self.assertEqual(CAUGHT_MESSAGES[3].record, expected_rec_1)

            expected_rec_2 = expected_rec_1
            expected_rec_2.update({
                'ID': decimal.Decimal(2),
                'size_number_4_0' : decimal.Decimal('101'),
                'our_number_10_2' : decimal.Decimal('101.11') + 1,
                'our_double_precision' : our_double_precision + 1,
                'our_date' : '1996-06-07T00:00:00.00+00:00',
                'NAME_NCHAR' :  'name-nchar II                                                                                                              '})


            self.assertTrue(math.isnan(CAUGHT_MESSAGES[4].record.get('our_nan')))
            CAUGHT_MESSAGES[4].record.pop('our_nan')

            self.assertEqual(CAUGHT_MESSAGES[4].record, expected_rec_2)

            #run another do_sync
            CAUGHT_MESSAGES.clear()
            tap_oracle.do_sync(conn, catalog, None, state)

            self.assertEqual(6, len(CAUGHT_MESSAGES))
            self.assertTrue(isinstance(CAUGHT_MESSAGES[0], singer.SchemaMessage))
            self.assertTrue(isinstance(CAUGHT_MESSAGES[1], singer.StateMessage))
            self.assertTrue(isinstance(CAUGHT_MESSAGES[2], singer.RecordMessage))
            self.assertTrue(isinstance(CAUGHT_MESSAGES[3], singer.RecordMessage))
            self.assertTrue(isinstance(CAUGHT_MESSAGES[4], singer.ActivateVersionMessage))
            self.assertTrue(isinstance(CAUGHT_MESSAGES[5], singer.StateMessage))

            nascent_version = CAUGHT_MESSAGES[1].value.get('bookmarks', {}).get(chicken_stream.tap_stream_id, {}).get('version')
            self.assertTrue( nascent_version > version)


if __name__== "__main__":
    test1 = FullTable()
    test1.setUp()
    test1.test_catalog()
