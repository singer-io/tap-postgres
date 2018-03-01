import unittest
import os
import cx_Oracle, sys, string, datetime
import tap_oracle
import pdb
import singer
from singer import get_logger, metadata, write_bookmark
from tests.utils import get_test_connection, ensure_test_table, select_all_of_stream, set_replication_method_for_stream, crud_up_log_miner_fixtures, verify_crud_messages
import tap_oracle.sync_strategies.log_miner as log_miner
import decimal
import math

LOGGER = get_logger()

CAUGHT_MESSAGES = []

def singer_write_message(message):
    CAUGHT_MESSAGES.append(message)


def do_not_dump_catalog(catalog):
    pass

tap_oracle.dump_catalog = do_not_dump_catalog

class MineFloats(unittest.TestCase):
    maxDiff = None
    def setUp(self):
        with get_test_connection() as conn:
            cur = conn.cursor()
            cur.execute("""
                begin
                    rdsadmin.rdsadmin_util.set_configuration(
                        name  => 'archivelog retention hours',
                        value => '24');
                end;
            """)

            cur.execute("""
                begin
                   rdsadmin.rdsadmin_util.alter_supplemental_logging(
                      p_action => 'ADD');
                end;
            """)

            result = cur.execute("select log_mode from v$database").fetchall()
            self.assertEqual(result[0][0], "ARCHIVELOG")


        table_spec = {"columns": [{"name" : '"our_float"',                "type" : "float", "primary_key": True, "identity": True },
                                  {"name" : '"our_double_precision"',      "type" : "double precision"},
                                  {"name" : '"our_real"',                  "type" : "real"},

                                  {"name" : '"our_nan"',                   "type" : "binary_float"},
                                  {"name" : '"our_+_infinity"',            "type" : "binary_float"},
                                  {"name" : '"our_-_infinity"',            "type" : "binary_float"},

                                  #only ones that support NaN/+Inf/-Inf
                                  {"name" : '"our_binary_float"',          "type" : "binary_float"},
                                  {"name" : '"our_binary_double"',         "type" : "binary_double"}],
                     "name" : "CHICKEN"}

        ensure_test_table(table_spec)


    def update_add_5(self, v):
        if v is not None:
            return v + 5
        else:
            return None

    def test_catalog(self):

        singer.write_message = singer_write_message
        log_miner.UPDATE_BOOKMARK_PERIOD = 1

        with get_test_connection() as conn:
            conn.autocommit = True
            catalog = tap_oracle.do_discovery(conn)
            chicken_stream = [s for s in catalog.streams if s.table == 'CHICKEN'][0]
            chicken_stream = select_all_of_stream(chicken_stream)

            chicken_stream = set_replication_method_for_stream(chicken_stream, 'LOG_BASED')

            cur = conn.cursor()
            prev_scn = cur.execute("SELECT current_scn FROM V$DATABASE").fetchall()[0][0]
            our_fake_float = decimal.Decimal('1234567.8901234')
            our_real_float = 1234567.8901234
            crud_up_log_miner_fixtures(cur, 'CHICKEN', {
                '"our_double_precision"': our_fake_float,
                '"our_real"':         our_fake_float,
                '"our_binary_float"': our_real_float,
                '"our_binary_double"': our_real_float,
                '"our_nan"': float('nan'),
                '"our_+_infinity"': float('+inf'),
                '"our_-_infinity"': float('-inf')
            }, self.update_add_5)

            post_scn = cur.execute("SELECT current_scn FROM V$DATABASE").fetchall()[0][0]
            LOGGER.info("post SCN: {}".format(post_scn))

            state = write_bookmark({}, chicken_stream.tap_stream_id, 'scn', prev_scn)
            state = write_bookmark(state, chicken_stream.tap_stream_id, 'version', 1)
            tap_oracle.do_sync(conn, catalog, None, state)

            verify_crud_messages(self, CAUGHT_MESSAGES, ['our_float'])

            #verify message 1 - first insert
            insert_rec_1 = CAUGHT_MESSAGES[1].record
            self.assertIsNotNone(insert_rec_1.get('scn'))
            insert_rec_1.pop('scn')


            self.assertEqual(float('+inf'), insert_rec_1.get('our_+_infinity'))
            insert_rec_1.pop('our_+_infinity')
            self.assertEqual(float('-inf'), insert_rec_1.get('our_-_infinity'))
            insert_rec_1.pop('our_-_infinity')

            self.assertTrue(math.isnan(insert_rec_1.get('our_nan')))
            insert_rec_1.pop('our_nan')

            self.assertEqual(insert_rec_1, {
                                 'our_float': decimal.Decimal('1.0'),
                                 'our_double_precision': our_fake_float,
                                 'our_real': our_fake_float,
                                 'our_binary_float': 1234567.88, #weird
                                 'our_binary_double': 1234567.890123,
                                 '_sdc_deleted_at': None})

            #verify UPDATE
            update_rec = CAUGHT_MESSAGES[5].record
            self.assertIsNotNone(update_rec.get('scn'))
            update_rec.pop('scn')

            self.assertEqual(float('+inf'), update_rec.get('our_+_infinity'))
            update_rec.pop('our_+_infinity')
            self.assertEqual(float('-inf'), update_rec.get('our_-_infinity'))
            update_rec.pop('our_-_infinity')

            self.assertTrue(math.isnan(update_rec.get('our_nan')))
            update_rec.pop('our_nan')

            self.assertEqual(update_rec, {'our_binary_double': 1234572.890123,
                                          '_sdc_deleted_at': None,
                                          'our_binary_float': 1234572.88,
                                          'our_float': decimal.Decimal('1'),
                                          'our_double_precision': decimal.Decimal('1234572.8901234'),
                                          'our_real': decimal.Decimal('1234572.8901234')})

            #verify first DELETE message
            delete_rec = CAUGHT_MESSAGES[9].record

            self.assertEqual(float('+inf'), delete_rec.get('our_+_infinity'))
            delete_rec.pop('our_+_infinity')
            self.assertEqual(float('-inf'), delete_rec.get('our_-_infinity'))
            delete_rec.pop('our_-_infinity')

            self.assertTrue(math.isnan(delete_rec.get('our_nan')))
            delete_rec.pop('our_nan')

            self.assertIsNotNone(delete_rec.get('scn'))
            self.assertIsNotNone(delete_rec.get('_sdc_deleted_at'))
            delete_rec.pop('scn')
            delete_rec.pop('_sdc_deleted_at')

            self.assertEqual(delete_rec,
                             {'our_binary_double': 1234572.890123,
                              'our_binary_float': 1234572.88,
                              'our_float': decimal.Decimal('1'),
                              'our_double_precision': decimal.Decimal('1234572.8901234'),
                              'our_real': decimal.Decimal('1234572.8901234')})

            #verify second DELETE message
            delete_rec_2 = CAUGHT_MESSAGES[11].record

            self.assertEqual(float('+inf'), delete_rec_2.get('our_+_infinity'))
            delete_rec_2.pop('our_+_infinity')
            self.assertEqual(float('-inf'), delete_rec_2.get('our_-_infinity'))
            delete_rec_2.pop('our_-_infinity')

            self.assertTrue(math.isnan(delete_rec_2.get('our_nan')))
            delete_rec_2.pop('our_nan')


            self.assertIsNotNone(delete_rec_2.get('scn'))
            self.assertIsNotNone(delete_rec_2.get('_sdc_deleted_at'))
            delete_rec_2.pop('scn')
            delete_rec_2.pop('_sdc_deleted_at')

            self.assertEqual(delete_rec_2,
                             {'our_binary_double': 1234572.890123,
                              'our_binary_float': 1234572.88,
                              'our_float': decimal.Decimal('2'),
                              'our_double_precision': decimal.Decimal('1234572.8901234'),
                              'our_real': decimal.Decimal('1234572.8901234')})




if __name__== "__main__":
    test1 = MineFloats()
    test1.setUp()
    test1.test_catalog()
