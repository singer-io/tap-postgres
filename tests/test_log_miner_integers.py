import unittest
import os
import cx_Oracle, sys, string, datetime
import tap_oracle
import pdb
import singer
from singer import get_logger, metadata, write_bookmark
from tests.utils import get_test_connection, ensure_test_table, select_all_of_stream, set_replication_method_for_stream, crud_up_log_miner_fixtures, verify_crud_messages
import tap_oracle.sync_strategies.log_miner as log_miner

LOGGER = get_logger()

CAUGHT_MESSAGES = []

def singer_write_message(message):
    CAUGHT_MESSAGES.append(message)

def do_not_dump_catalog(catalog):
    pass

tap_oracle.dump_catalog = do_not_dump_catalog

class MineInts(unittest.TestCase):
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


        table_spec = {"columns": [{"name" :  "size_pk",               "type" : "number(4,0)", "primary_key" : True, "identity" : True},
                                  {"name" : '"size_none"',            "type" : "number(4,0)"},
                                  {"name" : '"size_number_4_0"',      "type" : "number(4,0)"},
                                  {"name" : '"size_number_*_0"',      "type" : "number(*,0)"},
                                  {"name" : '"size_number_10_-1"',    "type" : "number(10,-1)"},
                                  {"name" : '"size_number_integer"',  "type" : "integer"},
                                  {"name" : '"size_number_int"',      "type" : "int"},
                                  {"name" : '"size_number_smallint"', "type" : "smallint"}],
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

            crud_up_log_miner_fixtures(cur, 'CHICKEN',
                                       {
                                           '"size_none"': None,
                                           '"size_number_4_0"': 100,
                                           '"size_number_*_0"' : 200,
                                           '"size_number_10_-1"': 311,
                                           '"size_number_integer"': 400,
                                           '"size_number_int"':  500,
                                           '"size_number_smallint"': 50000
                                       }, self.update_add_5)

            post_scn = cur.execute("SELECT current_scn FROM V$DATABASE").fetchall()[0][0]
            LOGGER.info("post SCN: {}".format(post_scn))

            state = write_bookmark({}, chicken_stream.tap_stream_id, 'scn', prev_scn)
            state = write_bookmark(state, chicken_stream.tap_stream_id, 'version', 1)
            tap_oracle.do_sync(conn, catalog, None, state)

            verify_crud_messages(self, CAUGHT_MESSAGES, ['SIZE_PK'])

            #verify message 1 - first insert
            insert_rec_1 = CAUGHT_MESSAGES[1].record
            self.assertIsNotNone(insert_rec_1.get('scn'))
            insert_rec_1.pop('scn')
            self.assertEqual(insert_rec_1, {
                                           'SIZE_PK': 1,
                                           '_sdc_deleted_at': None,
                                           'size_none': None,
                                           'size_number_4_0': 100,
                                           'size_number_*_0' : 200,
                                           'size_number_10_-1': 310,
                                           'size_number_integer': 400,
                                           'size_number_int':  500,
                                           'size_number_smallint': 50000})


            #verify UPDATE
            update_rec = CAUGHT_MESSAGES[5].record
            self.assertIsNotNone(update_rec.get('scn'))
            update_rec.pop('scn')
            self.assertEqual(update_rec, {
                                           'SIZE_PK': 1,
                                           'size_none': None,
                                           '_sdc_deleted_at': None,
                                           'size_number_4_0': 105,
                                           'size_number_*_0' : 205,
                                           'size_number_10_-1': 320,
                                           'size_number_integer': 405,
                                           'size_number_int':  505,
                                           'size_number_smallint': 50005})


            #verify first DELETE message
            delete_rec = CAUGHT_MESSAGES[9].record
            self.assertIsNotNone(delete_rec.get('scn'))
            self.assertIsNotNone(delete_rec.get('_sdc_deleted_at'))
            delete_rec.pop('scn')
            delete_rec.pop('_sdc_deleted_at')
            self.assertEqual(delete_rec, {
                                          'SIZE_PK': 1,
                                          'size_none': None,
                                          'size_number_4_0': 105,
                                          'size_number_*_0' : 205,
                                          'size_number_10_-1': 320,
                                          'size_number_integer': 405,
                                          'size_number_int':  505,
                                          'size_number_smallint': 50005})


            #verify second DELETE message
            delete_rec_2 = CAUGHT_MESSAGES[11].record
            self.assertIsNotNone(delete_rec_2.get('scn'))
            self.assertIsNotNone(delete_rec_2.get('_sdc_deleted_at'))
            delete_rec_2.pop('scn')
            delete_rec_2.pop('_sdc_deleted_at')
            self.assertEqual(delete_rec_2, {
                                           'SIZE_PK': 2,
                                           'size_none': None,
                                           'size_number_4_0': 105,
                                           'size_number_*_0' : 205,
                                           'size_number_10_-1': 320,
                                           'size_number_integer': 405,
                                           'size_number_int':  505,
                                           'size_number_smallint': 50005})




if __name__== "__main__":
    test1 = MineInts()
    test1.setUp()
    test1.test_catalog()
