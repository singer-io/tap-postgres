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

class MineStrings(unittest.TestCase):
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



        table_spec = {"columns": [{"name" : "id",                            "type" : "integer", "primary_key" : True, "identity" : True},
                                  {"name" : '"name-char-explicit-byte"',     "type": "char(250 byte)"},
                                  {"name" : '"name-char-explicit-char"',     "type": "char(250 char)"},
                                  {"name" : 'name_nchar',                   "type": "nchar(123)"},
                                  {"name" : '"name-nvarchar2"',              "type": "nvarchar2(234)"},

                                  {"name" : '"name-varchar-explicit-byte"',  "type": "varchar(250 byte)"},
                                  {"name" : '"name-varchar-explicit-char"',  "type": "varchar(251 char)"},

                                  {"name" : '"name-varchar2-explicit-byte"', "type": "varchar2(250 byte)"},
                                  {"name" : '"name-varchar2-explicit-char"', "type": "varchar2(251 char)"}],
                      "name" : "CHICKEN"}

        ensure_test_table(table_spec)


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
                                           '"name-char-explicit-byte"':'name-char-explicit-byte I',
                                           '"name-char-explicit-char"':'name-char-explicit-char I',
                                           'name_nchar'               :  'name-nchar I',
                                           '"name-nvarchar2"'         : 'name-nvarchar2 I',
                                           '"name-varchar-explicit-byte"' : 'name-varchar-explicit-byte I',
                                           '"name-varchar-explicit-char"' : 'name-varchar-explicit-char I',
                                           '"name-varchar2-explicit-byte"': 'name-varchar2-explicit-byte I',
                                           '"name-varchar2-explicit-char"': 'name-varchar2-explicit-char I'
                                       }, lambda s: s.replace("I", "II"))

            post_scn = cur.execute("SELECT current_scn FROM V$DATABASE").fetchall()[0][0]
            LOGGER.info("post SCN: {}".format(post_scn))

            state = write_bookmark({}, chicken_stream.tap_stream_id, 'scn', prev_scn)
            state = write_bookmark(state, chicken_stream.tap_stream_id, 'version', 1)
            tap_oracle.do_sync(conn, catalog, None, state)

            verify_crud_messages(self, CAUGHT_MESSAGES, ['ID'])

            #verify message 1 - first insert
            insert_rec_1 = CAUGHT_MESSAGES[1].record
            self.assertIsNotNone(insert_rec_1.get('scn'))
            insert_rec_1.pop('scn')
            self.assertEqual(insert_rec_1, {'name-varchar2-explicit-byte': 'name-varchar2-explicit-byte I',
                                    'name-char-explicit-char': 'name-char-explicit-char I                                                                                                                                                                                                                                 ',
                                    'name-nvarchar2': 'name-nvarchar2 I', 'name-varchar-explicit-char': 'name-varchar-explicit-char I',
                                    'name-varchar2-explicit-char': 'name-varchar2-explicit-char I',
                                    'NAME_NCHAR': 'name-nchar I                                                                                                               ',
                                    'name-char-explicit-byte': 'name-char-explicit-byte I                                                                                                                                                                                                                                 ',
                                    '_sdc_deleted_at': None, 'name-varchar-explicit-byte': 'name-varchar-explicit-byte I', 'ID': 1})


            #verify UPDATE
            update_rec = CAUGHT_MESSAGES[5].record
            self.assertIsNotNone(update_rec.get('scn'))
            update_rec.pop('scn')
            self.assertEqual(update_rec, {'name-varchar2-explicit-byte': 'name-varchar2-explicit-byte II',
                                    'name-char-explicit-char': 'name-char-explicit-char II                                                                                                                                                                                                                                ',
                                    'name-nvarchar2': 'name-nvarchar2 II', 'name-varchar-explicit-char': 'name-varchar-explicit-char II',
                                    'name-varchar2-explicit-char': 'name-varchar2-explicit-char II',
                                    'NAME_NCHAR': 'name-nchar II                                                                                                              ',
                                    'name-char-explicit-byte': 'name-char-explicit-byte II                                                                                                                                                                                                                                ',
                                    '_sdc_deleted_at': None, 'name-varchar-explicit-byte': 'name-varchar-explicit-byte II', 'ID': 1})


            #verify first DELETE message
            delete_rec = CAUGHT_MESSAGES[9].record
            self.assertIsNotNone(delete_rec.get('scn'))
            self.assertIsNotNone(delete_rec.get('_sdc_deleted_at'))
            delete_rec.pop('scn')
            delete_rec.pop('_sdc_deleted_at')
            self.assertEqual(delete_rec, {'name-varchar2-explicit-byte': 'name-varchar2-explicit-byte II',
                                                 'name-char-explicit-char': 'name-char-explicit-char II                                                                                                                                                                                                                                ',
                                                 'name-nvarchar2': 'name-nvarchar2 II', 'name-varchar-explicit-char': 'name-varchar-explicit-char II',
                                                 'name-varchar2-explicit-char': 'name-varchar2-explicit-char II',
                                                 'NAME_NCHAR': 'name-nchar II                                                                                                              ',
                                                 'name-char-explicit-byte': 'name-char-explicit-byte II                                                                                                                                                                                                                                ',
                                                 'name-varchar-explicit-byte': 'name-varchar-explicit-byte II', 'ID': 1})


            #verify second DELETE message
            delete_rec_2 = CAUGHT_MESSAGES[11].record
            self.assertIsNotNone(delete_rec_2.get('scn'))
            self.assertIsNotNone(delete_rec_2.get('_sdc_deleted_at'))
            delete_rec_2.pop('scn')
            delete_rec_2.pop('_sdc_deleted_at')
            self.assertEqual(delete_rec_2, {'name-varchar2-explicit-byte': 'name-varchar2-explicit-byte II',
                                                 'name-char-explicit-char': 'name-char-explicit-char II                                                                                                                                                                                                                                ',
                                                 'name-nvarchar2': 'name-nvarchar2 II', 'name-varchar-explicit-char': 'name-varchar-explicit-char II',
                                                 'name-varchar2-explicit-char': 'name-varchar2-explicit-char II',
                                                 'NAME_NCHAR': 'name-nchar II                                                                                                              ',
                                                 'name-char-explicit-byte': 'name-char-explicit-byte II                                                                                                                                                                                                                                ',
                                                 'name-varchar-explicit-byte': 'name-varchar-explicit-byte II', 'ID': 2})




if __name__== "__main__":
    test1 = MineStrings()
    test1.setUp()
    test1.test_catalog()
