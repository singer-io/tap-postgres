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
import pytz
import strict_rfc3339

LOGGER = get_logger()

CAUGHT_MESSAGES = []

def do_not_dump_catalog(catalog):
    pass

tap_oracle.dump_catalog = do_not_dump_catalog

def singer_write_message(message):
    CAUGHT_MESSAGES.append(message)

class MineDates(unittest.TestCase):
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

            table_spec = {"columns": [{"name": "id", "type" : "integer",       "primary_key" : True, "identity" : True},
                                      {"name" : '"our_date"',                   "type" : "DATE"},
                                      {"name" : '"our_ts"',                     "type" : "TIMESTAMP"},
                                      {"name" : '"our_ts_tz_edt"',              "type" : "TIMESTAMP WITH TIME ZONE"},
                                      {"name" : '"our_ts_tz_utc"',              "type" : "TIMESTAMP WITH TIME ZONE"},
                                      {"name" : '"our_ts_tz_local"',            "type" : "TIMESTAMP WITH LOCAL TIME ZONE"}
            ],
                          "name" : "CHICKEN"}
            ensure_test_table(table_spec)

    def update_add_1_day(self, v):
        if v is not None:
            return v + datetime.timedelta(days=1)
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

            our_date = datetime.date(1996, 6, 6)
            our_ts    = datetime.datetime(1997, 2, 2, 2, 2, 2, 722184)
            nyc_tz = pytz.timezone('America/New_York')
            our_ts_tz_edt = nyc_tz.localize(datetime.datetime(1997, 3, 3, 3, 3, 3, 722184))
            our_ts_tz_utc = datetime.datetime(1997, 3, 3, 3, 3, 3, 722184, pytz.UTC)
            auckland_tz = pytz.timezone('Pacific/Auckland')
            our_ts_local  = auckland_tz.localize(datetime.datetime(1997, 3, 3, 18, 3, 3, 722184))

            crud_up_log_miner_fixtures(cur, 'CHICKEN', {
                '"our_date"'           :  our_date,
                '"our_ts"'             :  our_ts,
                '"our_ts_tz_edt"'      :  our_ts_tz_edt,
                '"our_ts_tz_utc"'      :  our_ts_tz_utc,
                '"our_ts_tz_local"'    :  our_ts_local
            }, self.update_add_1_day)



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
            self.assertIsNone(insert_rec_1.get('_sdc_deleted_at'))
            self.assertEqual(insert_rec_1.get('ID'), 1)
            insert_rec_1.pop('_sdc_deleted_at')
            insert_rec_1.pop('ID')

            self.assertEqual(insert_rec_1, {'our_ts': '1997-02-02T02:02:02.722184+00:00',
                                            'our_ts_tz_edt': '1997-03-03T03:03:03.722184-05:00',
                                            'our_ts_tz_utc': '1997-03-03T03:03:03.722184+00:00',
                                            'our_date': '1996-06-06T00:00:00.00+00:00',
                                            'our_ts_tz_local': '1997-03-03T05:03:03.722184+00:00'
                                            })


            for v in insert_rec_1.values():
                self.assertTrue(strict_rfc3339.validate_rfc3339(v))


            #verify UPDATE
            update_rec = CAUGHT_MESSAGES[5].record
            self.assertIsNotNone(update_rec.get('scn'))
            update_rec.pop('scn')

            self.assertEqual(update_rec,
                             {'our_ts': '1997-02-03T02:02:02.722184+00:00',
                              'our_ts_tz_edt': '1997-03-04T03:03:03.722184-05:00',
                              'our_ts_tz_utc': '1997-03-04T03:03:03.722184+00:00',
                              'our_date': '1996-06-07T00:00:00.00+00:00',
                              '_sdc_deleted_at': None,
                              'our_ts_tz_local': '1997-03-04T05:03:03.722184+00:00',
                              'ID' : 1})


            #verify first DELETE message
            delete_rec = CAUGHT_MESSAGES[9].record
            self.assertIsNotNone(delete_rec.get('scn'))
            self.assertIsNotNone(delete_rec.get('_sdc_deleted_at'))
            delete_rec.pop('scn')
            delete_rec.pop('_sdc_deleted_at')
            self.assertEqual(delete_rec,
                             {'our_ts': '1997-02-03T02:02:02.722184+00:00',
                              'our_ts_tz_edt': '1997-03-04T03:03:03.722184-05:00',
                              'our_ts_tz_utc': '1997-03-04T03:03:03.722184+00:00',
                              'our_date': '1996-06-07T00:00:00.00+00:00',
                              'our_ts_tz_local': '1997-03-04T05:03:03.722184+00:00',
                              'ID': 1})


            #verify second DELETE message
            delete_rec_2 = CAUGHT_MESSAGES[11].record

            self.assertIsNotNone(delete_rec_2.get('scn'))
            self.assertIsNotNone(delete_rec_2.get('_sdc_deleted_at'))
            delete_rec_2.pop('scn')
            delete_rec_2.pop('_sdc_deleted_at')
            self.assertEqual(delete_rec_2,
                             {'our_ts': '1997-02-03T02:02:02.722184+00:00',
                              'our_ts_tz_edt': '1997-03-04T03:03:03.722184-05:00',
                              'our_ts_tz_utc': '1997-03-04T03:03:03.722184+00:00',
                              'our_date': '1996-06-07T00:00:00.00+00:00',
                              'our_ts_tz_local': '1997-03-04T05:03:03.722184+00:00',
                              'ID': 2})


if __name__== "__main__":
    test1 = MineDates()
    test1.setUp()
    test1.test_catalog()
