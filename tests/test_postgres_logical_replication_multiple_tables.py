import os
import unittest

import psycopg2.extras
from psycopg2.extensions import quote_ident
from tap_tester.scenario import (SCENARIOS)
import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner

import db_utils  # pylint: disable=import-error


expected_schemas = {'postgres_logical_replication_test_cows':
                    {'type': 'object',
                     'selected': True,
                     'properties': {'cow_name': {'selected': True, 'type': ['null', 'string'], 'inclusion': 'available'},
                                    'id': {'maximum': 2147483647, 'inclusion': 'automatic', 'type': ['integer'], 'minimum': -2147483648, 'selected': True},
                                    'cow_age': {'selected': True, 'type': ['null', 'integer'], 'inclusion': 'available'}}},

                    'postgres_logical_replication_test_chickens':
                    {'type': 'object',
                     'selected': True,
                     'properties': {'cow_name': {'selected': True, 'type': ['null', 'string'], 'inclusion': 'available'},
                                    'id': {'maximum': 2147483647, 'inclusion': 'automatic', 'type': ['integer'], 'minimum': -2147483648, 'selected': True},
                                    'cow_age': {'selected': True, 'type': ['null', 'integer'], 'inclusion': 'available'}}}}


def insert_record(cursor, table_name, data):
    our_keys = list(data.keys())
    our_keys.sort()
    our_values = [data.get(key) for key in our_keys]

    columns_sql = ", \n ".join(our_keys)
    value_sql = ",".join(["%s" for i in range(len(our_keys))])

    insert_sql = """ INSERT INTO {}
                            ( {} )
                     VALUES ( {} )""".format(quote_ident(table_name, cursor), columns_sql, value_sql)
    cursor.execute(insert_sql, our_values)

test_schema_name = "public"
test_table_name_cows = "postgres_logical_replication_test_cows"
test_table_name_chickens = "postgres_logical_replication_test_chickens"

def canonicalized_table_name(schema, table, cur):
    return "{}.{}".format(quote_ident(schema, cur), quote_ident(table, cur))


class PostgresLogicalRepMultipleTables(unittest.TestCase):
    def tearDown(self):
        with db_utils.get_test_connection('dev') as conn:
            conn.autocommit = True
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute(""" SELECT pg_drop_replication_slot('stitch') """)

    def setUp(self):
        db_utils.ensure_environment_variables_set()

        db_utils.ensure_db("dev")

        self.maxDiff = None

        with db_utils.get_test_connection('dev') as conn:
            conn.autocommit = True
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute(""" SELECT EXISTS (SELECT 1
                                                FROM  pg_replication_slots
                                               WHERE  slot_name = 'stitch') """)
                old_slot = cur.fetchone()[0]
                with db_utils.get_test_connection('dev', True) as conn2:
                    with conn2.cursor() as cur2:
                        if old_slot:
                            cur2.drop_replication_slot("stitch")
                        cur2.create_replication_slot('stitch', output_plugin='wal2json')

                for t in [test_table_name_cows, test_table_name_chickens]:
                    old_table = cur.execute("""SELECT EXISTS (
                                          SELECT 1
                                          FROM  information_schema.tables
                                          WHERE  table_schema = %s
                                          AND  table_name =   %s);""",
                                            [test_schema_name, t])
                    old_table = cur.fetchone()[0]

                    if old_table:
                        cur.execute("DROP TABLE {}".format(canonicalized_table_name(test_schema_name, t, cur)))


                cur = conn.cursor()
                create_table_sql = """
                CREATE TABLE {} (id            SERIAL PRIMARY KEY,
                                cow_age        integer,
                                cow_name       varchar)
                """.format(canonicalized_table_name(test_schema_name, test_table_name_cows, cur))
                cur.execute(create_table_sql)

                create_table_sql = """
                CREATE TABLE {} (id            SERIAL PRIMARY KEY,
                                chicken_age        integer,
                                chicken_name       varchar)
                """.format(canonicalized_table_name(test_schema_name, test_table_name_chickens, cur))
                cur.execute(create_table_sql)

                #insert a cow
                self.cows_rec_1 = {'cow_name' : "anne_cow", 'cow_age' : 30}
                insert_record(cur, test_table_name_cows, self.cows_rec_1)

                #insert a chicken
                self.chickens_rec_1 = {'chicken_name' : "alfred_chicken", 'chicken_age' : 4}
                insert_record(cur, test_table_name_chickens, self.chickens_rec_1)

    @staticmethod
    def expected_check_streams():
        return { 'dev-public-postgres_logical_replication_test_cows', 'dev-public-postgres_logical_replication_test_chickens'}

    @staticmethod
    def expected_sync_streams():
        return { 'postgres_logical_replication_test_cows', 'postgres_logical_replication_test_chickens' }

    @staticmethod
    def expected_pks():
        return {
            'postgres_logical_replication_test_cows' : {'id'},
            'postgres_logical_replication_test_chickens' : {'id'}
        }

    @staticmethod
    def tap_name():
        return "tap-postgres"

    @staticmethod
    def name():
        return "tap_tester_postgres_logical_multiple_tables"

    @staticmethod
    def get_type():
        return "platform.postgres"

    @staticmethod
    def get_credentials():
        return {'password': os.getenv('TAP_POSTGRES_PASSWORD')}

    @staticmethod
    def get_properties():
        return {'host' : os.getenv('TAP_POSTGRES_HOST'),
                'dbname' : os.getenv('TAP_POSTGRES_DBNAME'),
                'port' : os.getenv('TAP_POSTGRES_PORT'),
                'user' : os.getenv('TAP_POSTGRES_USER'),
                'default_replication_method' : 'LOG_BASED',
                'logical_poll_total_seconds': '10'
        }


    def test_run(self):
        conn_id = connections.ensure_connection(self)

        # run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        # verify check  exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        # verify the tap discovered the right streams
        found_catalogs = [fc for fc
                          in menagerie.get_catalogs(conn_id)
                          if fc['tap_stream_id'] in self.expected_check_streams()]


        self.assertGreaterEqual(len(found_catalogs),
                                2,
                                msg="unable to locate schemas for connection {}".format(conn_id))

        found_catalog_names = set(map(lambda c: c['tap_stream_id'], found_catalogs))
        diff = self.expected_check_streams().symmetric_difference(found_catalog_names)
        self.assertEqual(len(diff), 0, msg="discovered schemas do not match: {}".format(diff))

        # verify that persisted streams have the correct properties

        test_catalog_cows = list(filter( lambda c: c['stream_name'] == 'postgres_logical_replication_test_cows', found_catalogs))[0]
        self.assertEqual('postgres_logical_replication_test_cows', test_catalog_cows['stream_name'])


        test_catalog_chickens = list(filter( lambda c: c['stream_name'] == 'postgres_logical_replication_test_chickens', found_catalogs))[0]
        self.assertEqual('postgres_logical_replication_test_chickens', test_catalog_chickens['stream_name'])
        print("discovered streams are correct")

        additional_md = [{ "breadcrumb" : [], "metadata" : {'replication-method' : 'LOG_BASED'}}]
        connections.select_catalog_and_fields_via_metadata(conn_id, test_catalog_cows,
                                                           menagerie.get_annotated_schema(conn_id, test_catalog_cows['stream_id']),
                                                           additional_md)
        connections.select_catalog_and_fields_via_metadata(conn_id, test_catalog_chickens,
                                                           menagerie.get_annotated_schema(conn_id, test_catalog_chickens['stream_id']),
                                                           additional_md)

        # clear state
        menagerie.set_state(conn_id, {})

        sync_job_name = runner.run_sync_mode(self, conn_id)

        # verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        record_count_by_stream = runner.examine_target_output_file(self,
                                                                   conn_id,
                                                                   self.expected_sync_streams(),
                                                                   self.expected_pks())


        self.assertEqual(record_count_by_stream, { 'postgres_logical_replication_test_cows': 1, 'postgres_logical_replication_test_chickens': 1})
        records_by_stream = runner.get_records_from_target_output()

        table_version_cows = records_by_stream['postgres_logical_replication_test_cows']['table_version']
        self.assertEqual(records_by_stream['postgres_logical_replication_test_cows']['messages'][0]['action'], 'activate_version')
        self.assertEqual(records_by_stream['postgres_logical_replication_test_cows']['messages'][1]['action'], 'upsert')
        self.assertEqual(records_by_stream['postgres_logical_replication_test_cows']['messages'][2]['action'], 'activate_version')

        table_version_chickens = records_by_stream['postgres_logical_replication_test_chickens']['table_version']
        self.assertEqual(records_by_stream['postgres_logical_replication_test_chickens']['messages'][0]['action'], 'activate_version')
        self.assertEqual(records_by_stream['postgres_logical_replication_test_chickens']['messages'][1]['action'], 'upsert')
        self.assertEqual(records_by_stream['postgres_logical_replication_test_chickens']['messages'][2]['action'], 'activate_version')

        # verify state and bookmarks
        state = menagerie.get_state(conn_id)
        self.assertIsNone(state['currently_syncing'], msg="expected state's currently_syncing to be None")

        bookmark_cows = state['bookmarks']['dev-public-postgres_logical_replication_test_cows']
        self.assertIsNotNone(bookmark_cows['lsn'], msg="expected bookmark for stream to have an lsn")
        lsn_cows_1 = bookmark_cows['lsn']
        self.assertEqual(bookmark_cows['version'], table_version_cows, msg="expected bookmark for stream to match version")

        bookmark_chickens = state['bookmarks']['dev-public-postgres_logical_replication_test_chickens']
        self.assertIsNotNone(bookmark_chickens['lsn'], msg="expected bookmark for stream to have an lsn")
        lsn_chickens_1 = bookmark_chickens['lsn']
        self.assertEqual(bookmark_chickens['version'], table_version_chickens, msg="expected bookmark for stream to match version")


        #----------------------------------------------------------------------
        # invoke the sync job again after adding records
        #----------------------------------------------------------------------
        print("inserting 2 more cows and 2 more chickens")

        with db_utils.get_test_connection('dev') as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                # insert another cow
                self.cows_rec_2 = {'cow_name' : "betty cow", 'cow_age' : 21}
                insert_record(cur, test_table_name_cows, self.cows_rec_2)
                # update that cow's expected values
                self.cows_rec_2['id'] = 2
                self.cows_rec_2['_sdc_deleted_at'] = None

                # insert another chicken
                self.chicken_rec_2 = {'chicken_name' : "burt chicken", 'chicken_age' : 14}
                insert_record(cur, test_table_name_chickens, self.chicken_rec_2)
                # update that cow's expected values
                self.chicken_rec_2['id'] = 2
                self.chicken_rec_2['_sdc_deleted_at'] = None

                # and repeat...

                self.cows_rec_3 = {'cow_name' : "cindy cow", 'cow_age' : 10}
                insert_record(cur, test_table_name_cows, self.cows_rec_3)
                self.cows_rec_3['id'] = 3
                self.cows_rec_3['_sdc_deleted_at'] = None


                self.chicken_rec_3 = {'chicken_name' : "carl chicken", 'chicken_age' : 4}
                insert_record(cur, test_table_name_chickens, self.chicken_rec_3)
                self.chicken_rec_3['id'] = 3
                self.chicken_rec_3['_sdc_deleted_at'] = None


        sync_job_name = runner.run_sync_mode(self, conn_id)

        # verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        record_count_by_stream = runner.examine_target_output_file(self,
                                                                   conn_id,
                                                                   self.expected_sync_streams(),
                                                                   self.expected_pks())
        self.assertEqual(record_count_by_stream, { 'postgres_logical_replication_test_cows': 2, 'postgres_logical_replication_test_chickens': 2})
        records_by_stream = runner.get_records_from_target_output()
        chicken_messages = records_by_stream["postgres_logical_replication_test_chickens"]['messages']
        cow_messages = records_by_stream["postgres_logical_replication_test_cows"]['messages']

        self.assertDictEqual(self.cows_rec_2, cow_messages[0]['data'])
        self.assertDictEqual(self.chicken_rec_2, chicken_messages[0]['data'])
        self.assertDictEqual(self.cows_rec_3, cow_messages[1]['data'])
        self.assertDictEqual(self.chicken_rec_3, chicken_messages[1]['data'])

        print("inserted record is correct")

        state = menagerie.get_state(conn_id)
        self.assertIsNone(state['currently_syncing'], msg="expected state's currently_syncing to be None")
        cows_bookmark = state['bookmarks']['dev-public-postgres_logical_replication_test_cows']
        self.assertIsNotNone(cows_bookmark['lsn'], msg="expected bookmark for stream public-postgres_logical_replication_test to have an scn")
        lsn_cows_2 = cows_bookmark['lsn']
        self.assertTrue(lsn_cows_2 >= lsn_cows_1)

        chickens_bookmark = state['bookmarks']['dev-public-postgres_logical_replication_test_chickens']
        self.assertIsNotNone(chickens_bookmark['lsn'], msg="expected bookmark for stream public-postgres_logical_replication_test to have an scn")
        lsn_chickens_2 = chickens_bookmark['lsn']
        self.assertTrue(lsn_chickens_2 >= lsn_chickens_1)

        #table_version does NOT change
        self.assertEqual(chickens_bookmark['version'], table_version_chickens, msg="expected bookmark for stream public-postgres_logical_replication_test to match version")

        #table_version does NOT change
        self.assertEqual(cows_bookmark['version'], table_version_cows, msg="expected bookmark for stream public-postgres_logical_replication_test to match version")



SCENARIOS.add(PostgresLogicalRepMultipleTables)
