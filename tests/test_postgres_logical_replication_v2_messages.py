import os
import decimal
import unittest
import datetime
import uuid
import json

import pytz
import psycopg2.extras
from psycopg2.extensions import quote_ident
from tap_tester.scenario import (SCENARIOS)
import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner

import db_utils  # pylint: disable=import-error


expected_schemas = {'postgres_logical_replication_test':
                    {'definitions' : {
                        'sdc_recursive_integer_array' : { 'type' : ['null', 'integer', 'array'], 'items' : { '$ref': '#/definitions/sdc_recursive_integer_array'}},
                        'sdc_recursive_number_array' : { 'type' : ['null', 'number', 'array'], 'items' : { '$ref': '#/definitions/sdc_recursive_number_array'}},
                        'sdc_recursive_string_array' : { 'type' : ['null', 'string', 'array'], 'items' : { '$ref': '#/definitions/sdc_recursive_string_array'}},
                        'sdc_recursive_boolean_array' : { 'type' : ['null', 'boolean', 'array'], 'items' : { '$ref': '#/definitions/sdc_recursive_boolean_array'}},
                        'sdc_recursive_timestamp_array' : { 'type' : ['null', 'string', 'array'], 'format' : 'date-time', 'items' : { '$ref': '#/definitions/sdc_recursive_timestamp_array'}},
                        'sdc_recursive_object_array' : { 'type' : ['null','object', 'array'], 'items' : { '$ref': '#/definitions/sdc_recursive_object_array'}}
                    },
                     'type': 'object',
                     'properties': {'our_boolean': {'type': ['null', 'boolean']},
                                    '_sdc_deleted_at': {'format': 'date-time', 'type': ['null', 'string']},
                                    'OUR TS TZ': {'format' : 'date-time', 'type': ['null', 'string']},
                                    'OUR TS': {'format' : 'date-time', 'type': ['null', 'string']},
                                    'our_real': {'type': ['null', 'number']},
                                    'our_uuid': {'type': ['null', 'string']},
                                    'our_store': {'type': ['null', 'object'], 'properties' : {}},
                                    'our_smallint': {'maximum': 32767, 'type': ['null', 'integer'], 'minimum': -32768},
                                    'our_decimal': {'multipleOf': decimal.Decimal('0.01'), 'type': ['null', 'number'],
                                                    'maximum': 10000000000, 'exclusiveMinimum': True, 'minimum': -10000000000, 'exclusiveMaximum': True},
                                    'OUR DATE': {'format': 'date-time', 'type': ['null', 'string']},
                                    'our_jsonb': {'type': ['null', 'string']},
                                    'our_integer': {'maximum': 2147483647, 'type': ['null', 'integer'], 'minimum': -2147483648},
                                    'our_text': {'type': ['null', 'string']},
                                    'our_text_2': {'type': ['null', 'string']},
                                    'our_json': {'type': ['null', 'string']},
                                    'our_double': {'type': ['null', 'number']},
                                    'our_varchar': {'type': ['null', 'string']},
                                    'our_bigint': {'maximum': 9223372036854775807, 'type': ['null', 'integer'], 'minimum': -9223372036854775808},
                                    'id': {'maximum': 2147483647, 'type': ['integer'], 'minimum': -2147483648},
                                    'our_varchar_10': {'type': ['null', 'string'], 'maxLength': 10},
                                    'OUR TIME': {'type': ['null', 'string']},
                                    'OUR TIME TZ': {'type': ['null', 'string']},
                                    'our_bit': {'type': ['null', 'boolean']},
                                    'our_citext': {'type': ['null', 'string']},
                                    'our_cidr': {'type': ['null', 'string']},
                                    'our_inet': {'type': ['null', 'string']},
                                    'our_mac': {'type': ['null', 'string']},
                                    'our_alignment_enum': {'type': ['null', 'string']},
                                    'our_money': {'type': ['null', 'string']}}}}


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
test_table_name = "postgres_logical_replication_test"

def canonicalized_table_name(schema, table, cur):
    return "{}.{}".format(quote_ident(schema, cur), quote_ident(table, cur))


class PostgresLogicalRep(unittest.TestCase):
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

                old_table = cur.execute("""SELECT EXISTS (
                                          SELECT 1
                                          FROM  information_schema.tables
                                          WHERE  table_schema = %s
                                          AND  table_name =   %s);""",
                                        [test_schema_name, test_table_name])
                old_table = cur.fetchone()[0]

                if old_table:
                    cur.execute("DROP TABLE {}".format(canonicalized_table_name(test_schema_name, test_table_name, cur)))


                cur = conn.cursor()
                cur.execute(""" SELECT installed_version FROM pg_available_extensions WHERE name = 'hstore' """)
                if cur.fetchone()[0] is None:
                    cur.execute(""" CREATE EXTENSION hstore; """)

                cur.execute(""" CREATE EXTENSION IF NOT EXISTS citext WITH SCHEMA public;""")
                cur.execute(""" DROP TYPE IF EXISTS ALIGNMENT CASCADE """)
                cur.execute(""" CREATE TYPE ALIGNMENT AS ENUM ('good', 'bad', 'ugly') """)


                create_table_sql = """
CREATE TABLE {} (id            SERIAL PRIMARY KEY,
                our_varchar    VARCHAR,
                our_varchar_10 VARCHAR(10),
                our_text       TEXT,
                our_text_2     TEXT,
                our_integer    INTEGER,
                our_smallint   SMALLINT,
                our_bigint     BIGINT,
                our_decimal    NUMERIC(12,2),
                "OUR TS"       TIMESTAMP WITHOUT TIME ZONE,
                "OUR TS TZ"    TIMESTAMP WITH TIME ZONE,
                "OUR TIME"     TIME WITHOUT TIME ZONE,
                "OUR TIME TZ"  TIME WITH TIME ZONE,
                "OUR DATE"     DATE,
                our_double     DOUBLE PRECISION,
                our_real       REAL,
                our_boolean    BOOLEAN,
                our_bit        BIT(1),
                our_json       JSON,
                our_jsonb      JSONB,
                our_uuid       UUID,
                our_store      HSTORE,
                our_citext     CITEXT,
                our_cidr       cidr,
                our_inet       inet,
                our_mac            macaddr,
                our_alignment_enum ALIGNMENT,
                our_money          money)
                """.format(canonicalized_table_name(test_schema_name, test_table_name, cur))

                cur.execute(create_table_sql)

                #insert fixture data 1
                our_ts = datetime.datetime(1997, 2, 2, 2, 2, 2, 722184)
                nyc_tz = pytz.timezone('America/New_York')
                our_ts_tz = nyc_tz.localize(our_ts)
                our_time  = datetime.time(12,11,10)
                our_time_tz = our_time.isoformat() + "-04:00"
                our_date = datetime.date(1998, 3, 4)
                my_uuid =  str(uuid.uuid1())

                self.rec_1 = {'our_varchar' : "our_varchar",
                              'our_varchar_10' : "varchar_10",
                              'our_text' : "some text",
                              'our_text_2' : "NOT SELECTED",
                              'our_integer' : 44100,
                              'our_smallint' : 1, 'our_bigint' : 1000000,
                              'our_decimal' : decimal.Decimal('1234567890.01'),
                              quote_ident('OUR TS', cur) : our_ts,
                              quote_ident('OUR TS TZ', cur) : our_ts_tz,
                              quote_ident('OUR TIME', cur) : our_time,
                              quote_ident('OUR TIME TZ', cur) : our_time_tz,
                              quote_ident('OUR DATE', cur) : our_date,
                              'our_double' : 1.1,
                              'our_real' : 1.2,
                              'our_boolean' : True,
                              'our_bit' : '0',
                              'our_json' : json.dumps({'secret' : 55}),
                              'our_jsonb' : json.dumps(['burgers are good']),
                              'our_uuid' : my_uuid,
                              'our_store' : 'size=>"small",name=>"betty"',
                              'our_citext': 'maGICKal',
                              'our_cidr' : '192.168.100.128/25',
                              'our_inet': '192.168.100.128/24',
                              'our_mac' : '08:00:2b:01:02:03',
                              'our_alignment_enum': 'bad'}


                insert_record(cur, test_table_name, self.rec_1)

                #insert fixture data 2
                our_ts = datetime.datetime(1987, 3, 3, 3, 3, 3, 733184)
                nyc_tz = pytz.timezone('America/New_York')
                our_ts_tz = nyc_tz.localize(our_ts)
                our_time  = datetime.time(10,9,8)
                our_time_tz = our_time.isoformat() + "-04:00"
                our_date = datetime.date(1964, 7, 1)
                my_uuid =  str(uuid.uuid1())

                self.rec_2 = {'our_varchar' : "our_varchar 2",
                              'our_varchar_10' : "varchar_10",
                              'our_text' : "some text 2",
                              'our_text_2' : "NOT SELECTED",
                              'our_integer' : 44101,
                              'our_smallint' : 2,
                              'our_bigint' : 1000001,
                              'our_decimal' : decimal.Decimal('9876543210.02'),
                              quote_ident('OUR TS', cur) : our_ts,
                              quote_ident('OUR TS TZ', cur) : our_ts_tz,
                              quote_ident('OUR TIME', cur) : our_time,
                              quote_ident('OUR TIME TZ', cur) : our_time_tz,
                              quote_ident('OUR DATE', cur) : our_date,
                              'our_double' : 1.1,
                              'our_real' : 1.2,
                              'our_boolean' : True,
                              'our_bit' : '1',
                              'our_json' : json.dumps({'nymn' : 77}),
                              'our_jsonb' : json.dumps({'burgers' : 'good++'}),
                              'our_uuid' : my_uuid,
                              'our_store' : 'dances=>"floor",name=>"betty"',
                              'our_citext': 'maGICKal 2',
                              'our_cidr' : '192.168.101.128/25',
                              'our_inet': '192.168.101.128/24',
                              'our_mac' : '08:00:2b:01:02:04',
                }

                insert_record(cur, test_table_name, self.rec_2)

    @staticmethod
    def expected_check_streams():
        return { 'dev-public-postgres_logical_replication_test'}

    @staticmethod
    def expected_sync_streams():
        return { 'postgres_logical_replication_test' }

    @staticmethod
    def expected_pks():
        return {
            'postgres_logical_replication_test' : {'id'}
        }

    @staticmethod
    def tap_name():
        return "tap-postgres"

    @staticmethod
    def name():
        return "tap_tester_postgres_logical_replication_v2_message"

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
                'logical_poll_total_seconds': '10',
                'wal2json_message_format': '2'
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
                                1,
                                msg="unable to locate schemas for connection {}".format(conn_id))

        found_catalog_names = set(map(lambda c: c['tap_stream_id'], found_catalogs))
        diff = self.expected_check_streams().symmetric_difference(found_catalog_names)
        self.assertEqual(len(diff), 0, msg="discovered schemas do not match: {}".format(diff))

        # verify that persisted streams have the correct properties
        test_catalog = found_catalogs[0]

        self.assertEqual('postgres_logical_replication_test', test_catalog['stream_name'])

        print("discovered streams are correct")

        additional_md = [{ "breadcrumb" : [], "metadata" : {'replication-method' : 'LOG_BASED'}}]
        #don't selcted our_text_2
        _ = connections.select_catalog_and_fields_via_metadata(conn_id, test_catalog,
                                                               menagerie.get_annotated_schema(conn_id, test_catalog['stream_id']),
                                                               additional_md,
                                                               ['our_text_2'])

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


        self.assertEqual(record_count_by_stream, { 'postgres_logical_replication_test': 2})
        records_by_stream = runner.get_records_from_target_output()

        table_version = records_by_stream['postgres_logical_replication_test']['table_version']

        self.assertEqual(records_by_stream['postgres_logical_replication_test']['messages'][0]['action'],
                         'activate_version')

        self.assertEqual(records_by_stream['postgres_logical_replication_test']['messages'][1]['action'],
                         'upsert')

        self.assertEqual(records_by_stream['postgres_logical_replication_test']['messages'][2]['action'],
                         'upsert')

        self.assertEqual(records_by_stream['postgres_logical_replication_test']['messages'][3]['action'],
                         'activate_version')

        # verify state and bookmarks
        state = menagerie.get_state(conn_id)


        bookmark = state['bookmarks']['dev-public-postgres_logical_replication_test']
        self.assertIsNone(state['currently_syncing'], msg="expected state's currently_syncing to be None")

        self.assertIsNotNone(bookmark['lsn'],
                             msg="expected bookmark for stream to have an lsn")
        lsn_1 = bookmark['lsn']

        self.assertEqual(bookmark['version'], table_version,
                         msg="expected bookmark for stream to match version")


        #----------------------------------------------------------------------
        # invoke the sync job again after adding a record
        #----------------------------------------------------------------------
        print("inserting a record 3")

        with db_utils.get_test_connection('dev') as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                #insert fixture data 3
                our_ts = datetime.datetime(1993, 3, 3, 3, 3, 3, 333333)
                nyc_tz = pytz.timezone('America/New_York')
                our_ts_tz = nyc_tz.localize(our_ts)
                our_time  = datetime.time(3,4,5)
                our_time_tz = our_time.isoformat() + "-04:00"
                our_date = datetime.date(1933, 3, 3)
                my_uuid =  str(uuid.uuid1())

                #STRINGS:
                #OUR TS: '1993-03-03 03:03:03.333333'
                #OUR TS TZ: '1993-03-03 08:03:03.333333+00'
                #'OUR TIME': '03:04:05'
                #'OUR TIME TZ': '03:04:05+00'
                self.rec_3 = {'our_varchar' : "our_varchar 3", # str
                              'our_varchar_10' : "varchar13", # str
                              'our_text' : "some text 3", #str
                              'our_text_2' : "NOT SELECTED",
                              'our_integer' : 96000, #int
                              'our_smallint' : 3, # int
                              'our_bigint' : 3000000, #int
                              'our_decimal' : decimal.Decimal('1234567890.03'), #1234567890.03 / our_decimal is a <class 'float'>
                              quote_ident('OUR TS', cur) : our_ts,              # str '1993-03-03 03:03:03.333333'
                              quote_ident('OUR TS TZ', cur) : our_ts_tz,        #str '1993-03-03 08:03:03.333333+00'
                              quote_ident('OUR TIME', cur) : our_time,          # str '03:04:05'
                              quote_ident('OUR TIME TZ', cur) : our_time_tz,    # str '03:04:05+00'
                              quote_ident('OUR DATE', cur) : our_date,          #1933-03-03 / OUR DATE is a <class 'str'>
                              'our_double' : 3.3,                               #3.3 / our_double is a <class 'float'>
                              'our_real' : 6.6,                                 #6.6 / our_real is a <class 'float'>
                              'our_boolean' : True,                             #boolean
                              'our_bit' : '1',                                  #string
                              'our_json' : json.dumps({'secret' : 33}),         #string
                              'our_jsonb' : json.dumps(['burgers make me hungry']),
                              'our_uuid' : my_uuid, #string
                              'our_store' : 'jumps=>"high",name=>"betty"', #string
                              'our_citext': 'maGICKal 3',
                              'our_cidr' : '192.168.102.128/32',
                              'our_inet': '192.168.102.128/32',
                              'our_mac' : '08:00:2b:01:02:05',
                              'our_money':     '$412.1234'
                }

                insert_record(cur, test_table_name, self.rec_3)

        sync_job_name = runner.run_sync_mode(self, conn_id)

        # verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        record_count_by_stream = runner.examine_target_output_file(self,
                                                                   conn_id,
                                                                   self.expected_sync_streams(),
                                                                   self.expected_pks())

        self.assertEqual(record_count_by_stream, { 'postgres_logical_replication_test': 1 })
        records_by_stream = runner.get_records_from_target_output()

        self.assertTrue(len(records_by_stream) > 0)

        for stream, recs in records_by_stream.items():
            # verify the persisted schema was correct
            self.assertEqual(recs['schema'],
                             expected_schemas[stream],
                             msg="Persisted schema did not match expected schema for stream `{}`.".format(stream))

        self.assertEqual(1, len(records_by_stream['postgres_logical_replication_test']['messages']))
        actual_record_1 = records_by_stream['postgres_logical_replication_test']['messages'][0]['data']

        expected_inserted_record = {'our_text': 'some text 3',
                                    'our_real': decimal.Decimal('6.6'),
                                    '_sdc_deleted_at': None,
                                    'our_store' : {'name' : 'betty', 'jumps' : 'high' },
                                    'our_bigint': 3000000,
                                    'our_varchar': 'our_varchar 3',
                                    'our_double': decimal.Decimal('3.3'),
                                    'our_bit': True,
                                    'our_uuid': self.rec_3['our_uuid'],
                                    'OUR TS': '1993-03-03T03:03:03.333333+00:00',
                                    'OUR TS TZ': '1993-03-03T08:03:03.333333+00:00',
                                    'OUR TIME': '03:04:05',
                                    'OUR TIME TZ': '03:04:05-04:00',
                                    'OUR DATE': '1933-03-03T00:00:00+00:00',
                                    'our_decimal': decimal.Decimal('1234567890.03'),
                                    'id': 3,
                                    'our_varchar_10': 'varchar13',
                                    'our_json': '{"secret": 33}',
                                    'our_jsonb': self.rec_3['our_jsonb'],
                                    'our_smallint': 3,
                                    'our_integer': 96000,
                                    'our_boolean': True,
                                    'our_citext': 'maGICKal 3',
                                    'our_cidr': self.rec_3['our_cidr'],
                                    'our_inet': '192.168.102.128',
                                    'our_mac': self.rec_3['our_mac'],
                                    'our_alignment_enum' : None,
                                    'our_money'          :'$412.12'
        }
        self.assertEqual(set(actual_record_1.keys()), set(expected_inserted_record.keys()),
                         msg="keys for expected_record_1 are wrong: {}".format(set(actual_record_1.keys()).symmetric_difference(set(expected_inserted_record.keys()))))

        for k,v in actual_record_1.items():
            self.assertEqual(actual_record_1[k], expected_inserted_record[k], msg="{} != {} for key {}".format(actual_record_1[k], expected_inserted_record[k], k))

        self.assertEqual(records_by_stream['postgres_logical_replication_test']['messages'][0]['action'], 'upsert')
        print("inserted record is correct")

        state = menagerie.get_state(conn_id)
        chicken_bookmark = state['bookmarks']['dev-public-postgres_logical_replication_test']
        self.assertIsNone(state['currently_syncing'], msg="expected state's currently_syncing to be None")

        self.assertIsNotNone(chicken_bookmark['lsn'],
                             msg="expected bookmark for stream public-postgres_logical_replication_test to have an scn")
        lsn_2 = chicken_bookmark['lsn']

        self.assertTrue(lsn_2 >= lsn_1)

        #table_version does NOT change
        self.assertEqual(chicken_bookmark['version'], table_version,
                         msg="expected bookmark for stream public-postgres_logical_replication_test to match version")

        #----------------------------------------------------------------------
        # invoke the sync job again after deleting a record
        #----------------------------------------------------------------------
        print("delete row from source db")
        with db_utils.get_test_connection('dev') as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute("DELETE FROM {} WHERE id = 3".format(canonicalized_table_name(test_schema_name, test_table_name, cur)))

        sync_job_name = runner.run_sync_mode(self, conn_id)

        # verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        record_count_by_stream = runner.examine_target_output_file(self,
                                                                   conn_id,
                                                                   self.expected_sync_streams(),
                                                                   self.expected_pks())

        self.assertEqual(record_count_by_stream, { 'postgres_logical_replication_test': 1 })
        records_by_stream = runner.get_records_from_target_output()

        for stream, recs in records_by_stream.items():
            # verify the persisted schema was correct
            self.assertEqual(recs['schema'],
                             expected_schemas[stream],
                             msg="Persisted schema did not match expected schema for stream `{}`.".format(stream))

        #the message will be the delete
        delete_message = records_by_stream['postgres_logical_replication_test']['messages'][0]
        self.assertEqual(delete_message['action'], 'upsert')

        sdc_deleted_at = delete_message['data'].get('_sdc_deleted_at')
        self.assertIsNotNone(sdc_deleted_at)
        self.assertEqual(delete_message['data']['id'], 3)
        print("deleted record is correct")

        state = menagerie.get_state(conn_id)
        bookmark = state['bookmarks']['dev-public-postgres_logical_replication_test']
        self.assertIsNone(state['currently_syncing'], msg="expected state's currently_syncing to be None")

        self.assertIsNotNone(bookmark['lsn'],
                             msg="expected bookmark for stream ROOT-CHICKEN to have an scn")

        lsn_3 = bookmark['lsn']
        self.assertTrue(lsn_3 >= lsn_2)

        #table_version does NOT change
        self.assertEqual(bookmark['version'], table_version,
                         msg="expected bookmark for stream postgres_logical_replication_test to match version")
        #----------------------------------------------------------------------
        # invoke the sync job again after updating a record
        #----------------------------------------------------------------------
        print("updating row from source db")
        with db_utils.get_test_connection('dev') as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute("UPDATE {} SET our_varchar = 'THIS HAS BEEN UPDATED', our_money = '$56.811', our_decimal = 'NaN', our_real = '+Infinity', our_double = 'NaN' WHERE id = 1".format(canonicalized_table_name(test_schema_name, test_table_name, cur)))

        sync_job_name = runner.run_sync_mode(self, conn_id)
        # verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        record_count_by_stream = runner.examine_target_output_file(self,
                                                                   conn_id,
                                                                   self.expected_sync_streams(),
                                                                   self.expected_pks())

        self.assertEqual(record_count_by_stream, { 'postgres_logical_replication_test': 1 })
        records_by_stream = runner.get_records_from_target_output()
        for stream, recs in records_by_stream.items():
            # verify the persisted schema was correct
            self.assertEqual(recs['schema'],
                             expected_schemas[stream],
                             msg="Persisted schema did not match expected schema for stream `{}`.".format(stream))


        # verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        self.assertEqual(len(records_by_stream['postgres_logical_replication_test']['messages']), 1)

        #record will be the new update
        update_message = records_by_stream['postgres_logical_replication_test']['messages'][0]
        self.assertEqual(update_message['action'], 'upsert')

        expected_updated_rec = {'our_varchar' : 'THIS HAS BEEN UPDATED',
                                'id' : 1,
                                'our_varchar_10' : "varchar_10",
                                'our_text' : "some text",
                                'our_integer' : 44100,
                                'our_smallint' : 1,
                                'our_bigint' : 1000000,
                                'our_decimal' : None,
                                'OUR TS': '1997-02-02T02:02:02.722184+00:00',
                                'OUR TS TZ' : '1997-02-02T07:02:02.722184+00:00',
                                'OUR TIME' : '12:11:10',
                                'OUR TIME TZ' : '12:11:10-04:00',
                                'OUR DATE': '1998-03-04T00:00:00+00:00',
                                'our_double' : None,
                                'our_real' : None,
                                'our_boolean' : True,
                                'our_bit' : False,
                                'our_json' : '{"secret": 55}',
                                'our_jsonb' : self.rec_1['our_jsonb'],
                                'our_uuid' : self.rec_1['our_uuid'],
                                '_sdc_deleted_at' : None,
                                'our_store' : {'name' : 'betty', 'size' : 'small' },
                                'our_citext': 'maGICKal',
                                'our_cidr': self.rec_1['our_cidr'],
                                'our_inet': self.rec_1['our_inet'],
                                'our_mac': self.rec_1['our_mac'],
                                'our_alignment_enum' : 'bad',
                                'our_money' : '$56.81'
        }

        self.assertEqual(set(update_message['data'].keys()), set(expected_updated_rec.keys()),
                         msg="keys for expected_record_1 are wrong: {}".format(set(update_message['data'].keys()).symmetric_difference(set(expected_updated_rec.keys()))))


        for k,v in update_message['data'].items():
            self.assertEqual(v, expected_updated_rec[k], msg="{} != {} for key {}".format(v, expected_updated_rec[k], k))

        print("updated record is correct")

        #check state again
        state = menagerie.get_state(conn_id)
        self.assertIsNone(state['currently_syncing'], msg="expected state's currently_syncing to be None")
        chicken_bookmark = state['bookmarks']['dev-public-postgres_logical_replication_test']
        self.assertIsNone(state['currently_syncing'], msg="expected state's currently_syncing to be None")
        self.assertIsNotNone(chicken_bookmark['lsn'],
                             msg="expected bookmark for stream public-postgres_logical_replication_test to have an scn")
        lsn_3 = chicken_bookmark['lsn']
        self.assertTrue(lsn_3 >= lsn_2)

        #table_version does NOT change
        self.assertEqual(chicken_bookmark['version'], table_version,
                         msg="expected bookmark for stream public-postgres_logical_replication_test to match version")


        #----------------------------------------------------------------------
        # invoke the sync job one last time. should only get the PREVIOUS update
        #----------------------------------------------------------------------
        sync_job_name = runner.run_sync_mode(self, conn_id)
        # verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        record_count_by_stream = runner.examine_target_output_file(self,
                                                                   conn_id,
                                                                   self.expected_sync_streams(),
                                                                   self.expected_pks())
        #we should not get any records
        self.assertEqual(record_count_by_stream, {})

        #check state again
        state = menagerie.get_state(conn_id)
        chicken_bookmark = state['bookmarks']['dev-public-postgres_logical_replication_test']
        self.assertIsNone(state['currently_syncing'], msg="expected state's currently_syncing to be None")
        self.assertIsNotNone(chicken_bookmark['lsn'],
                             msg="expected bookmark for stream public-postgres_logical_replication_test to have an scn")
        lsn_4 = chicken_bookmark['lsn']
        self.assertTrue(lsn_4 >= lsn_3)

        #table_version does NOT change
        self.assertEqual(chicken_bookmark['version'], table_version,
                         msg="expected bookmark for stream public-postgres_logical_replication_test to match version")

SCENARIOS.add(PostgresLogicalRep)
