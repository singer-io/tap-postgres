import os
import decimal
import unittest
import datetime
import uuid
import json

import psycopg2.extras
from psycopg2.extensions import quote_ident
import pytz
from tap_tester.scenario import (SCENARIOS)
import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner

import db_utils  # pylint: disable=import-error


NUMERIC_SCALE=2
NUMERIC_PRECISION=12

expected_schemas = {'postgres_full_table_replication_test':
                    {'definitions' : {
                        'sdc_recursive_integer_array' : { 'type' : ['null', 'integer', 'array'], 'items' : { '$ref': '#/definitions/sdc_recursive_integer_array'}},
                        'sdc_recursive_number_array' : { 'type' : ['null', 'number', 'array'], 'items' : { '$ref': '#/definitions/sdc_recursive_number_array'}},
                        'sdc_recursive_string_array' : { 'type' : ['null', 'string', 'array'], 'items' : { '$ref': '#/definitions/sdc_recursive_string_array'}},
                        'sdc_recursive_boolean_array' : { 'type' : ['null', 'boolean', 'array'], 'items' : { '$ref': '#/definitions/sdc_recursive_boolean_array'}},
                        'sdc_recursive_timestamp_array' : { 'type' : ['null', 'string', 'array'], 'format' : 'date-time', 'items' : { '$ref': '#/definitions/sdc_recursive_timestamp_array'}},
                        'sdc_recursive_object_array' : { 'type' : ['null','object', 'array'], 'items' : { '$ref': '#/definitions/sdc_recursive_object_array'}},
                    },
                     'type': 'object',
                     'properties': {'our_boolean': {'type': ['null', 'boolean']},
                                    'OUR TS TZ': {'format' : 'date-time', 'type': ['null', 'string']},
                                    'OUR TS': {'format' : 'date-time', 'type': ['null', 'string']},
                                    'our_real': {'type': ['null', 'number']},
                                    'our_uuid': {'type': ['null', 'string']},
                                    'our_store': {'type': ['null', 'object'], 'properties': {}},
                                    'our_smallint': {'maximum': 32767, 'type': ['null', 'integer'], 'minimum': -32768},
                                    'our_decimal': {'multipleOf': decimal.Decimal(str(10 ** (0 - NUMERIC_SCALE))), 'type': ['null', 'number'],
                                                    'maximum': 10 ** (NUMERIC_PRECISION - NUMERIC_SCALE), 'exclusiveMinimum': True,
                                                    'minimum': -10 ** (NUMERIC_PRECISION - NUMERIC_SCALE), 'exclusiveMaximum': True},
                                    'OUR DATE': {'format': 'date-time', 'type': ['null', 'string']},
                                    'our_jsonb': {'type': ['null', 'string']},
                                    'our_integer': {'maximum': 2147483647, 'type': ['null', 'integer'], 'minimum': -2147483648},
                                    'our_text': {'type': ['null', 'string']},
                                    'our_json': {'type': ['null', 'string']},
                                    'our_double': {'type': ['null', 'number']},
                                    'our_varchar': {'type': ['null', 'string']},
                                    'our_bigint': {'maximum': 9223372036854775807,'type': ['null', 'integer'], 'minimum': -9223372036854775808},
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
                                    'our_money': {'type': ['null', 'string']}
                     }}}

test_schema_name = "public"
test_table_name = "postgres_full_table_replication_test"


class PostgresFullTable(unittest.TestCase):

    def setUp(self):
        db_utils.ensure_environment_variables_set()

        db_utils.ensure_db("dev")

        self.maxDiff = None

        with db_utils.get_test_connection('dev') as conn:
            conn.autocommit = True

            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                canon_table_name = db_utils.canonicalized_table_name(cur, test_schema_name, test_table_name)

                cur = db_utils.ensure_fresh_table(conn, cur, test_schema_name, test_table_name)

                create_table_sql = """
CREATE TABLE {} (id            SERIAL PRIMARY KEY,
                our_varchar    VARCHAR,
                our_varchar_10 VARCHAR(10),
                our_text       TEXT,
                our_integer    INTEGER,
                our_smallint   SMALLINT,
                our_bigint     BIGINT,
                our_decimal    NUMERIC({},{}),
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
                our_inet       inet,
                our_cidr       cidr,
                our_mac        macaddr,
                our_alignment_enum ALIGNMENT,
                our_money          money)
                """.format(canon_table_name, NUMERIC_PRECISION, NUMERIC_SCALE)

                cur.execute(create_table_sql)

                # insert fixture data and track expected records
                self.inserted_records = []
                self.expected_records = []

                # record 1
                our_ts = datetime.datetime(1997, 2, 2, 2, 2, 2, 722184)
                nyc_tz = pytz.timezone('America/New_York')
                our_ts_tz = nyc_tz.localize(our_ts)
                our_time  = datetime.time(12,11,10)
                our_time_tz = our_time.isoformat() + "-04:00"
                our_date = datetime.date(1998, 3, 4)
                my_uuid =  str(uuid.uuid1())
                self.inserted_records.append({
                    'our_varchar' : "our_varchar",
                    'our_varchar_10' : "varchar_10",
                    'our_text' : "some text",
                    'our_integer' : 44100,
                    'our_smallint' : 1,
                    'our_bigint' : 1000000,
                    'our_decimal' : decimal.Decimal('.01'),
                    quote_ident('OUR TS', cur) : our_ts,
                    quote_ident('OUR TS TZ', cur) :  our_ts_tz,
                    quote_ident('OUR TIME', cur) : our_time,
                    quote_ident('OUR TIME TZ', cur) : our_time_tz,
                    quote_ident('OUR DATE', cur) : our_date,
                    'our_double' : decimal.Decimal('1.1'),
                    'our_real' : 1.2,
                    'our_boolean' : True,
                    'our_bit' : '0',
                    'our_json' : json.dumps({'secret' : 55}),
                    'our_jsonb' : json.dumps({'burgers' : 'good'}),
                    'our_uuid' : my_uuid,
                    'our_store' : 'size=>"small",name=>"betty"',
                    'our_citext': 'maGICKal 4',
                    'our_cidr' : '192.168.100.128/25',
                    'our_inet': '192.168.100.128/24',
                    'our_mac' : '08:00:2b:01:02:03',
                    'our_alignment_enum': 'good',
                    'our_money':    '100.1122',
                })
                self.expected_records.append({
                    'our_decimal': decimal.Decimal('.01'),
                    'our_text': 'some text',
                    'our_bit': False,
                    'our_integer': 44100,
                    'our_double': decimal.Decimal('1.1'),
                    'id': 1,
                    'our_json': '{"secret": 55}',
                    'our_boolean': True,
                    'our_jsonb': '{"burgers": "good"}',
                    'our_bigint': 1000000,
                    'OUR TS':       self.expected_ts(our_ts),
                    'OUR TS TZ': self.expected_ts_tz(our_ts_tz),
                    'OUR TIME': str(our_time),
                    'OUR TIME TZ': str(our_time_tz),
                    'our_store': {"name" : "betty", "size" :"small"},
                    'our_smallint': 1,
                    'OUR DATE': '1998-03-04T00:00:00+00:00',
                    'our_varchar': 'our_varchar',
                    'our_uuid': self.inserted_records[0]['our_uuid'],
                    'our_real': decimal.Decimal('1.2'),
                    'our_varchar_10': 'varchar_10',
                    'our_citext'    : self.inserted_records[0]['our_citext'],
                    'our_inet'    : self.inserted_records[0]['our_inet'],
                    'our_cidr'    : self.inserted_records[0]['our_cidr'],
                    'our_mac'    : self.inserted_records[0]['our_mac'],
                    'our_alignment_enum' : self.inserted_records[0]['our_alignment_enum'],
                    'our_money'      : '$100.11'
                })
                # record 2
                our_ts = datetime.datetime(1987, 3, 3, 3, 3, 3, 733184)
                nyc_tz = pytz.timezone('America/New_York')
                our_ts_tz = nyc_tz.localize(our_ts)
                our_time  = datetime.time(10,9,8)
                our_time_tz = our_time.isoformat() + "-04:00"
                our_date = datetime.date(1964, 7, 1)
                my_uuid =  str(uuid.uuid1())
                self.inserted_records.append({
                    'our_varchar' : "our_varchar 2",
                    'our_varchar_10' : "varchar_10",
                    'our_text' : "some text 2",
                    'our_integer' : 44101,
                    'our_smallint' : 2,
                    'our_bigint' : 1000001,
                    'our_decimal' : decimal.Decimal('.02'),
                    quote_ident('OUR TS', cur) : our_ts,
                    quote_ident('OUR TS TZ', cur) : our_ts_tz,
                    quote_ident('OUR TIME', cur) : our_time,
                    quote_ident('OUR TIME TZ', cur) : our_time_tz,
                    quote_ident('OUR DATE', cur) : our_date,
                    'our_double' : decimal.Decimal('1.1'),
                    'our_real' : decimal.Decimal('1.2'),
                    'our_boolean' : True,
                    'our_bit' : '1',
                    'our_json' : json.dumps(["nymn 77"]),
                    'our_jsonb' : json.dumps({'burgers' : 'good++'}),
                    'our_uuid' : my_uuid,
                    'our_store' : 'dances=>"floor",name=>"betty"',
                    'our_citext': 'maGICKal 2',
                    'our_cidr' : '192.168.101.128/25',
                    'our_inet': '192.168.101.128/24',
                    'our_mac' : '08:00:2b:01:02:04',
                    'our_money': None
                })
                self.expected_records.append({
                    'our_decimal': decimal.Decimal('.02'),
                    'OUR TIME': str(our_time),
                    'our_text': 'some text 2',
                    'our_bit': True,
                    'our_integer': 44101,
                    'our_double': decimal.Decimal('1.1'),
                    'id': 2,
                    'our_json': '["nymn 77"]',
                    'our_boolean': True,
                    'our_jsonb': '{"burgers": "good++"}',
                    'our_bigint': 1000001,
                    'OUR TIME TZ': str(our_time_tz),
                    'our_store': {"name" : "betty", "dances" :"floor"},
                    'OUR TS TZ': self.expected_ts_tz(our_ts_tz),
                    'our_smallint': 2,
                    'OUR DATE': '1964-07-01T00:00:00+00:00',
                    'our_varchar': 'our_varchar 2',
                    'OUR TS':       self.expected_ts(our_ts),
                    'our_uuid': self.inserted_records[1]['our_uuid'],
                    'our_real': decimal.Decimal('1.2'),
                    'our_varchar_10': 'varchar_10',
                    'our_citext'    : self.inserted_records[1]['our_citext'],
                    'our_inet'    : self.inserted_records[1]['our_inet'],
                    'our_cidr'    : self.inserted_records[1]['our_cidr'],
                    'our_mac'     : self.inserted_records[1]['our_mac'],
                    'our_alignment_enum' : None,
                    'our_money':    None
                })
                # record 3
                self.inserted_records.append({
                    'our_decimal' : decimal.Decimal('NaN'),
                    'our_double' : float('nan'),
                    'our_real' : float('-inf')
                })
                self.expected_records.append({
                    'id': 3,
                    # We cast NaN's, +Inf, -Inf to NULL as wal2json does not support
                    # them and now we are at least consistent(ly wrong).
                    'our_decimal' : None,
                    'our_double' : None,
                    'our_real' : None,
                    # any field without a set value will be set to NULL
                    'OUR TIME': None,
                    'our_text': None,
                    'our_bit': None,
                    'our_integer': None,
                    'our_json': None,
                    'our_boolean': None,
                    'our_jsonb': None,
                    'our_bigint': None,
                    'OUR TIME TZ': None,
                    'our_store': None,
                    'OUR TS TZ': None,
                    'our_smallint': None,
                    'OUR DATE': None,
                    'our_varchar': None,
                    'OUR TS': None,
                    'our_uuid': None,
                    'our_varchar_10': None,
                    'our_citext': None,
                    'our_inet': None,
                    'our_cidr': None,
                    'our_mac': None,
                    'our_alignment_enum': None,
                    'our_money': None
                })

                for record in self.inserted_records:
                    db_utils.insert_record(cur, test_table_name, record)

    @staticmethod
    def expected_check_streams():
        return { 'dev-public-postgres_full_table_replication_test'}

    @staticmethod
    def expected_sync_streams():
        return { 'postgres_full_table_replication_test' }

    @staticmethod
    def expected_pks():
        return {
            'postgres_full_table_replication_test' : {'id'}
        }

    @staticmethod
    def tap_name():
        return "tap-postgres"

    @staticmethod
    def name():
        return "tap_tester_postgres_full_table_replication"

    @staticmethod
    def get_type():
        return "platform.postgres"

    @staticmethod
    def get_credentials():
        return {'password': os.getenv('TAP_POSTGRES_PASSWORD')}

    @staticmethod
    def get_properties():
        return {'host' :   os.getenv('TAP_POSTGRES_HOST'),
                'dbname' : os.getenv('TAP_POSTGRES_DBNAME'),
                'port' : os.getenv('TAP_POSTGRES_PORT'),
                'user' : os.getenv('TAP_POSTGRES_USER'),
                'frequency_in_minutes': '1',
                # 'default_replication_method' : 'LOG_BASED',
                'filter_dbs' : 'postgres,dev',
                # 'ssl' : 'true', # TODO: Disabling for docker-based container
                'itersize' : '10'
        }

    @staticmethod
    def expected_ts_tz(our_ts_tz):
        our_ts_tz_utc = our_ts_tz.astimezone(pytz.utc)
        expected_value = datetime.datetime.strftime(our_ts_tz_utc, "%Y-%m-%dT%H:%M:%S.%f+00:00")

        return expected_value

    @staticmethod
    def expected_ts(our_ts):
        expected_value = datetime.datetime.strftime(our_ts, "%Y-%m-%dT%H:%M:%S.%f+00:00")

        return expected_value


    def test_run(self):
        conn_id = connections.ensure_connection(self)

        # run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        # verify check  exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        # verify discovery produced (at least) 1 expected catalog
        found_catalogs = [found_catalog for found_catalog in menagerie.get_catalogs(conn_id)
                          if found_catalog['tap_stream_id'] in self.expected_check_streams()]
        self.assertGreaterEqual(len(found_catalogs), 1)

        # verify the tap discovered the expected streams
        found_catalog_names = {catalog['tap_stream_id'] for catalog in found_catalogs}
        self.assertSetEqual(self.expected_check_streams(), found_catalog_names)

        # verify that persisted streams have the correct properties
        test_catalog = found_catalogs[0]
        self.assertEqual(test_table_name, test_catalog['stream_name'])
        print("discovered streams are correct")

        # perform table selection
        print('selecting {} and all fields within the table'.format(test_table_name))
        schema_and_metadata = menagerie.get_annotated_schema(conn_id, test_catalog['stream_id'])
        additional_md = [{ "breadcrumb" : [], "metadata" : {'replication-method' : 'FULL_TABLE'}}]
        _ = connections.select_catalog_and_fields_via_metadata(conn_id, test_catalog, schema_and_metadata, additional_md)

        # clear state
        menagerie.set_state(conn_id, {})

        # run sync job 1 and verify exit codes
        sync_job_name = runner.run_sync_mode(self, conn_id)
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # get records
        record_count_by_stream = runner.examine_target_output_file(
            self, conn_id, self.expected_sync_streams(), self.expected_pks()
        )
        records_by_stream = runner.get_records_from_target_output()
        table_version_1 = records_by_stream[test_table_name]['table_version']
        messages = records_by_stream[test_table_name]['messages']

        # verify the execpted number of records were replicated
        self.assertEqual(3, record_count_by_stream[test_table_name])

        # verify the message actions match expectations
        self.assertEqual(5, len(messages))
        self.assertEqual('activate_version', messages[0]['action'])
        self.assertEqual('upsert', messages[1]['action'])
        self.assertEqual('upsert', messages[2]['action'])
        self.assertEqual('upsert', messages[3]['action'])
        self.assertEqual('activate_version', messages[4]['action'])

        # verify the persisted schema matches expectations
        self.assertEqual(expected_schemas[test_table_name], records_by_stream[test_table_name]['schema'])

        # verify replicated records match expectations
        self.assertDictEqual(self.expected_records[0], messages[1]['data'])
        self.assertDictEqual(self.expected_records[1], messages[2]['data'])
        self.assertDictEqual(self.expected_records[2], messages[3]['data'])

        print("records are correct")

        # grab bookmarked state
        state = menagerie.get_state(conn_id)
        bookmark = state['bookmarks']['dev-public-postgres_full_table_replication_test']

        # verify state and bookmarks meet expectations
        self.assertIsNone(state['currently_syncing'])
        self.assertIsNone(bookmark.get('lsn'))
        self.assertIsNone(bookmark.get('replication_key'))
        self.assertIsNone(bookmark.get('replication_key_value'))
        self.assertEqual(table_version_1, bookmark['version'])

        #----------------------------------------------------------------------
        # invoke the sync job AGAIN and get the same 3 records
        #----------------------------------------------------------------------

        # run sync job 2 and verify exit codes
        sync_job_name = runner.run_sync_mode(self, conn_id)
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # get records
        record_count_by_stream = runner.examine_target_output_file(
            self, conn_id, self.expected_sync_streams(), self.expected_pks()
        )
        records_by_stream = runner.get_records_from_target_output()
        table_version_2 = records_by_stream[test_table_name]['table_version']
        messages = records_by_stream[test_table_name]['messages']

        # verify the execpted number of records were replicated
        self.assertEqual(3, record_count_by_stream[test_table_name])

        # verify the message actions match expectations
        self.assertEqual(4, len(messages))
        self.assertEqual('upsert', messages[0]['action'])
        self.assertEqual('upsert', messages[1]['action'])
        self.assertEqual('upsert', messages[2]['action'])
        self.assertEqual('activate_version', messages[3]['action'])

        # verify the new table version increased on the second sync
        self.assertGreater(table_version_2, table_version_1)

        # verify the persisted schema still matches expectations
        self.assertEqual(expected_schemas[test_table_name], records_by_stream[test_table_name]['schema'])

        # verify replicated records still match expectations
        self.assertDictEqual(self.expected_records[0], messages[0]['data'])
        self.assertDictEqual(self.expected_records[1], messages[1]['data'])
        self.assertDictEqual(self.expected_records[2], messages[2]['data'])

        # grab bookmarked state
        state = menagerie.get_state(conn_id)
        bookmark = state['bookmarks']['dev-public-postgres_full_table_replication_test']

        # verify state and bookmarks meet expectations
        self.assertIsNone(state['currently_syncing'])
        self.assertIsNone(bookmark.get('lsn'))
        self.assertIsNone(bookmark.get('replication_key'))
        self.assertIsNone(bookmark.get('replication_key_value'))
        self.assertEqual(table_version_2, bookmark['version'])


        #----------------------------------------------------------------------
        # invoke the sync job AGAIN following various manipulations to the data
        #----------------------------------------------------------------------

        with db_utils.get_test_connection('dev') as conn:
            conn.autocommit = True
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:

                # NB | We will perform the following actions prior to the next sync:
                #      [Action (EXPECTED RESULT)]

                #      Insert a record
                #      Insert a record to be updated prior to sync
                #      Insert a record to be deleted prior to sync (NOT REPLICATED)

                #      Update an existing record
                #      Update a newly inserted record

                #      Delete an existing record
                #      Delete  a newly inserted record

                # inserting...
                # a new record
                nyc_tz = pytz.timezone('America/New_York')
                our_time_offset = "-04:00"
                our_ts = datetime.datetime(1996, 4, 4, 4, 4, 4, 733184)
                our_ts_tz = nyc_tz.localize(our_ts)
                our_time  = datetime.time(6,6,6)
                our_time_tz = our_time.isoformat() + our_time_offset
                our_date = datetime.date(1970, 7, 1)
                my_uuid =  str(uuid.uuid1())
                self.inserted_records.append({
                    'our_varchar' : "our_varchar 2",
                    'our_varchar_10' : "varchar_10",
                    'our_text' : "some text 2",
                    'our_integer' : 44101,
                    'our_smallint' : 2,
                    'our_bigint' : 1000001,
                    'our_decimal' : decimal.Decimal('9876543210.02'),
                    quote_ident('OUR TS', cur) : our_ts,
                    quote_ident('OUR TS TZ', cur) : our_ts_tz,
                    quote_ident('OUR TIME', cur) : our_time,
                    quote_ident('OUR TIME TZ', cur) : our_time_tz,
                    quote_ident('OUR DATE', cur) : our_date,
                    'our_double' : decimal.Decimal('1.1'),
                    'our_real' : decimal.Decimal('1.2'),
                    'our_boolean' : True,
                    'our_bit' : '1',
                    'our_json' : json.dumps({'nymn' : 77}),
                    'our_jsonb' : json.dumps({'burgers' : 'good++'}),
                    'our_uuid' : my_uuid,
                    'our_citext' : 'cyclops 2',
                    'our_store' : 'dances=>"floor",name=>"betty"',
                    'our_cidr' : '192.168.101.128/25',
                    'our_inet': '192.168.101.128/24',
                    'our_mac' : '08:00:2b:01:02:04',
                    'our_money': '$0.98789'
                })
                self.expected_records.append({
                    'id': 4,
                    'our_varchar' : "our_varchar 2",
                    'our_varchar_10' : "varchar_10",
                    'our_text' : "some text 2",
                    'our_integer' : 44101,
                    'our_smallint' : 2,
                    'our_bigint' : 1000001,
                    'our_decimal' : decimal.Decimal('9876543210.02'),
                    'OUR TS' : self.expected_ts(our_ts),
                    'OUR TS TZ' : self.expected_ts_tz(our_ts_tz),
                    'OUR TIME' : str(our_time),
                    'OUR TIME TZ' : str(our_time_tz),
                    'OUR DATE' : '1970-07-01T00:00:00+00:00',
                    'our_double' : decimal.Decimal('1.1'),
                    'our_real' : decimal.Decimal('1.2'),
                    'our_boolean' : True,
                    'our_bit' : True,
                    'our_json': '{"nymn": 77}',
                    'our_jsonb': '{"burgers": "good++"}',
                    'our_uuid': self.inserted_records[-1]['our_uuid'],
                    'our_citext': self.inserted_records[-1]['our_citext'],
                    'our_store': {"name" : "betty", "dances" :"floor"},
                    'our_cidr': self.inserted_records[-1]['our_cidr'],
                    'our_inet': self.inserted_records[-1]['our_inet'],
                    'our_mac': self.inserted_records[-1]['our_mac'],
                    'our_money': '$0.99',
                    'our_alignment_enum': None,
                })
                # a new record which we will then update prior to sync
                our_ts = datetime.datetime(2007, 1, 1, 12, 12, 12, 222111)
                nyc_tz = pytz.timezone('America/New_York')
                our_ts_tz = nyc_tz.localize(our_ts)
                our_time  = datetime.time(12,11,10)
                our_time_tz = our_time.isoformat() + "-04:00"
                our_date = datetime.date(1999, 9, 9)
                my_uuid =  str(uuid.uuid1())
                self.inserted_records.append({
                    'our_varchar' : "our_varchar 4",
                    'our_varchar_10' : "varchar_3",
                    'our_text' : "some text 4",
                    'our_integer' : 55200,
                    'our_smallint' : 1,
                    'our_bigint' : 100000,
                    'our_decimal' : decimal.Decimal('1234567899.99'),
                    quote_ident('OUR TS', cur) : our_ts,
                    quote_ident('OUR TS TZ', cur) :  our_ts_tz,
                    quote_ident('OUR TIME', cur) : our_time,
                    quote_ident('OUR TIME TZ', cur) : our_time_tz,
                    quote_ident('OUR DATE', cur) : our_date,
                    'our_double' : decimal.Decimal('1.1'),
                    'our_real' : decimal.Decimal('1.2'),
                    'our_boolean' : True,
                    'our_bit' : '0',
                    'our_json' : json.dumps('some string'),
                    'our_jsonb' : json.dumps(['burgers are good']),
                    'our_uuid' : my_uuid,
                    'our_store' : 'size=>"small",name=>"betty"',
                    'our_citext' : 'cyclops 3',
                    'our_cidr' : '192.168.101.128/25',
                    'our_inet': '192.168.101.128/24',
                    'our_mac' : '08:00:2b:01:02:04',
                    'our_money':  None,
                })
                self.expected_records.append({
                    'our_decimal': decimal.Decimal('1234567899.99'),
                    'our_text': 'some text 4',
                    'our_bit': False,
                    'our_integer': 55200,
                    'our_double': decimal.Decimal('1.1'),
                    'id': 5,
                    'our_json': self.inserted_records[-1]['our_json'],
                    'our_boolean': True,
                    'our_jsonb': self.inserted_records[-1]['our_jsonb'],
                    'our_bigint': 100000,
                    'OUR TS': self.expected_ts(our_ts),
                    'OUR TS TZ': self.expected_ts_tz(our_ts_tz),
                    'OUR TIME': str(our_time),
                    'OUR TIME TZ': str(our_time_tz),
                    'our_store': {"name" : "betty", "size" :"small"},
                    'our_smallint': 1,
                    'OUR DATE': '1999-09-09T00:00:00+00:00',
                    'our_varchar': 'our_varchar 4',
                    'our_uuid': self.inserted_records[-1]['our_uuid'],
                    'our_real': decimal.Decimal('1.2'),
                    'our_varchar_10': 'varchar_3',
                    'our_citext' : 'cyclops 3',
                    'our_cidr' : '192.168.101.128/25',
                    'our_inet': '192.168.101.128/24',
                    'our_mac' : '08:00:2b:01:02:04',
                    'our_money' : None,
                    'our_alignment_enum': None,
                })
                # a new record to be deleted prior to sync
                our_ts = datetime.datetime(2111, 1, 1, 12, 12, 12, 222111)
                nyc_tz = pytz.timezone('America/New_York')
                our_ts_tz = nyc_tz.localize(our_ts)
                our_time  = datetime.time(12,11,10)
                our_time_tz = our_time.isoformat() + "-04:00"
                our_date = datetime.date(1999, 9, 9)
                my_uuid =  str(uuid.uuid1())
                self.inserted_records.append({
                    'our_varchar' : "our_varchar 4",
                    'our_varchar_10' : "varchar_3",
                    'our_text' : "some text 4",
                    'our_integer' : 55200,
                    'our_smallint' : 1,
                    'our_bigint' : 100000,
                    'our_decimal' : decimal.Decimal('1234567899.99'),
                    quote_ident('OUR TS', cur) : our_ts,
                    quote_ident('OUR TS TZ', cur) :  our_ts_tz,
                    quote_ident('OUR TIME', cur) : our_time,
                    quote_ident('OUR TIME TZ', cur) : our_time_tz,
                    quote_ident('OUR DATE', cur) : our_date,
                    'our_double' : decimal.Decimal('1.1'),
                    'our_real' : decimal.Decimal('1.2'),
                    'our_boolean' : True,
                    'our_bit' : '0',
                    'our_json' : json.dumps('some string'),
                    'our_jsonb' : json.dumps(['burgers are good']),
                    'our_uuid' : my_uuid,
                    'our_store' : 'size=>"small",name=>"betty"',
                    'our_citext' : 'cyclops 3',
                    'our_cidr' : '192.168.101.128/25',
                    'our_inet': '192.168.101.128/24',
                    'our_mac' : '08:00:2b:01:02:04',
                    'our_money':  None,
                })
                self.expected_records.append({
                    'our_decimal': decimal.Decimal('1234567899.99'),
                    'our_text': 'some text 4',
                    'our_bit': False,
                    'our_integer': 55200,
                    'our_double': decimal.Decimal('1.1'),
                    'id': 6,
                    'our_json': self.inserted_records[-1]['our_json'],
                    'our_boolean': True,
                    'our_jsonb': self.inserted_records[-1]['our_jsonb'],
                    'our_bigint': 100000,
                    'OUR TS': self.expected_ts(our_ts),
                    'OUR TS TZ': self.expected_ts_tz(our_ts_tz),
                    'OUR TIME': str(our_time),
                    'OUR TIME TZ': str(our_time_tz),
                    'our_store': {"name" : "betty", "size" :"small"},
                    'our_smallint': 1,
                    'OUR DATE': '1999-09-09T00:00:00+00:00',
                    'our_varchar': 'our_varchar 4',
                    'our_uuid': self.inserted_records[-1]['our_uuid'],
                    'our_real': decimal.Decimal('1.2'),
                    'our_varchar_10': 'varchar_3',
                    'our_citext' : 'cyclops 3',
                    'our_cidr' : '192.168.101.128/25',
                    'our_inet': '192.168.101.128/24',
                    'our_mac' : '08:00:2b:01:02:04',
                    'our_money' : None,
                    'our_alignment_enum': None,
                })

                db_utils.insert_record(cur, test_table_name, self.inserted_records[3])
                db_utils.insert_record(cur, test_table_name, self.inserted_records[4])
                db_utils.insert_record(cur, test_table_name, self.inserted_records[5])


                # updating ...
                # an existing record
                canon_table_name = db_utils.canonicalized_table_name(cur, test_schema_name, test_table_name)
                record_pk = 1
                our_ts = datetime.datetime(2021, 4, 4, 4, 4, 4, 733184)
                our_ts_tz = nyc_tz.localize(our_ts)
                updated_data = {
                    "OUR TS TZ": our_ts_tz,
                    "our_double": decimal.Decimal("6.6"),
                    "our_money": "$0.00"
                }
                self.expected_records[0]["OUR TS TZ"] = self.expected_ts_tz(our_ts_tz)
                self.expected_records[0]["our_double"] = decimal.Decimal("6.6")
                self.expected_records[0]["our_money"] = "$0.00"

                db_utils.update_record(cur, canon_table_name, record_pk, updated_data)

                # a newly inserted record
                canon_table_name = db_utils.canonicalized_table_name(cur, test_schema_name, test_table_name)
                record_pk = 5
                our_ts = datetime.datetime(2021, 4, 4, 4, 4, 4, 733184)
                our_ts_tz = nyc_tz.localize(our_ts)
                updated_data = {
                    "OUR TS TZ": our_ts_tz,
                    "our_double": decimal.Decimal("6.6"),
                    "our_money": "$0.00"
                }
                self.expected_records[4]["OUR TS TZ"] = self.expected_ts_tz(our_ts_tz)
                self.expected_records[4]["our_double"] = decimal.Decimal("6.6")
                self.expected_records[4]["our_money"] = "$0.00"

                db_utils.update_record(cur, canon_table_name, record_pk, updated_data)


                # deleting
                # an existing record
                record_pk = 2
                db_utils.delete_record(cur, canon_table_name, record_pk)

                # a newly inserted record
                record_pk = 6
                db_utils.delete_record(cur, canon_table_name, record_pk)

        #----------------------------------------------------------------------
        # invoke the sync job AGAIN after vairous manipulations
        #----------------------------------------------------------------------

        # run sync job 3 and verify exit codes
        sync_job_name = runner.run_sync_mode(self, conn_id)
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # get records
        record_count_by_stream = runner.examine_target_output_file(
            self, conn_id, self.expected_sync_streams(), self.expected_pks()
        )
        records_by_stream = runner.get_records_from_target_output()
        table_version_3 = records_by_stream[test_table_name]['table_version']
        messages = records_by_stream[test_table_name]['messages']

        # verify the execpted number of records were replicated
        self.assertEqual(4, record_count_by_stream[test_table_name])

        # verify the message actions match expectations
        self.assertEqual(5, len(messages))
        self.assertEqual('upsert', messages[0]['action'])
        self.assertEqual('upsert', messages[1]['action'])
        self.assertEqual('upsert', messages[2]['action'])
        self.assertEqual('upsert', messages[3]['action'])
        self.assertEqual('activate_version', messages[4]['action'])

        # verify the new table version increased on the second sync
        self.assertGreater(table_version_3, table_version_2)

        # verify the persisted schema still matches expectations
        self.assertEqual(expected_schemas[test_table_name], records_by_stream[test_table_name]['schema'])


        # NB | This is a little tough to track mentally so here's a breakdown of
        #      the order of operations by expected records indexes:

        #      Prior to Sync 1
        #        insert 0, 1, 2

        #      Prior to Sync 2
        #        No db changes

        #      Prior to Sync 3
        #        insert 3, 4, 5
        #        update 0, 4
        #        delete 1, 5

        #      Resulting Synced Records: 2, 3, 0, 4


        # verify replicated records still match expectations
        self.assertDictEqual(self.expected_records[2], messages[0]['data']) # existing insert
        self.assertDictEqual(self.expected_records[3], messages[1]['data']) # new insert
        self.assertDictEqual(self.expected_records[0], messages[2]['data']) # existing update
        self.assertDictEqual(self.expected_records[4], messages[3]['data']) # new insert / update

        # grab bookmarked state
        state = menagerie.get_state(conn_id)
        bookmark = state['bookmarks']['dev-public-postgres_full_table_replication_test']

        # verify state and bookmarks meet expectations
        self.assertIsNone(state['currently_syncing'])
        self.assertIsNone(bookmark.get('lsn'))
        self.assertIsNone(bookmark.get('replication_key'))
        self.assertIsNone(bookmark.get('replication_key_value'))
        self.assertEqual(table_version_3, bookmark['version'])


SCENARIOS.add(PostgresFullTable)
