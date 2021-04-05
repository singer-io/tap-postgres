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

expected_schemas = {'postgres_automatic_fields_test':
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
test_table_name = "postgres_automatic_fields_test"
test_db = "dev"

class PostgresAutomaticFields(unittest.TestCase):
    INCREMENTAL = "INCREMENTAL"
    FULL_TABLE = "FULL_TABLE"
    LOG_BASED = "LOG_BASED"

    default_replication_method = ""

    def tearDown(self):
        with db_utils.get_test_connection(test_db) as conn:
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
                    'our_integer' : 19972,
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
                    'our_integer': 19972,
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
                    'our_integer' : 19873,
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
                    'our_integer': 19873,
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

                for record in self.inserted_records:
                    db_utils.insert_record(cur, test_table_name, record)

    @staticmethod
    def expected_check_streams():
        return { 'dev-public-postgres_automatic_fields_test'}

    @staticmethod
    def expected_sync_streams():
        return { 'postgres_automatic_fields_test' }

    @staticmethod
    def expected_primary_keys():
        return {
            'postgres_automatic_fields_test' : {'id'}
        }

    def expected_replication_keys(self):
        replication_keys = {
            'postgres_automatic_fields_test' : {'our_integer'}
        }

        if self.default_replication_method == self.INCREMENTAL:
            return replication_keys
        else:
            return {'postgres_automatic_fields_test' : set()}

    @staticmethod
    def tap_name():
        return "tap-postgres"

    @staticmethod
    def name():
        return "tap_tester_postgres_automatic_fields"

    @staticmethod
    def get_type():
        return "platform.postgres"

    @staticmethod
    def get_credentials():
        return {'password': os.getenv('TAP_POSTGRES_PASSWORD')}

    def get_properties(self, original_properties=True):
        return_value = {
            'host' :   os.getenv('TAP_POSTGRES_HOST'),
            'dbname' : os.getenv('TAP_POSTGRES_DBNAME'),
            'port' : os.getenv('TAP_POSTGRES_PORT'),
            'user' : os.getenv('TAP_POSTGRES_USER'),
            'frequency_in_minutes': '1',
            'default_replication_method' : self.FULL_TABLE,
            'filter_dbs' : 'postgres,dev',
            # 'ssl' : 'true', # TODO: Disabling for docker-based container
            'itersize' : '10'
        }
        if not original_properties:
            if self.default_replication_method is self.LOG_BASED:
                return_value['wal2json_message_format'] = '1'

            return_value['default_replication_method'] = self.default_replication_method

        return return_value

    @staticmethod
    def expected_ts_tz(our_ts_tz):
        our_ts_tz_utc = our_ts_tz.astimezone(pytz.utc)
        expected_value = datetime.datetime.strftime(our_ts_tz_utc, "%Y-%m-%dT%H:%M:%S.%f+00:00")

        return expected_value

    @staticmethod
    def expected_ts(our_ts):
        expected_value = datetime.datetime.strftime(our_ts, "%Y-%m-%dT%H:%M:%S.%f+00:00")

        return expected_value

    def select_streams_and_fields(self, conn_id, catalog, select_all_fields: bool = False):
        """Select all streams and all fields within streams or all streams and no fields."""

        schema = menagerie.get_annotated_schema(conn_id, catalog['stream_id'])

        if self.default_replication_method is self.FULL_TABLE:
            additional_md = [{
                "breadcrumb": [], "metadata": {"replication-method": self.FULL_TABLE}
            }]

        elif self.default_replication_method is self.INCREMENTAL:
            additional_md = [{
                "breadcrumb": [], "metadata": {
                    "replication-method": self.INCREMENTAL, "replication-key": "our_integer"
                }
            }]

        else:
            additional_md = [{
                "breadcrumb": [], "metadata": {"replication-method": self.LOG_BASED}
            }]

        non_selected_properties = []
        if not select_all_fields:
            # get a list of all properties so that none are selected
            non_selected_properties = schema.get('annotated-schema', {}).get(
                'properties', {}).keys()

        connections.select_catalog_and_fields_via_metadata(
            conn_id, catalog, schema, additional_md, non_selected_properties)


    def test_run(self):
        """Parametrized automatic fields test running against each replication method."""

        # Test running a sync with no fields selected using full-table replication
        self.default_replication_method = self.FULL_TABLE
        full_table_conn_id = connections.ensure_connection(self)
        self.automatic_fields_test(full_table_conn_id)

        # NB | We expect primary keys and replication keys to have inclusion automatic for
        #      key-based incremental replication. But that is only true for primary keys.
        #      As a result we cannot run a sync with no fields selected. This BUG should not
        #      be carried over into hp-postgres, but will not be fixed for this tap.

        # Test running a sync with no fields selected using key-based incremental replication
        # self.default_replication_method = self.INCREMENTAL
        # incremental_conn_id = connections.ensure_connection(self, original_properties=False)
        # self.automatic_fields_test(incremental_conn_id)

        # Test running a sync with no fields selected using logical replication
        self.default_replication_method = self.LOG_BASED
        with db_utils.get_test_connection('dev') as conn:
            conn.autocommit = True
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                db_utils.ensure_replication_slot(cur, test_db)
        log_based_conn_id = connections.ensure_connection(self, original_properties=False)
        self.automatic_fields_test(log_based_conn_id)


    def automatic_fields_test(self, conn_id):
        """Just testing we can sync with no fields selected. And that automatic fields still get synced."""

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
        print('selecting {} and NO FIELDS within the table'.format(test_table_name))
        self.select_streams_and_fields(conn_id, test_catalog, select_all_fields=False)

        # clear state
        menagerie.set_state(conn_id, {})

        # run sync job 1 and verify exit codes
        sync_job_name = runner.run_sync_mode(self, conn_id)
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # get records
        record_count_by_stream = runner.examine_target_output_file(
            self, conn_id, self.expected_sync_streams(), self.expected_primary_keys()
        )
        records_by_stream = runner.get_records_from_target_output()
        messages = records_by_stream[test_table_name]['messages']

        # expected values
        expected_primary_keys = self.expected_primary_keys()[test_table_name]
        expected_replication_keys = self.expected_replication_keys()[test_table_name]
        expected_automatic_fields = expected_primary_keys.union(expected_replication_keys)

        # collect actual values
        record_messages_keys = [set(message['data'].keys()) for message in messages[1:-1]]

        # verify the message actions match expectations for all replication methods
        self.assertEqual(4, len(messages))
        self.assertEqual('activate_version', messages[0]['action'])
        self.assertEqual('upsert', messages[1]['action'])
        self.assertEqual('upsert', messages[2]['action'])
        self.assertEqual('activate_version', messages[3]['action'])

        # Verify that you get some records for each stream
        self.assertGreater(record_count_by_stream[test_table_name], 0)

        # Verify that only the automatic fields are sent to the target
        for actual_fields in record_messages_keys:
            self.assertSetEqual(expected_automatic_fields, actual_fields)


SCENARIOS.add(PostgresAutomaticFields)
