import os
import decimal
import unittest
import datetime
import uuid
import json
import copy

import pytz
import psycopg2.extras
from psycopg2.extensions import quote_ident
import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner

import db_utils  # pylint: disable=import-error

test_schema_name = "public"
test_table_name = "postgres_incremental_noreset_test"
reset_table_name = "postgres_incremental_table_reset_test"
expected_schemas = {test_table_name:
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
                                    'our_json': {'type': ['null', 'string']},
                                    'our_double': {'type': ['null', 'number']},
                                    'our_varchar': {'type': ['null', 'string']},
                                    'our_bigint': {'maximum': 9223372036854775807, 'type': ['null', 'integer'], 'minimum': -9223372036854775808},
                                    'id': {'maximum': 2147483647, 'type': ['integer'], 'minimum': -2147483648},
                                    'our_varchar_10': {'type': ['null', 'string'], 'maxLength': 10},
                                    'OUR TIME': {'type': ['null', 'string']},
                                    'OUR TIME TZ': {'type': ['null', 'string']},
                                    'our_bit': {'type': ['null', 'boolean']},
                                    'our_cidr': {'type': ['null', 'string']},
                                    'our_citext': {'type': ['null', 'string']},
                                    'our_inet': {'type': ['null', 'string']},
                                    'our_mac': {'type': ['null', 'string']},
                                    'our_money': {'type': ['null', 'string']}
                     }},
                     reset_table_name:
                    {'definitions' : {
                        'sdc_recursive_integer_array' : { 'type' : ['null', 'integer', 'array'], 'items' : { '$ref': '#/definitions/sdc_recursive_integer_array'}},
                        'sdc_recursive_number_array' : { 'type' : ['null', 'number', 'array'], 'items' : { '$ref': '#/definitions/sdc_recursive_number_array'}},
                        'sdc_recursive_string_array' : { 'type' : ['null', 'string', 'array'], 'items' : { '$ref': '#/definitions/sdc_recursive_string_array'}},
                        'sdc_recursive_boolean_array' : { 'type' : ['null', 'boolean', 'array'], 'items' : { '$ref': '#/definitions/sdc_recursive_boolean_array'}},
                        'sdc_recursive_timestamp_array' : { 'type' : ['null', 'string', 'array'], 'format' : 'date-time', 'items' : { '$ref': '#/definitions/sdc_recursive_timestamp_array'}},
                        'sdc_recursive_object_array' : { 'type' : ['null','object', 'array'], 'items' : { '$ref': '#/definitions/sdc_recursive_object_array'}}
                    },
                     'type': 'object',
                     'properties': {
                                    'id': {'maximum': 2147483647, 'type': ['integer'], 'minimum': -2147483648},
                                    'OUR TS TZ': {'format' : 'date-time', 'type': ['null', 'string']},
                                    'OUR TS': {'format' : 'date-time', 'type': ['null', 'string']},
                                    'our_real': {'type': ['null', 'number']},
                                    'our_uuid': {'type': ['null', 'string']},
                                    'our_store': {'type': ['null', 'object'], 'properties' : {}},
                                    'our_smallint': {'maximum': 32767, 'type': ['null', 'integer'], 'minimum': -32768}}}}

class PostgresIncrementalTableResetTable(unittest.TestCase):

    def setUp(self):
        db_utils.ensure_environment_variables_set()

        db_utils.ensure_db('dev')

        self.maxDiff = None

        with db_utils.get_test_connection('dev') as conn:
            conn.autocommit = True
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:

                cur = db_utils.ensure_fresh_table(conn, cur, test_schema_name, test_table_name)

                create_table_sql = """
CREATE TABLE {} (id            SERIAL PRIMARY KEY,
                our_varchar    VARCHAR,
                our_varchar_10 VARCHAR(10),
                our_text       TEXT,
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
                our_inet       inet,
                our_cidr       cidr,
                our_mac        macaddr,
                our_money      money)
                """.format(db_utils.canonicalized_table_name(cur, test_schema_name, test_table_name))

                cur.execute(create_table_sql)

                # insert fixture data and track expected records
                self.inserted_records = dict()
                self.expected_records = dict()
                self.inserted_records[test_table_name] = []
                self.expected_records[test_table_name] = []
                self.inserted_records[reset_table_name] = []
                self.expected_records[reset_table_name] = []

                nyc_tz = pytz.timezone('America/New_York')
                our_time_offset = "-04:00"

                # record 1
                our_ts = datetime.datetime(1977, 3, 3, 3, 3, 3, 733184)
                our_ts_tz = nyc_tz.localize(our_ts)
                our_time  = datetime.time(10,9,8)
                our_time_tz = our_time.isoformat() + our_time_offset
                our_date = datetime.date(1964, 7, 1)
                my_uuid =  str(uuid.uuid1())
                self.inserted_records[test_table_name].append({
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
                })
                self.expected_records[test_table_name].append({
                    'our_decimal': decimal.Decimal('9876543210.02'),
                    'OUR TIME': str(our_time),
                    'our_text': 'some text 2',
                    'our_bit': True,
                    'our_integer': 44101,
                    'our_double': decimal.Decimal('1.1'),
                    'id': 1,
                    'our_json': '{"nymn": 77}',
                    'our_boolean': True,
                    'our_jsonb': '{"burgers": "good++"}',
                    'our_bigint': 1000001,
                    'OUR TIME TZ': str(our_time_tz),
                    'our_store': {"name" : "betty", "dances" :"floor"},
                    'OUR TS TZ': self.expected_ts_tz(our_ts_tz),
                    'our_smallint': 2,
                    'OUR DATE':     '1964-07-01T00:00:00+00:00',
                    'our_varchar':  'our_varchar 2',
                    'OUR TS':       self.expected_ts(our_ts),
                    'our_uuid':     self.inserted_records[test_table_name][0]['our_uuid'],
                    'our_real':     decimal.Decimal('1.2'),
                    'our_varchar_10': 'varchar_10',
                    'our_citext'  : self.inserted_records[test_table_name][0]['our_citext'],
                    'our_inet'    : self.inserted_records[test_table_name][0]['our_inet'],
                    'our_cidr'    : self.inserted_records[test_table_name][0]['our_cidr'],
                    'our_mac'     : self.inserted_records[test_table_name][0]['our_mac'],
                    'our_money'      : None
                })
                # record 2
                our_ts = datetime.datetime(1987, 2, 2, 2, 2, 2, 722184)
                our_ts_tz = nyc_tz.localize(our_ts)
                our_time  = datetime.time(12,11,10)
                our_time_tz = our_time.isoformat() + our_time_offset
                our_date = datetime.date(1998, 3, 4)
                my_uuid =  str(uuid.uuid1())
                self.inserted_records[test_table_name].append({
                    'our_varchar' : "our_varchar",
                    'our_varchar_10' : "varchar_10", 'our_text' :
                    "some text", 'our_integer' : 44100,
                    'our_smallint' : 1, 'our_bigint' : 1000000,
                    'our_decimal' : decimal.Decimal('1234567890.01'),
                    quote_ident('OUR TS', cur) : our_ts,
                    quote_ident('OUR TS TZ', cur) :  our_ts_tz,
                    quote_ident('OUR TIME', cur) : our_time,
                    quote_ident('OUR TIME TZ', cur) : our_time_tz,
                    quote_ident('OUR DATE', cur) : our_date,
                    'our_double' : decimal.Decimal('1.1'),
                    'our_real' : decimal.Decimal('1.2'),
                    'our_boolean' : True,
                    'our_bit' : '0',
                    'our_json' : json.dumps({'secret' : 55}),
                    'our_jsonb' : json.dumps(6777777),
                    'our_uuid' : my_uuid,
                    'our_store' : 'size=>"small",name=>"betty"',
                    'our_citext': 'cyclops 1',
                    'our_cidr' : '192.168.100.128/25',
                    'our_inet': '192.168.100.128/24',
                    'our_mac' : '08:00:2b:01:02:03',
                    'our_money'     : '$1,445.5678'
                })
                self.expected_records[test_table_name].append({
                    'our_decimal': decimal.Decimal('1234567890.01'),
                    'our_text': 'some text',
                    'our_bit': False,
                    'our_integer': 44100,
                    'our_double': decimal.Decimal('1.1'),
                    'id': 2,
                    'our_json': '{"secret": 55}',
                    'our_boolean': True,
                    'our_jsonb':  self.inserted_records[test_table_name][1]['our_jsonb'],
                    'our_bigint': 1000000,
                    'OUR TS': self.expected_ts(our_ts),
                    'OUR TS TZ': self.expected_ts_tz(our_ts_tz),
                    'OUR TIME': str(our_time),
                    'OUR TIME TZ': str(our_time_tz),
                    'our_store': {"name" : "betty", "size" :"small"},
                    'our_smallint': 1,
                    'OUR DATE': '1998-03-04T00:00:00+00:00',
                    'our_varchar': 'our_varchar',
                    'our_uuid': self.inserted_records[test_table_name][1]['our_uuid'],
                    'our_real': decimal.Decimal('1.2'),
                    'our_varchar_10': 'varchar_10',
                    'our_citext': self.inserted_records[test_table_name][1]['our_citext'],
                    'our_inet'    : self.inserted_records[test_table_name][1]['our_inet'],
                    'our_cidr'    : self.inserted_records[test_table_name][1]['our_cidr'],
                    'our_mac'    : self.inserted_records[test_table_name][1]['our_mac'],
                    'our_money'      : '$1,445.57'
                })
                # record 3
                our_ts = datetime.datetime(1997, 2, 2, 2, 2, 2, 722184)
                our_ts_tz = nyc_tz.localize(our_ts)
                our_time  = datetime.time(12,11,10)
                our_time_tz = our_time.isoformat() + our_time_offset
                our_date = datetime.date(1998, 3, 4)
                my_uuid =  str(uuid.uuid1())
                self.inserted_records[test_table_name].append({
                    'our_varchar' : "our_varchar",
                    'our_varchar_10' : "varchar_10", 'our_text' :
                    "some text", 'our_integer' : 44100,
                    'our_smallint' : 1, 'our_bigint' : 1000000,
                    'our_decimal' : decimal.Decimal('1234567890.01'),
                    quote_ident('OUR TS', cur) : our_ts,
                    quote_ident('OUR TS TZ', cur) :  our_ts_tz,
                    quote_ident('OUR TIME', cur) : our_time,
                    quote_ident('OUR TIME TZ', cur) : our_time_tz,
                    quote_ident('OUR DATE', cur) : our_date,
                    'our_double' : '1.1',
                    'our_real' : decimal.Decimal('1.2'),
                    'our_boolean' : True,
                    'our_bit' : '0',
                    'our_json' : json.dumps({'secret' : 55}),
                    'our_jsonb' : json.dumps(6777777),
                    'our_uuid' : my_uuid,
                    'our_store' : 'size=>"small",name=>"betty"',
                    'our_citext': 'cyclops 1',
                    'our_cidr' : '192.168.100.128/25',
                    'our_inet': '192.168.100.128/24',
                    'our_mac' : '08:00:2b:01:02:03',
                    'our_money'     : '$1,445.5678'
                })
                self.expected_records[test_table_name].append({
                    'our_decimal': decimal.Decimal('1234567890.01'),
                    'our_text': 'some text',
                    'our_bit': False,
                    'our_integer': 44100,
                    'our_double': decimal.Decimal('1.1'),
                    'id': 3,
                    'our_json': '{"secret": 55}',
                    'our_boolean': True,
                    'our_jsonb':  self.inserted_records[test_table_name][1]['our_jsonb'],
                    'our_bigint': 1000000,
                    'OUR TS': self.expected_ts(our_ts),
                    'OUR TS TZ': self.expected_ts_tz(our_ts_tz),
                    'OUR TIME': str(our_time),
                    'OUR TIME TZ': str(our_time_tz),
                    'our_store': {"name" : "betty", "size" :"small"},
                    'our_smallint': 1,
                    'OUR DATE': '1998-03-04T00:00:00+00:00',
                    'our_varchar': 'our_varchar',
                    'our_uuid': self.inserted_records[test_table_name][2]['our_uuid'],
                    'our_real': decimal.Decimal('1.2'),
                    'our_varchar_10': 'varchar_10',
                    'our_citext': self.inserted_records[test_table_name][2]['our_citext'],
                    'our_inet'    : self.inserted_records[test_table_name][2]['our_inet'],
                    'our_cidr'    : self.inserted_records[test_table_name][2]['our_cidr'],
                    'our_mac'    : self.inserted_records[test_table_name][2]['our_mac'],
                    'our_money'      : '$1,445.57'
                })

                for rec in self.inserted_records[test_table_name]:
                    db_utils.insert_record(cur, test_table_name, rec)

                # Repeat the same steps for the table reset
                cur = db_utils.ensure_fresh_table(conn, cur, test_schema_name, reset_table_name)

                create_table_sql = """
CREATE TABLE {} (id            SERIAL PRIMARY KEY,
                our_smallint   SMALLINT,
                "OUR TS"       TIMESTAMP WITHOUT TIME ZONE,
                "OUR TS TZ"    TIMESTAMP WITH TIME ZONE,
                our_real       REAL,
                our_uuid       UUID,
                our_store      HSTORE)
                """.format(db_utils.canonicalized_table_name(cur, test_schema_name, reset_table_name))

                cur.execute(create_table_sql)

                # record 1
                our_ts = datetime.datetime(1977, 3, 3, 3, 3, 3, 733184)
                our_ts_tz = nyc_tz.localize(our_ts)
                our_time  = datetime.time(10,9,8)
                our_time_tz = our_time.isoformat() + our_time_offset
                our_date = datetime.date(1964, 7, 1)
                my_uuid =  str(uuid.uuid1())

                self.inserted_records[reset_table_name].append({
                    'our_smallint' : 2,
                    quote_ident('OUR TS', cur) : our_ts,
                    quote_ident('OUR TS TZ', cur) : our_ts_tz,
                    'our_real' : decimal.Decimal('1.2'),
                    'our_uuid' : my_uuid,
                    'our_store' : 'dances=>"floor",name=>"betty"',
                })
                self.expected_records[reset_table_name].append({
                    'id': 1,
                    'our_store': {"name" : "betty", "dances" :"floor"},
                    'OUR TS TZ': self.expected_ts_tz(our_ts_tz),
                    'our_smallint': 2,
                    'OUR TS':       self.expected_ts(our_ts),
                    'our_uuid':     self.inserted_records[reset_table_name][0]['our_uuid'],
                    'our_real':     decimal.Decimal('1.2')
                })

                # record 2
                our_ts = datetime.datetime(1987, 2, 2, 2, 2, 2, 722184)
                our_ts_tz = nyc_tz.localize(our_ts)
                our_time  = datetime.time(12,11,10)
                our_time_tz = our_time.isoformat() + our_time_offset
                our_date = datetime.date(1998, 3, 4)
                my_uuid =  str(uuid.uuid1())
                self.inserted_records[reset_table_name].append({
                    'our_smallint' : 1, 
                    quote_ident('OUR TS', cur) : our_ts,
                    quote_ident('OUR TS TZ', cur) :  our_ts_tz,
                    'our_real' : decimal.Decimal('1.2'),
                    'our_uuid' : my_uuid,
                    'our_store' : 'size=>"small",name=>"betty"'
                })
                self.expected_records[reset_table_name].append({
                    'id': 2,
                    'OUR TS': self.expected_ts(our_ts),
                    'OUR TS TZ': self.expected_ts_tz(our_ts_tz),
                    'our_store': {"name" : "betty", "size" :"small"},
                    'our_smallint': 1,
                    'our_uuid': self.inserted_records[reset_table_name][1]['our_uuid'],
                    'our_real': decimal.Decimal('1.2')
                })

                # record 3
                our_ts = datetime.datetime(1997, 2, 2, 2, 2, 2, 722184)
                our_ts_tz = nyc_tz.localize(our_ts)
                our_time  = datetime.time(12,11,10)
                our_time_tz = our_time.isoformat() + our_time_offset
                our_date = datetime.date(1998, 3, 4)
                my_uuid =  str(uuid.uuid1())
                self.inserted_records[reset_table_name].append({
                    'our_smallint' : 1, 
                    quote_ident('OUR TS', cur) : our_ts,
                    quote_ident('OUR TS TZ', cur) :  our_ts_tz,
                    'our_real' : decimal.Decimal('1.2'),
                    'our_uuid' : my_uuid,
                    'our_store' : 'size=>"small",name=>"betty"'
                })
                self.expected_records[reset_table_name].append({
                    'id': 3,
                    'OUR TS': self.expected_ts(our_ts),
                    'OUR TS TZ': self.expected_ts_tz(our_ts_tz),
                    'our_store': {"name" : "betty", "size" :"small"},
                    'our_uuid': self.inserted_records[reset_table_name][2]['our_uuid'],
                    'our_real': decimal.Decimal('1.2'),
                    'our_smallint': 1 })

                for rec in self.inserted_records[reset_table_name]:
                    db_utils.insert_record(cur, reset_table_name, rec)

    @staticmethod
    def expected_check_streams():
        return { 'dev-public-postgres_incremental_noreset_test', 'dev-public-postgres_incremental_table_reset_test'}

    @staticmethod
    def expected_sync_streams():
        return { 'postgres_incremental_noreset_test', 'postgres_incremental_table_reset_test' }

    @staticmethod
    def expected_replication_keys():
        return {
            'postgres_incremental_noreset_test' : {'OUR TS TZ'},
            'postgres_incremental_table_reset_test' : {'OUR TS TZ'}
        }
    @staticmethod
    def expected_primary_keys():
        return {
            'postgres_incremental_noreset_test' : {'id'},
            'postgres_incremental_table_reset_test' : {'id'}
        }

    @staticmethod
    def tap_name():
        return "tap-postgres"

    @staticmethod
    def name():
        return "tap_tester_postgres_incremental_table_reset"

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
                'default_replication_method' : 'INCREMENTAL'
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

    @staticmethod
    def check_bookmark_value(self, stream, state, messages, expected_replication_key ):
            # grab bookmarked state
            bookmarktable = 'dev-public-'+stream
            bookmark = state['bookmarks'][f'{bookmarktable}']

            # verify state and bookmarks meet expectations
            self.assertIsNone(state['currently_syncing'])
            self.assertIsNone(bookmark.get('lsn'))
            self.assertEqual(messages['table_version'], bookmark['version'])
            self.assertEqual(expected_replication_key, bookmark['replication_key'])
            self.assertEqual(self.expected_records[f'{stream}'][2][expected_replication_key], bookmark['replication_key_value'])

    def test_run(self):
        conn_id = connections.ensure_connection(self)

        # run in check mode and verify exit codes
        check_job_name = runner.run_check_mode(self, conn_id)
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        # verify basics of discovery are consistent with expectations...

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
        for test_catalog in found_catalogs:
            print(f"selecting {test_catalog['stream_id']} {test_catalog['stream_name']} and all fields within the table'.format(test_catalog['stream_name'])")
            schema_and_metadata = menagerie.get_annotated_schema(conn_id, test_catalog['stream_id'])
            additional_md = [{ "breadcrumb" : [], "metadata" : {'replication-method' : 'INCREMENTAL', 'replication-key' : 'OUR TS TZ'}}]
            connections.select_catalog_and_fields_via_metadata(conn_id, test_catalog, schema_and_metadata, additional_md)

        # clear state
        menagerie.set_state(conn_id, {})

        # run sync job 1 and verify exit codes
        sync_job_name = runner.run_sync_mode(self, conn_id)
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        state = menagerie.get_state(conn_id)

        record_count_by_stream = runner.examine_target_output_file(
           self, conn_id, self.expected_sync_streams(), self.expected_primary_keys())

        records_by_stream = runner.get_records_from_target_output()

        for stream in self.expected_sync_streams():
            messages = records_by_stream[f'{stream}']
            # get records
            # verify the execpted number of records were replicated
            self.assertEqual(3, record_count_by_stream[stream])

            # verify the message actions match expectations
            self.assertEqual(4, len(messages))
            message = messages['messages']
            self.assertEqual('activate_version', message[0]['action'])
            self.assertEqual('upsert', message[1]['action'])
            self.assertEqual('upsert', message[2]['action'])
            self.assertEqual('upsert', message[3]['action'])

            # verify the persisted schema matches expectations
            self.assertEqual(expected_schemas[stream], records_by_stream[stream]['schema'])

            # verify replicated records match expectations
            self.assertDictEqual(self.expected_records[stream][0], message[1]['data'])
            self.assertDictEqual(self.expected_records[stream][1], message[2]['data'])
            self.assertDictEqual(self.expected_records[stream][2], message[3]['data'])

            # verify records are in ascending order by replication-key value
            expected_replication_key = list(self.expected_replication_keys()[stream])[0]
            self.assertLess(message[1]['data'][expected_replication_key], message[2]['data'][expected_replication_key])
            self.assertLess(message[2]['data'][expected_replication_key], message[3]['data'][expected_replication_key])

            print("records are correct")

            #Verify bookmark values
            self.check_bookmark_value(self, stream, state, messages, expected_replication_key )

            # grab bookmarked state
#            bookmarktable = 'dev-public-'+stream
#            bookmark = state['bookmarks'][f'{bookmarktable}']

            # verify state and bookmarks meet expectations
#            self.assertIsNone(state['currently_syncing'])
#            self.assertIsNone(bookmark.get('lsn'))
#            self.assertEqual(messages['table_version'], bookmark['version'])
#            self.assertEqual(expected_replication_key, bookmark['replication_key'])
#            self.assertEqual(self.expected_records[f'{stream}'][2][expected_replication_key], bookmark['replication_key_value'])

        #----------------------------------------------------------------------
        # Overwrite state to mimic table reset
        #----------------------------------------------------------------------

        reset_state = copy.deepcopy(state)

        # modify state for table_reset
        del reset_state['bookmarks']['dev-public-postgres_incremental_table_reset_test']

        menagerie.set_state(conn_id, reset_state)

        # run sync job 2 and verify exit codes
        sync_job_name = runner.run_sync_mode(self, conn_id)
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        #Get the records
        record_count_by_stream = runner.examine_target_output_file(
           self, conn_id, self.expected_sync_streams(), self.expected_primary_keys())

        records_by_stream = runner.get_records_from_target_output()

        # grab bookmarked state
        new_state = menagerie.get_state(conn_id)

        for stream in self.expected_sync_streams():
            messages = records_by_stream[f'{stream}']
            message = messages['messages']

            #Get replication key
            expected_replication_key = list(self.expected_replication_keys()[stream])[0]

            # get records
            # verify the execpted number of records were replicated
            if ( stream ==  test_table_name ):
                self.assertEqual(1, record_count_by_stream[stream])
                self.assertDictEqual(self.expected_records[stream][2], message[1]['data'])
            else:
                # verify replicated records match expectations
                self.assertEqual(3, record_count_by_stream[stream])
                self.assertEqual('upsert', message[2]['action'])
                self.assertEqual('upsert', message[3]['action'])
                self.assertDictEqual(self.expected_records[stream][0], message[1]['data'])
                self.assertDictEqual(self.expected_records[stream][1], message[2]['data'])
                self.assertDictEqual(self.expected_records[stream][2], message[3]['data'])

                # verify records are in ascending order by replication-key value
                self.assertLess(message[1]['data'][expected_replication_key], message[2]['data'][expected_replication_key])
                self.assertLess(message[2]['data'][expected_replication_key], message[3]['data'][expected_replication_key])

            # verify the message actions match expectations
            self.assertEqual(4, len(messages))
            self.assertEqual('activate_version', message[0]['action'])
            self.assertEqual('upsert', message[1]['action'])

            # verify the persisted schema matches expectations
            self.assertEqual(expected_schemas[stream], records_by_stream[stream]['schema'])

            print("records are correct")

            #Verify bookmark values
            self.check_bookmark_value(self, stream, new_state, messages, expected_replication_key )
