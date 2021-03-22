import os
import decimal
import unittest
import datetime
import uuid
import json

import pytz
import psycopg2.extras
from psycopg2.extensions import quote_ident
from singer import metadata
from tap_tester.scenario import (SCENARIOS)
import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner

import db_utils  # pylint: disable=import-error


test_schema_name = "public"
test_table_name = "postgres_incremental_replication_test"
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
                     }}}

class PostgresIncrementalTable(unittest.TestCase):

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
                self.inserted_records = []
                self.expected_records = []

                nyc_tz = pytz.timezone('America/New_York')
                our_time_offset = "-04:00"

                # record 1
                our_ts = datetime.datetime(1977, 3, 3, 3, 3, 3, 733184)
                our_ts_tz = nyc_tz.localize(our_ts)
                our_time  = datetime.time(10,9,8)
                our_time_tz = our_time.isoformat() + our_time_offset
                our_date = datetime.date(1964, 7, 1)
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
                })
                self.expected_records.append({
                    'our_decimal': decimal.Decimal('9876543210.02'),
                    'OUR TIME': '10:09:08',
                    'our_text': 'some text 2',
                    'our_bit': True,
                    'our_integer': 44101,
                    'our_double': decimal.Decimal('1.1'),
                    'id': 1,
                    'our_json': '{"nymn": 77}',
                    'our_boolean': True,
                    'our_jsonb': '{"burgers": "good++"}',
                    'our_bigint': 1000001,
                    'OUR TIME TZ': '10:09:08-04:00',
                    'our_store': {"name" : "betty", "dances" :"floor"},
                    'OUR TS TZ': '1977-03-03T08:03:03.733184+00:00',
                    'our_smallint': 2,
                    'OUR DATE':     '1964-07-01T00:00:00+00:00',
                    'our_varchar':  'our_varchar 2',
                    'OUR TS':       '1977-03-03T03:03:03.733184+00:00',
                    'our_uuid':     self.inserted_records[0]['our_uuid'],
                    'our_real':     decimal.Decimal('1.2'),
                    'our_varchar_10': 'varchar_10',
                    'our_citext'  : self.inserted_records[0]['our_citext'],
                    'our_inet'    : self.inserted_records[0]['our_inet'],
                    'our_cidr'    : self.inserted_records[0]['our_cidr'],
                    'our_mac'     : self.inserted_records[0]['our_mac'],
                    'our_money'      : None
                })

                # record 2
                our_ts = datetime.datetime(1987, 2, 2, 2, 2, 2, 722184)
                our_ts_tz = nyc_tz.localize(our_ts)
                our_time  = datetime.time(12,11,10)
                our_time_tz = our_time.isoformat() + our_time_offset
                our_date = datetime.date(1998, 3, 4)
                my_uuid =  str(uuid.uuid1())
                self.inserted_records.append({
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
                self.expected_records.append({
                    'our_decimal': decimal.Decimal('1234567890.01'),
                    'our_text': 'some text',
                    'our_bit': False,
                    'our_integer': 44100,
                    'our_double': decimal.Decimal('1.1'),
                    'id': 2,
                    'our_json': '{"secret": 55}',
                    'our_boolean': True,
                    'our_jsonb':  self.inserted_records[1]['our_jsonb'],
                    'our_bigint': 1000000,
                    'OUR TS': '1987-02-02T02:02:02.722184+00:00',
                    'OUR TS TZ': '1987-02-02T07:02:02.722184+00:00',
                    'OUR TIME': '12:11:10',
                    'OUR TIME TZ': '12:11:10-04:00',
                    'our_store': {"name" : "betty", "size" :"small"},
                    'our_smallint': 1,
                    'OUR DATE': '1998-03-04T00:00:00+00:00',
                    'our_varchar': 'our_varchar',
                    'our_uuid': self.inserted_records[1]['our_uuid'],
                    'our_real': decimal.Decimal('1.2'),
                    'our_varchar_10': 'varchar_10',
                    'our_citext': self.inserted_records[1]['our_citext'],
                    'our_inet'    : self.inserted_records[1]['our_inet'],
                    'our_cidr'    : self.inserted_records[1]['our_cidr'],
                    'our_mac'    : self.inserted_records[1]['our_mac'],
                    'our_money'      : '$1,445.57'
                })
                # record 3
                our_ts = datetime.datetime(1997, 2, 2, 2, 2, 2, 722184)
                our_ts_tz = nyc_tz.localize(our_ts)
                our_time  = datetime.time(12,11,10)
                our_time_tz = our_time.isoformat() + our_time_offset
                our_date = datetime.date(1998, 3, 4)
                my_uuid =  str(uuid.uuid1())
                self.inserted_records.append({
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
                self.expected_records.append({
                    'our_decimal': decimal.Decimal('1234567890.01'),
                    'our_text': 'some text',
                    'our_bit': False,
                    'our_integer': 44100,
                    'our_double': decimal.Decimal('1.1'),
                    'id': 3,
                    'our_json': '{"secret": 55}',
                    'our_boolean': True,
                    'our_jsonb':  self.inserted_records[1]['our_jsonb'],
                    'our_bigint': 1000000,
                    'OUR TS': '1997-02-02T02:02:02.722184+00:00',
                    'OUR TS TZ': '1997-02-02T07:02:02.722184+00:00',
                    'OUR TIME': '12:11:10',
                    'OUR TIME TZ': '12:11:10-04:00',
                    'our_store': {"name" : "betty", "size" :"small"},
                    'our_smallint': 1,
                    'OUR DATE': '1998-03-04T00:00:00+00:00',
                    'our_varchar': 'our_varchar',
                    'our_uuid': self.inserted_records[2]['our_uuid'],
                    'our_real': decimal.Decimal('1.2'),
                    'our_varchar_10': 'varchar_10',
                    'our_citext': self.inserted_records[2]['our_citext'],
                    'our_inet'    : self.inserted_records[2]['our_inet'],
                    'our_cidr'    : self.inserted_records[2]['our_cidr'],
                    'our_mac'    : self.inserted_records[2]['our_mac'],
                    'our_money'      : '$1,445.57'
                })

                for rec in self.inserted_records:
                    db_utils.insert_record(cur, test_table_name, rec)

    @staticmethod
    def expected_check_streams():
        return { 'dev-public-postgres_incremental_replication_test'}

    @staticmethod
    def expected_sync_streams():
        return { 'postgres_incremental_replication_test' }

    @staticmethod
    def expected_replication_keys():
        return {
            'postgres_incremental_replication_test' : {'OUR TS TZ'}
        }
    @staticmethod
    def expected_primary_keys():
        return {
            'postgres_incremental_replication_test' : {'id'}
        }

    @staticmethod
    def tap_name():
        return "tap-postgres"

    @staticmethod
    def name():
        return "tap_tester_postgres_incremental_replication"

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
                'default_replication_method' : 'LOG_BASED'
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
        self.assertEqual(test_table_name, test_catalog['stream_name'])

        print("discovered streams are correct")

        print('checking discoverd metadata for public-postgres_full_table_test...')
        md = menagerie.get_annotated_schema(conn_id, test_catalog['stream_id'])['metadata']

        self.assertEqual(
            {('properties', 'our_varchar'): {'inclusion': 'available', 'selected-by-default': True, 'sql-datatype': 'character varying'},
             ('properties', 'our_boolean'): {'inclusion': 'available', 'selected-by-default': True, 'sql-datatype': 'boolean'},
             ('properties', 'our_real'): {'inclusion': 'available', 'selected-by-default': True, 'sql-datatype': 'real'},
             ('properties', 'our_uuid'): {'inclusion': 'available', 'selected-by-default': True, 'sql-datatype': 'uuid'},
             ('properties', 'our_bit'): {'inclusion': 'available', 'selected-by-default': True, 'sql-datatype': 'bit'},
             ('properties', 'OUR TS TZ'): {'inclusion': 'available', 'selected-by-default': True, 'sql-datatype': 'timestamp with time zone'},
             ('properties', 'our_varchar_10'): {'inclusion': 'available', 'selected-by-default': True, 'sql-datatype': 'character varying'},
             ('properties', 'our_store'): {'inclusion': 'available', 'selected-by-default': True, 'sql-datatype': 'hstore'},
             ('properties', 'OUR TIME'): {'inclusion': 'available', 'selected-by-default': True, 'sql-datatype': 'time without time zone'},
             ('properties', 'our_decimal'): {'inclusion': 'available', 'selected-by-default': True, 'sql-datatype': 'numeric'},
             ('properties', 'OUR TS'): {'inclusion': 'available', 'selected-by-default': True, 'sql-datatype': 'timestamp without time zone'},
             ('properties', 'our_jsonb'): {'inclusion': 'available', 'selected-by-default': True, 'sql-datatype': 'jsonb'},
             ('properties', 'OUR TIME TZ'): {'inclusion': 'available', 'selected-by-default': True, 'sql-datatype': 'time with time zone'},
             ('properties', 'our_text'): {'inclusion': 'available', 'selected-by-default': True, 'sql-datatype': 'text'},
             ('properties', 'OUR DATE'): {'inclusion': 'available', 'selected-by-default': True, 'sql-datatype': 'date'},
             ('properties', 'our_double'): {'inclusion': 'available', 'selected-by-default': True, 'sql-datatype': 'double precision'},
             (): {'is-view': False, 'schema-name': 'public', 'table-key-properties': ['id'], 'database-name': 'dev', 'row-count': 0},
             ('properties', 'our_bigint'): {'inclusion': 'available', 'selected-by-default': True, 'sql-datatype': 'bigint'},
             ('properties', 'id'): {'inclusion': 'automatic', 'selected-by-default': True, 'sql-datatype': 'integer'},
             ('properties', 'our_json'): {'inclusion': 'available', 'selected-by-default': True, 'sql-datatype': 'json'},
             ('properties', 'our_smallint'): {'inclusion': 'available', 'selected-by-default': True, 'sql-datatype': 'smallint'},
             ('properties', 'our_integer'): {'inclusion': 'available', 'selected-by-default': True, 'sql-datatype': 'integer'},
             ('properties', 'our_cidr'): {'inclusion': 'available', 'selected-by-default': True, 'sql-datatype': 'cidr'},
             ('properties', 'our_citext'): {'inclusion': 'available', 'selected-by-default': True, 'sql-datatype': 'citext'},
             ('properties', 'our_inet'): {'inclusion': 'available', 'selected-by-default': True, 'sql-datatype': 'inet'},
             ('properties', 'our_mac'): {'inclusion': 'available', 'selected-by-default': True, 'sql-datatype': 'macaddr'},
             ('properties', 'our_money'): {'inclusion': 'available', 'selected-by-default': True, 'sql-datatype': 'money'}},
            metadata.to_map(md))

        additional_md = [{ "breadcrumb" : [], "metadata" : {'replication-method' : 'INCREMENTAL', 'replication-key' : 'OUR TS TZ'}}]
        _ = connections.select_catalog_and_fields_via_metadata(conn_id, test_catalog,
                                                               menagerie.get_annotated_schema(conn_id, test_catalog['stream_id']),
                                                               additional_md)

        # clear state
        menagerie.set_state(conn_id, {})

        # Sync Job 1
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        record_count_by_stream = runner.examine_target_output_file(self,
                                                                   conn_id,
                                                                   self.expected_sync_streams(),
                                                                   self.expected_primary_keys())

        self.assertEqual(record_count_by_stream, { test_table_name: 3})
        records_by_stream = runner.get_records_from_target_output()
        table_version = records_by_stream[test_table_name]['table_version']
        messages = records_by_stream[test_table_name]['messages']

        # verify the message actions match expectations
        self.assertEqual(4, len(messages))
        self.assertEqual(messages[0]['action'], 'activate_version')
        self.assertEqual(messages[1]['action'], 'upsert')
        self.assertEqual(messages[2]['action'], 'upsert')
        self.assertEqual(messages[3]['action'], 'upsert')

        # # verifications about individual records # TODO Do we need this if checking keys and values of records?
        # for table_name, recs in records_by_stream.items():
        #     # verify the persisted schema was correct
        #     self.assertEqual(recs['schema'],
        #                      expected_schemas[table_name],
        #                      msg="Persisted schema did not match expected schema for table `{}`.".format(table_name))

        self.assertDictEqual(self.expected_records[0], messages[1]['data'])
        self.assertDictEqual(self.expected_records[1], messages[2]['data'])
        self.assertDictEqual(self.expected_records[2], messages[3]['data'])
        print("records are correct")

        # verify state and bookmarks
        state = menagerie.get_state(conn_id)
        bookmark = state['bookmarks']['dev-public-postgres_incremental_replication_test']
        self.assertIsNone(state['currently_syncing'], msg="expected state's currently_syncing to be None")
        self.assertIsNone(bookmark.get('lsn'),
                          msg="expected bookmark for stream ROOT-CHICKEN to have NO lsn because we are using incremental replication")
        self.assertEqual(bookmark['version'], table_version,
                         msg="expected bookmark for stream ROOT-CHICKEN to match version")

        #----------------------------------------------------------------------
        # invoke the sync job AGAIN following various manipulations to the data
        #----------------------------------------------------------------------

        # make changes to the database between syncs
        with db_utils.get_test_connection('dev') as conn:
            conn.autocommit = True
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:

                # TODO DO all changes at once
                # [x] Insert a record with a lower replication-key value (it shouldn't replicate)
                # [x] Insert a record with a higher replication-key value
                # [x] Insert a record with a higher replication-key value that we will delete
                # [x] Update a record with a higher replication-key value
                # [] Update a record with a lower replication-key value
                # [x] Delete a newly inserted record
                # [] Delete a pre-existing record

                # insert more fixture data ...
                # a record with a replication-key value that is lower than the previous bookmark
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
                    'OUR TS' : '1996-04-04T04:04:04.733184+00:00',
                    'OUR TS TZ' : '1996-04-04T08:04:04.733184+00:00',
                    'OUR TIME' : '06:06:06',
                    'OUR TIME TZ' : '06:06:06-04:00',
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
                    'our_money': '$0.99'
                })
                # a record with a replication-key value that is higher than the previous bookmark
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
                    'OUR TS': '2007-01-01T12:12:12.222111+00:00',
                    'OUR TS TZ': '2007-01-01T17:12:12.222111+00:00',
                    'OUR TIME': '12:11:10',
                    'OUR TIME TZ': '12:11:10-04:00',
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
                    'our_money' : None
                })
                # a record with a replication-key value that is higher than the previous bookmark (to be deleted)
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
                    'OUR TS': '2111-01-01T12:12:12.222111+00:00',
                    'OUR TS TZ': '2111-01-01T17:12:12.222111+00:00',
                    'OUR TIME': '12:11:10',
                    'OUR TIME TZ': '12:11:10-04:00',
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
                    'our_money' : None
                })

                db_utils.insert_record(cur, test_table_name, self.inserted_records[-3])
                db_utils.insert_record(cur, test_table_name, self.inserted_records[-2])
                db_utils.insert_record(cur, test_table_name, self.inserted_records[-1])

                # update a record with a replication-key value that is higher than the previous bookmark
                canon_table_name = db_utils.canonicalized_table_name(cur, test_schema_name, test_table_name)
                record_pk = 1
                our_ts = datetime.datetime(2021, 4, 4, 4, 4, 4, 733184)
                our_ts_tz = nyc_tz.localize(our_ts)
                updated_data = {
                    "OUR TS TZ": our_ts_tz,
                    "our_double": decimal.Decimal("6.6"),
                    "our_money": "$0.00"
                }
                self.expected_records[0]["OUR TS TZ"] = '2021-04-04T08:04:04.733184+00:00'
                self.expected_records[0]["our_double"] = decimal.Decimal("6.6")
                self.expected_records[0]["our_money"] = "$0.00"
                db_utils.update_record(cur, canon_table_name, record_pk, updated_data)

                # update a record with a replication-key value that is lower than the previous bookmark
                canon_table_name = db_utils.canonicalized_table_name(cur, test_schema_name, test_table_name)
                record_pk = 2
                our_ts = datetime.datetime(1990, 4, 4, 4, 4, 4, 733184)
                our_ts_tz = nyc_tz.localize(our_ts)
                updated_data = {
                    "OUR TS TZ": our_ts_tz,
                    "our_double": decimal.Decimal("6.6"),
                    "our_money": "$0.00"
                }
                self.expected_records[0]["OUR TS TZ"] = '2021-04-04T08:04:04.733184+00:00'
                self.expected_records[0]["our_double"] = decimal.Decimal("6.6")
                self.expected_records[0]["our_money"] = "$0.00"
                db_utils.update_record(cur, canon_table_name, record_pk, updated_data)

                # delete some records
                record_pk = 5
                db_utils.delete_record(cur, canon_table_name, record_pk)

        # Sync Job 2
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # grab records
        record_count_by_stream = runner.examine_target_output_file(
            self, conn_id, self.expected_sync_streams(), self.expected_primary_keys()
        )
        records_by_stream = runner.get_records_from_target_output()
        messages = records_by_stream[test_table_name]['messages']

        # verify the expected number of records were synced
        self.assertEqual(3, record_count_by_stream[test_table_name])

        # verify the message actions match expectations
        self.assertEqual(messages[0]['action'], 'activate_version')
        self.assertEqual(messages[1]['action'], 'upsert')
        self.assertEqual(messages[2]['action'], 'upsert')
        self.assertEqual(messages[3]['action'], 'upsert')

        # verify the first record was the bookmarked record from the previous sync
        self.assertDictEqual(self.expected_records[2], messages[1]['data'])


        # verify the expected updated record with a higher replication-key value was replicated
        self.assertDictEqual(self.expected_records[0], messages[2]['data'])

        # verify the expected inserted record with a lower replication-key value was NOT replicated
        actual_record_ids = [message['data']['id'] for message in messages[1:]]
        expected_record_id = self.expected_records[3]['id']
        self.assertNotIn(expected_record_id, actual_record_ids)

        # verify the deleted  record with a lower replication-key value was NOT replicated
        expected_record_id = self.expected_records[4]['id']
        self.assertNotIn(expected_record_id, actual_record_ids)

        # verify the expected updated record with a lower replication-key value was NOT replicated
        expected_record_id = self.expected_records[1]['id']
        self.assertNotIn(expected_record_id, actual_record_ids)

        # verify the expected inserted record with a higher replication-key value was replicated
        self.assertDictEqual(self.expected_records[5], messages[3]['data'])

        #table version did NOT change
        self.assertEqual(records_by_stream[test_table_name]['table_version'], table_version)

        #----------------------------------------------------------------------
        # invoke the sync job AGAIN and get 1 record(the one matching the bookmark)
        #----------------------------------------------------------------------

        #Sync Job 2
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # get records
        record_count_by_stream = runner.examine_target_output_file(
            self, conn_id,self.expected_sync_streams(), self.expected_primary_keys()
        )
        records_by_stream = runner.get_records_from_target_output()
        messages = records_by_stream[test_table_name]['messages']

        # verify the expected number of records were replicated
        self.assertEqual(1, record_count_by_stream[test_table_name])

        # verify messages match our expectations
        self.assertEqual(2, len(messages))
        self.assertEqual(messages[0]['action'], 'activate_version')
        self.assertEqual(messages[1]['action'], 'upsert')
        self.assertEqual(records_by_stream[test_table_name]['table_version'], table_version)

        # verify only the previously bookmarked record was synced
        self.assertDictEqual(self.expected_records[5], messages[1]['data'])

        # get bookmark
        state = menagerie.get_state(conn_id)
        bookmark = state['bookmarks']['dev-public-postgres_incremental_replication_test']
        expected_replication_key = list(self.expected_replication_keys()[test_table_name])[0]

        # verify the bookmarked state matches our expectations
        self.assertIsNone(bookmark.get('lsn'))
        self.assertEqual(bookmark['version'], table_version)
        self.assertEqual(bookmark['replication_key'], expected_replication_key)
        self.assertEqual(bookmark['replication_key_value'], self.expected_records[5][expected_replication_key])


SCENARIOS.add(PostgresIncrementalTable)
