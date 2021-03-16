import os
import decimal
import unittest
import datetime
import uuid
import json

import psycopg2.extras
from psycopg2.extensions import quote_ident
import pytz
from singer import metadata
from tap_tester.scenario import (SCENARIOS)
import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner

import db_utils


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

def canonicalized_table_name(schema, table, cur):
    return "{}.{}".format(quote_ident(schema, cur), quote_ident(table, cur))

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


class PostgresFullTable(unittest.TestCase):

    def setUp(self):
        db_utils.ensure_db("dev")
        self.maxDiff = None

        missing_envs = [x for x in [os.getenv('TAP_POSTGRES_HOST'),
                                    os.getenv('TAP_POSTGRES_USER'),
                                    os.getenv('TAP_POSTGRES_PASSWORD'),
                                    os.getenv('TAP_POSTGRES_PORT'),
                                    os.getenv('TAP_POSTGRES_DBNAME')] if x is None]
        if len(missing_envs) != 0:
            #pylint: disable=line-too-long
            raise Exception("set TAP_POSTGRES_HOST, TAP_POSTGRES_DBNAME, TAP_POSTGRES_USER, TAP_POSTGRES_PASSWORD, TAP_POSTGRES_PORT")

        with db_utils.get_test_connection('dev') as conn:
            conn.autocommit = True
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:

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
                """.format(canonicalized_table_name(test_schema_name, test_table_name, cur), NUMERIC_PRECISION, NUMERIC_SCALE)

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
                }

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
                }

                insert_record(cur, test_table_name, self.rec_2)

                self.rec_3 = {'our_decimal' : decimal.Decimal('NaN'),
                              'our_double' : float('nan'),
                              'our_real' : float('-inf')
                }

                insert_record(cur, test_table_name, self.rec_3)


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
        print('Catalog', test_catalog)
        self.assertEqual('postgres_full_table_replication_test', test_catalog['stream_name'])

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
             ('properties', 'our_citext'): {'inclusion': 'available', 'selected-by-default': True, 'sql-datatype': 'citext'},
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
             ('properties', 'our_inet'): {'inclusion': 'available', 'selected-by-default': True, 'sql-datatype': 'inet'},
             ('properties', 'our_cidr'): {'inclusion': 'available', 'selected-by-default': True, 'sql-datatype': 'cidr'},
             ('properties', 'our_mac'): {'inclusion': 'available', 'selected-by-default': True, 'sql-datatype': 'macaddr'},
             ('properties', 'our_alignment_enum'): {'inclusion': 'available', 'selected-by-default': True, 'sql-datatype': 'alignment'},
             ('properties', 'our_money'): {'inclusion': 'available', 'selected-by-default': True, 'sql-datatype': 'money'}},
            metadata.to_map(md))

        additional_md = [{ "breadcrumb" : [], "metadata" : {'replication-method' : 'FULL_TABLE'}}]
        _ = connections.select_catalog_and_fields_via_metadata(conn_id, test_catalog,
                                                               menagerie.get_annotated_schema(conn_id, test_catalog['stream_id']),
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


        self.assertEqual(record_count_by_stream, { 'postgres_full_table_replication_test': 3})
        records_by_stream = runner.get_records_from_target_output()

        table_version = records_by_stream['postgres_full_table_replication_test']['table_version']

        self.assertEqual(records_by_stream['postgres_full_table_replication_test']['messages'][0]['action'],
                         'activate_version')

        self.assertEqual(records_by_stream['postgres_full_table_replication_test']['messages'][1]['action'],
                         'upsert')

        self.assertEqual(records_by_stream['postgres_full_table_replication_test']['messages'][2]['action'],
                         'upsert')

        self.assertEqual(records_by_stream['postgres_full_table_replication_test']['messages'][3]['action'],
                         'upsert')

        self.assertEqual(records_by_stream['postgres_full_table_replication_test']['messages'][4]['action'],
                         'activate_version')

        # verifications about individual records
        for table_name, recs in records_by_stream.items():
            # verify the persisted schema was correct
            self.assertEqual(recs['schema'],
                             expected_schemas[table_name],
                             msg="Persisted schema did not match expected schema for table `{}`.".format(table_name))

        expected_record_1 = {'our_decimal': decimal.Decimal('.01'),
                             'our_text': 'some text',
                             'our_bit': False,
                             'our_integer': 44100,
                             'our_double': decimal.Decimal('1.1'),
                             'id': 1,
                             'our_json': '{"secret": 55}',
                             'our_boolean': True,
                             'our_jsonb': '{"burgers": "good"}',
                             'our_bigint': 1000000,
                             'OUR TS': '1997-02-02T02:02:02.722184+00:00',
                             'OUR TS TZ': '1997-02-02T07:02:02.722184+00:00',
                             'OUR TIME': '12:11:10',
                             'OUR TIME TZ': '12:11:10-04:00',
                             'our_store': {"name" : "betty", "size" :"small"},
                             'our_smallint': 1,
                             'OUR DATE': '1998-03-04T00:00:00+00:00',
                             'our_varchar': 'our_varchar',
                             'our_uuid': self.rec_1['our_uuid'],
                             'our_real': decimal.Decimal('1.2'),
                             'our_varchar_10': 'varchar_10',
                             'our_citext'    : self.rec_1['our_citext'],
                             'our_inet'    : self.rec_1['our_inet'],
                             'our_cidr'    : self.rec_1['our_cidr'],
                             'our_mac'    : self.rec_1['our_mac'],
                             'our_alignment_enum' : self.rec_1['our_alignment_enum'],
                             'our_money'      : '$100.11'
        }

        expected_record_2 = {'our_decimal': decimal.Decimal('.02'),
                             'OUR TIME': '10:09:08',
                             'our_text': 'some text 2',
                             'our_bit': True,
                             'our_integer': 44101,
                             'our_double': decimal.Decimal('1.1'),
                             'id': 2,
                             'our_json': '["nymn 77"]',
                             'our_boolean': True,
                             'our_jsonb': '{"burgers": "good++"}',
                             'our_bigint': 1000001,
                             'OUR TIME TZ': '10:09:08-04:00',
                             'our_store': {"name" : "betty", "dances" :"floor"},
                             'OUR TS TZ': '1987-03-03T08:03:03.733184+00:00',
                             'our_smallint': 2,
                             'OUR DATE': '1964-07-01T00:00:00+00:00',
                             'our_varchar': 'our_varchar 2',
                             'OUR TS': '1987-03-03T03:03:03.733184+00:00',
                             'our_uuid': self.rec_2['our_uuid'],
                             'our_real': decimal.Decimal('1.2'),
                             'our_varchar_10': 'varchar_10',
                             'our_citext'    : self.rec_2['our_citext'],
                             'our_inet'    : self.rec_2['our_inet'],
                             'our_cidr'    : self.rec_2['our_cidr'],
                             'our_mac'     : self.rec_2['our_mac'],
                             'our_alignment_enum' : None,
                             'our_money':    None
        }

        actual_record_1 = records_by_stream['postgres_full_table_replication_test']['messages'][1]
        self.assertEqual(set(actual_record_1['data'].keys()), set(expected_record_1.keys()),
                         msg="keys for expected_record_1 are wrong: {}".format(set(actual_record_1.keys()).symmetric_difference(set(expected_record_1.keys()))))

        for k,v in actual_record_1['data'].items():
            self.assertEqual(actual_record_1['data'][k], expected_record_1[k], msg="{} != {} for key {}".format(actual_record_1['data'][k], expected_record_1[k], k))

        actual_record_2 = records_by_stream['postgres_full_table_replication_test']['messages'][2]
        self.assertEqual(set(actual_record_2['data'].keys()), set(expected_record_2.keys()),
                         msg="keys for expected_record_2 are wrong: {}".format(set(actual_record_2.keys()).symmetric_difference(set(expected_record_2.keys()))))

        for k,v in actual_record_2['data'].items():
            self.assertEqual(actual_record_2['data'][k], expected_record_2[k], msg="{} != {} for key {}".format(actual_record_2['data'][k], expected_record_2[k], k))

        #We cast NaN's, +Inf, -Inf to NULL as wal2json does not support them and now we are at least consistent(ly wrong)
        expected_record_3 = {'our_decimal' : None,
                             'our_double' : None,
                             'our_real' : None}
        actual_record_3 = records_by_stream['postgres_full_table_replication_test']['messages'][3]
        for k,v in expected_record_3.items():
            self.assertEqual(actual_record_3['data'][k], v, msg="{} != {} for key {}".format(actual_record_3['data'][k], v, k))


        print("records are correct")

        # verify state and bookmarks
        state = menagerie.get_state(conn_id)

        bookmark = state['bookmarks']['dev-public-postgres_full_table_replication_test']
        self.assertIsNone(state['currently_syncing'], msg="expected state's currently_syncing to be None")


        self.assertIsNone(bookmark.get('lsn'),
                          msg="expected bookmark for stream ROOT-CHICKEN to have NO lsn because we are using full-table replication")
        self.assertEqual(bookmark['version'], table_version,
                         msg="expected bookmark for stream ROOT-CHICKEN to match version")

        #----------------------------------------------------------------------
        # invoke the sync job AGAIN and get the same 3 records
        #----------------------------------------------------------------------
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        record_count_by_stream = runner.examine_target_output_file(self,
                                                                   conn_id,
                                                                   self.expected_sync_streams(),
                                                                   self.expected_pks())

        self.assertEqual(record_count_by_stream, { 'postgres_full_table_replication_test': 3})
        records_by_stream = runner.get_records_from_target_output()

        new_table_version = records_by_stream['postgres_full_table_replication_test']['table_version']

        self.assertEqual(records_by_stream['postgres_full_table_replication_test']['messages'][0]['action'],
                         'upsert')

        self.assertEqual(records_by_stream['postgres_full_table_replication_test']['messages'][1]['action'],
                         'upsert')

        self.assertEqual(records_by_stream['postgres_full_table_replication_test']['messages'][2]['action'],
                         'upsert')

        self.assertEqual(records_by_stream['postgres_full_table_replication_test']['messages'][3]['action'],
                         'activate_version')

        new_table_version = records_by_stream['postgres_full_table_replication_test']['table_version']

        self.assertGreater(new_table_version, table_version,
                           msg="table version {} didn't increate from {} on the second run".format(new_table_version, table_version))

        # verifications about individual records
        for stream, recs in records_by_stream.items():
            # verify the persisted schema was correct
            self.assertEqual(recs['schema'],
                             expected_schemas[stream],
                             msg="Persisted schema did not match expected schema for stream `{}`.".format(stream))




SCENARIOS.add(PostgresFullTable)
