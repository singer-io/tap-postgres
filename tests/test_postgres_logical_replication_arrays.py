from tap_tester.scenario import (SCENARIOS)
import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner
import os
import datetime
import unittest
import datetime
import pprint
import psycopg2
import psycopg2.extras
from psycopg2.extensions import quote_ident
import pdb
import pytz
import uuid
import json
from functools import reduce
from singer import utils, metadata

import decimal

test_schema_name = "public"
test_table_name = "postgres_logical_replication_array_test"


MAX_SCALE = 38
MAX_PRECISION = 100
expected_schemas = {test_table_name:
                    {'definitions' : {
                        'sdc_recursive_integer_array' : { 'type' : ['null', 'integer', 'array'], 'items' : { '$ref': '#/definitions/sdc_recursive_integer_array'}},
                        'sdc_recursive_number_array' : { 'type' : ['null', 'number', 'array'], 'items' : { '$ref': '#/definitions/sdc_recursive_number_array'}},
                        'sdc_recursive_string_array' : { 'type' : ['null', 'string', 'array'], 'items' : { '$ref': '#/definitions/sdc_recursive_string_array'}},
                        'sdc_recursive_boolean_array' : { 'type' : ['null', 'boolean', 'array'], 'items' : { '$ref': '#/definitions/sdc_recursive_boolean_array'}},
                        'sdc_recursive_timestamp_array' : { 'type' : ['null', 'string', 'array'], 'format' : 'date-time', 'items' : { '$ref': '#/definitions/sdc_recursive_timestamp_array'}},
                        'sdc_recursive_object_array' : { 'type' : ['null','object', 'array'], 'items' : { '$ref': '#/definitions/sdc_recursive_object_array'}},
                        "sdc_recursive_decimal_12_2_array": {"exclusiveMaximum": True,
			                                     "exclusiveMinimum": True,
			                                     "type": ['null', "number", "array"],
			                                     "items": {
				                                 "$ref": "#/definitions/sdc_recursive_decimal_12_2_array"
			                                     },
			                                     "minimum": -10000000000,
			                                     "multipleOf": decimal.Decimal('0.01'),
			                                     "maximum": 10000000000}},
                     'type': 'object',
                     'properties': {'id': {'maximum': 2147483647, 'type': ['integer'], 'minimum': -2147483648},
                                    '_sdc_deleted_at': {'format': 'date-time', 'type': ['null', 'string']},
                                    'our_bit_array': {'items': { '$ref' : '#/definitions/sdc_recursive_boolean_array'},'type': ['null', 'array']},
                                    'our_boolean_array': {'items': { '$ref' : '#/definitions/sdc_recursive_boolean_array'},'type': ['null', 'array']},
                                    'our_cidr_array': {'items':{ '$ref' : '#/definitions/sdc_recursive_string_array'},'type': ['null', 'array']},
                                    'our_citext_array': {'items':{ '$ref' : '#/definitions/sdc_recursive_string_array'},'type': ['null', 'array']},
                                    'our_date_array': {'items':{ '$ref' : '#/definitions/sdc_recursive_timestamp_array'},'type': ['null', 'array']},
                                    'our_decimal_array' : {'type': ['null', 'array'], 'items': {'$ref' : '#/definitions/sdc_recursive_decimal_12_2_array'}},
                                    'our_double_array': {'items': { '$ref' : '#/definitions/sdc_recursive_number_array'},'type': ['null', 'array']},
                                    'our_enum_array': {'type': ['null', 'array'], 'items': { '$ref' : '#/definitions/sdc_recursive_string_array'}},
                                    'our_float_array': {'items': { '$ref' : '#/definitions/sdc_recursive_number_array'},'type': ['null', 'array']},
                                    'our_hstore_array': {'items': { '$ref' : '#/definitions/sdc_recursive_object_array'},'type': ['null', 'array']},
                                    'our_inet_array': {'items': { '$ref' : '#/definitions/sdc_recursive_string_array'},'type': ['null', 'array']},
                                    'our_int_array': {'items': { '$ref' : '#/definitions/sdc_recursive_integer_array'},'type': ['null', 'array']},
                                    'our_int8_array': {'items': { '$ref' : '#/definitions/sdc_recursive_integer_array'},'type': ['null', 'array']},
                                    'our_json_array': {'items': { '$ref' : '#/definitions/sdc_recursive_string_array'},'type': ['null', 'array']},
                                    'our_jsonb_array': {'items': { '$ref' : '#/definitions/sdc_recursive_string_array'},'type': ['null', 'array']},
                                    'our_mac_array': {'items': { '$ref' : '#/definitions/sdc_recursive_string_array'},'type': ['null', 'array']},
                                    'our_money_array': {'items': { '$ref' : '#/definitions/sdc_recursive_string_array'},'type': ['null', 'array']},
                                    'our_real_array': {'items': { '$ref' : '#/definitions/sdc_recursive_number_array'},'type': ['null', 'array']},
                                    'our_smallint_array': {'items': { '$ref' : '#/definitions/sdc_recursive_integer_array'},'type': ['null', 'array']},
                                    'our_string_array': {'items': { '$ref' : '#/definitions/sdc_recursive_string_array'},'type': ['null', 'array']},
                                    'our_text_array': {'items': { '$ref' : '#/definitions/sdc_recursive_string_array'},'type': ['null', 'array']},
                                    'our_time_array': {'items': { '$ref' : '#/definitions/sdc_recursive_string_array'},'type': ['null', 'array']},
                                    'our_ts_tz_array': {'items': { '$ref' : '#/definitions/sdc_recursive_timestamp_array'},'type': ['null', 'array']},
                                    'our_uuid_array': {'items': { '$ref' : '#/definitions/sdc_recursive_string_array'},'type': ['null', 'array']}}
                     }}


def insert_record(cursor, table_name, data):
    our_keys = list(data.keys())
    our_keys.sort()
    our_values = list(map( lambda k: data.get(k), our_keys))


    columns_sql = ", \n ".join(our_keys)
    value_sql_array = []
    for k in our_keys:
        if k == 'our_json_array':
            value_sql_array.append("%s::json[]")
        elif k == 'our_jsonb_array':
            value_sql_array.append("%s::jsonb[]")
        else:
            value_sql_array.append("%s")

    value_sql = ",".join(value_sql_array)

    insert_sql = """ INSERT INTO {}
                            ( {} )
                     VALUES ( {} )""".format(quote_ident(table_name, cursor), columns_sql, value_sql)
    cursor.execute(insert_sql, our_values)

def get_test_connection(dbname=os.getenv('TAP_POSTGRES_DBNAME'), logical_replication=False):
    conn_string = "host='{}' dbname='{}' user='{}' password='{}' port='{}'".format(os.getenv('TAP_POSTGRES_HOST'),
                                                                                   dbname,
                                                                                   os.getenv('TAP_POSTGRES_USER'),
                                                                                   os.getenv('TAP_POSTGRES_PASSWORD'),
                                                                                   os.getenv('TAP_POSTGRES_PORT'))
    if logical_replication:
        return psycopg2.connect(conn_string, connection_factory=psycopg2.extras.LogicalReplicationConnection)
    else:
        return psycopg2.connect(conn_string)


def canonicalized_table_name(schema, table, cur):
    return "{}.{}".format(quote_ident(schema, cur), quote_ident(table, cur))


class PostgresLogicalRepArrays(unittest.TestCase):
    def tearDown(self):
        with get_test_connection('dev') as conn:
            conn.autocommit = True
            # with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            #     cur.execute(""" SELECT pg_drop_replication_slot('stitch') """)


    def setUp(self):
        self.maxDiff = None
        creds = {}
        missing_envs = [x for x in [os.getenv('TAP_POSTGRES_HOST'),
                                    os.getenv('TAP_POSTGRES_USER'),
                                    os.getenv('TAP_POSTGRES_PASSWORD'),
                                    os.getenv('TAP_POSTGRES_PORT'),
                                    os.getenv('TAP_POSTGRES_DBNAME')] if x == None]
        if len(missing_envs) != 0:
            #pylint: disable=line-too-long
            raise Exception("set TAP_POSTGRES_HOST, TAP_POSTGRES_DBNAME, TAP_POSTGRES_USER, TAP_POSTGRES_PASSWORD, TAP_POSTGRES_PORT")

        with get_test_connection('dev') as conn:
            conn.autocommit = True
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute(""" SELECT EXISTS (SELECT 1
                                                FROM  pg_replication_slots
                                               WHERE  slot_name = 'stitch') """)

                old_slot = cur.fetchone()[0]
                with get_test_connection('dev', True) as conn2:
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
CREATE TABLE {} (id                      SERIAL PRIMARY KEY,
                our_bit_array            BIT(1)[],
                our_boolean_array        BOOLEAN[],
                our_cidr_array           CIDR[],
                our_citext_array         CITEXT[],
                our_date_array           DATE[],
                our_decimal_array        NUMERIC(12,2)[],
                our_double_array         DOUBLE PRECISION[],
                our_enum_array           ALIGNMENT[],
                our_float_array          FLOAT[],
                our_hstore_array         HSTORE[],
                our_inet_array           INET[],
                our_int_array            INTEGER[][],
                our_int8_array           INT8[],
                our_json_array           JSON[],
                our_jsonb_array          JSONB[],
                our_mac_array            MACADDR[],
                our_money_array          MONEY[],
                our_real_array           REAL[],
                our_smallint_array       SMALLINT[],
                our_string_array         VARCHAR[],
                our_text_array           TEXT[],
                our_time_array           TIME[],
                our_ts_tz_array          TIMESTAMP WITH TIME ZONE[],
                our_uuid_array           UUID[])
                """.format(canonicalized_table_name(test_schema_name, test_table_name, cur))

                cur.execute(create_table_sql)

    def expected_check_streams(self):
        return { 'dev-public-postgres_logical_replication_array_test'}

    def expected_sync_streams(self):
        return { test_table_name }

    def expected_pks(self):
        return {
            test_table_name : {'id'}
        }

    def tap_name(self):
        return "tap-postgres"

    def name(self):
        return "tap_tester_postgres_logical_replication_arrays"

    def get_type(self):
        return "platform.postgres"

    def get_credentials(self):
        return {'password': os.getenv('TAP_POSTGRES_PASSWORD')}

    def get_properties(self):
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
                                1,
                                msg="unable to locate schemas for connection {}".format(conn_id))

        found_catalog_names = set(map(lambda c: c['tap_stream_id'], found_catalogs))
        diff = self.expected_check_streams().symmetric_difference(found_catalog_names)
        self.assertEqual(len(diff), 0, msg="discovered schemas do not match: {}".format(diff))

        # verify that persisted streams have the correct properties
        test_catalog = found_catalogs[0]

        self.assertEqual(test_table_name, test_catalog['stream_name'])

        print("discovered streams are correct")

        additional_md = [{ "breadcrumb" : [], "metadata" : {'replication-method' : 'LOG_BASED'}}]
        selected_metadata = connections.select_catalog_and_fields_via_metadata(conn_id, test_catalog,
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


        self.assertEqual(record_count_by_stream, { test_table_name: 0})
        records_by_stream = runner.get_records_from_target_output()

        table_version = records_by_stream[test_table_name]['table_version']

        self.assertEqual(records_by_stream[test_table_name]['messages'][0]['action'],
                         'activate_version')

        self.assertEqual(records_by_stream[test_table_name]['messages'][1]['action'],
                         'activate_version')

        # verify state and bookmarks
        state = menagerie.get_state(conn_id)


        bookmark = state['bookmarks']['dev-public-postgres_logical_replication_array_test']
        self.assertIsNone(state['currently_syncing'], msg="expected state's currently_syncing to be None")

        self.assertIsNotNone(bookmark['lsn'],
                             msg="expected bookmark for stream to have an lsn")
        lsn_1 = bookmark['lsn']

        self.assertEqual(bookmark['version'], table_version,
                         msg="expected bookmark for stream to match version")


        #----------------------------------------------------------------------
        # invoke the sync job again after adding a record
        #----------------------------------------------------------------------
        print("inserting a record")

        our_ts_tz = None
        our_date = None
        our_uuid = str(uuid.uuid1())
        with get_test_connection('dev') as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                #insert fixture data 2

                #insert fixture data 1
                our_ts = datetime.datetime(1997, 2, 2, 2, 2, 2, 722184)
                nyc_tz = pytz.timezone('America/New_York')
                our_ts_tz = nyc_tz.localize(our_ts)
                our_date = datetime.date(1998, 3, 4)

                self.rec_1 = {
                    'our_bit_array'         : '{{0,1,1}}',
                    'our_boolean_array'     : '{true}',
                    'our_cidr_array'        : '{{192.168.100.128/25}}',
                    'our_citext_array'      : '{{maGICKal 2}}',
                    'our_date_array'        : '{{{}}}'.format(our_date),
                    'our_decimal_array'     : '{{{}}}'.format(decimal.Decimal('1234567890.01')),
                    'our_double_array'      : '{{1.232323}}',
                    'our_enum_array'        : '{{bad}}',
                    'our_float_array'       : '{{5.23}}',
                    'our_hstore_array'      : """{{"size=>small","name=>betty"}}""",
                    'our_inet_array'        : '{{192.168.100.128/24}}',
                    'our_int_array'         : '{{1,2,3},{4,5,6}}',
                    'our_int8_array'        : '{16,32,64}',
                    'our_json_array'        : [psycopg2.extras.Json({'secret' : 55})],
                    'our_jsonb_array'       : [psycopg2.extras.Json({'secret' : 69})],
                    'our_mac_array'         : '{{08:00:2b:01:02:03}}',
                    'our_money_array'       : '{{$412.1234}}',
                    'our_real_array'        : '{{76.33}}',
                    'our_smallint_array'    : '{{10,20,30},{40,50,60}}',
                    'our_string_array'      : '{{one string, two strings}}',
                    'our_text_array'        : '{{three string, four}}',
                    'our_time_array'        : '{{03:04:05}}',
                    'our_ts_tz_array'       : '{{{}}}'.format(our_ts_tz),
                    'our_uuid_array'        : '{{{}}}'.format(our_uuid)}


                insert_record(cur, test_table_name, self.rec_1)


        sync_job_name = runner.run_sync_mode(self, conn_id)

        # verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        record_count_by_stream = runner.examine_target_output_file(self,
                                                                   conn_id,
                                                                   self.expected_sync_streams(),
                                                                   self.expected_pks())
        self.assertEqual(record_count_by_stream, { test_table_name: 1 })
        records_by_stream = runner.get_records_from_target_output()
        self.assertTrue(len(records_by_stream) > 0)

        for stream, recs in records_by_stream.items():
            # verify the persisted schema was correct
            self.assertEqual(recs['schema'],
                             expected_schemas[stream],
                             msg="Persisted schema did not match expected schema for stream `{}`.".format(stream))

        self.assertEqual(1, len(records_by_stream[test_table_name]['messages']))
        actual_record_1 = records_by_stream[test_table_name]['messages'][0]['data']

        expected_inserted_record = {'id': 1,
                                    '_sdc_deleted_at': None,
                                    'our_bit_array'         : [[False, True, True]],
                                    'our_boolean_array'     : [True],
                                    'our_cidr_array'        : [['192.168.100.128/25']],
                                    'our_citext_array'      : [['maGICKal 2']],
                                    'our_date_array'        : ['1998-03-04T00:00:00+00:00'],
                                    'our_decimal_array'     : [decimal.Decimal('1234567890.01')],
                                    'our_double_array'      : [[decimal.Decimal('1.232323')]],
                                    'our_enum_array'        : [['bad']],
                                    'our_float_array'       : [[decimal.Decimal('5.23')]],
                                    'our_hstore_array'      : [[{'size' : 'small' }, {'name' : 'betty'} ]],
                                    'our_inet_array'        : [['192.168.100.128/24']],
                                    'our_int_array'         : [[1,2,3],[4,5,6]],
                                    'our_int8_array'        : [16,32,64],
                                    'our_json_array'        : [json.dumps({'secret' : 55})],
                                    'our_jsonb_array'       : [json.dumps({'secret' : 69})],
                                    'our_mac_array'         : [['08:00:2b:01:02:03']],
                                    'our_money_array'       : [['$412.12']],
                                    'our_real_array'        : [[decimal.Decimal('76.33')]],
                                    'our_smallint_array'    : [[10,20,30],[40,50,60]],
                                    'our_string_array'      : [['one string', 'two strings']],
                                    'our_text_array'        : [['three string', 'four']],
                                    'our_time_array'        : [['03:04:05']],
                                    'our_ts_tz_array'       : ['1997-02-02T07:02:02.722184+00:00'],
                                    'our_uuid_array'        : ['{}'.format(our_uuid)]

        }

        self.assertEqual(set(actual_record_1.keys()), set(expected_inserted_record.keys()),
                         msg="keys for expected_record_1 are wrong: {}".format(set(actual_record_1.keys()).symmetric_difference(set(expected_inserted_record.keys()))))

        for k,v in actual_record_1.items():
            self.assertEqual(actual_record_1[k], expected_inserted_record[k], msg="{} != {} for key {}".format(actual_record_1[k], expected_inserted_record[k], k))

        self.assertEqual(records_by_stream[test_table_name]['messages'][0]['action'], 'upsert')
        print("inserted record is correct")

        state = menagerie.get_state(conn_id)
        chicken_bookmark = state['bookmarks']['dev-public-postgres_logical_replication_array_test']
        self.assertIsNone(state['currently_syncing'], msg="expected state's currently_syncing to be None")

        self.assertIsNotNone(chicken_bookmark['lsn'],
                             msg="expected bookmark for stream public-postgres_logical_replication_test to have an scn")
        lsn_2 = chicken_bookmark['lsn']

        self.assertTrue(lsn_2 >= lsn_1)

        #table_version does NOT change
        self.assertEqual(chicken_bookmark['version'], table_version,
                         msg="expected bookmark for stream public-postgres_logical_replication_test to match version")


SCENARIOS.add(PostgresLogicalRepArrays)
