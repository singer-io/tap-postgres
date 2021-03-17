import os
import unittest

import psycopg2.extras
from psycopg2.extensions import quote_ident
from singer import metadata
from tap_tester.scenario import (SCENARIOS)
import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner

import db_utils  # pylint: disable=import-error

expected_schemas = {'chicken_view':
                    {'properties': {'fk_id': {'maximum': 9223372036854775807, 'type': ['null', 'integer'],
                                              'minimum': -9223372036854775808},
                                    'size': {'type': ['null', 'string']},
                                    'name': {'type': ['null', 'string']},
                                    'id': {'maximum': 2147483647, 'type': ['null', 'integer'],
                                           'minimum': -2147483648},
                                    'age': {'maximum': 2147483647, 'type': ['null', 'integer'],
                                            'minimum': -2147483648}},
                     'type': 'object',
                     'definitions' : {
                         'sdc_recursive_integer_array' : { 'type' : ['null', 'integer', 'array'], 'items' : { '$ref': '#/definitions/sdc_recursive_integer_array'}},
                        'sdc_recursive_number_array' : { 'type' : ['null', 'number', 'array'], 'items' : { '$ref': '#/definitions/sdc_recursive_number_array'}},
                        'sdc_recursive_string_array' : { 'type' : ['null', 'string', 'array'], 'items' : { '$ref': '#/definitions/sdc_recursive_string_array'}},
                        'sdc_recursive_boolean_array' : { 'type' : ['null', 'boolean', 'array'], 'items' : { '$ref': '#/definitions/sdc_recursive_boolean_array'}},
                        'sdc_recursive_timestamp_array' : { 'type' : ['null', 'string', 'array'], 'format' : 'date-time', 'items' : { '$ref': '#/definitions/sdc_recursive_timestamp_array'}},
                        'sdc_recursive_object_array' : { 'type' : ['null','object', 'array'], 'items' : { '$ref': '#/definitions/sdc_recursive_object_array'}}
                     }}}


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



test_schema_name = "public"
test_table_name_1 = "postgres_views_full_table_replication_test"
test_table_name_2 = "postgres_views_full_table_replication_test_2"
test_view = 'chicken_view'

class PostgresViewsFullTable(unittest.TestCase):
    def setUp(self):
        db_utils.ensure_environment_variables_set()

        db_utils.ensure_db()

        self.maxDiff = None

        with db_utils.get_test_connection() as conn:
            conn.autocommit = True
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                for table in [test_table_name_1, test_table_name_2]:
                    old_table = cur.execute("""SELECT EXISTS (
                                                  SELECT 1
                                                    FROM  information_schema.tables
                                                   WHERE table_schema = %s
                                                     AND  table_name =  %s)""",
                                            [test_schema_name, table])
                    old_table = cur.fetchone()[0]
                    if old_table:
                        cur.execute("DROP TABLE {} CASCADE".format(canonicalized_table_name(test_schema_name, table, cur)))


                cur.execute("""DROP VIEW IF EXISTS {} """.format(quote_ident(test_view, cur)))
                cur.execute("""CREATE TABLE {}
                                (id SERIAL PRIMARY KEY,
                                 name VARCHAR,
                                 size VARCHAR) """.format(canonicalized_table_name(test_schema_name, test_table_name_1, cur)))

                cur.execute("""CREATE TABLE {}
                                (fk_id bigint,
                                 age integer) """.format(canonicalized_table_name(test_schema_name, test_table_name_2, cur)))

                cur.execute("""CREATE VIEW {} AS
                            (SELECT *
                              FROM {}
                              join {}
                                on {}.id = {}.fk_id
                    )""".format(quote_ident(test_view, cur),
                                canonicalized_table_name(test_schema_name, test_table_name_1, cur),
                                canonicalized_table_name(test_schema_name, test_table_name_2, cur),
                                canonicalized_table_name(test_schema_name, test_table_name_1, cur),
                                canonicalized_table_name(test_schema_name, test_table_name_2, cur)))

                self.rec_1 = { 'name' : 'fred', 'size' : 'big' }
                insert_record(cur, test_table_name_1, self.rec_1)

                cur.execute("SELECT id FROM {}".format(canonicalized_table_name(test_schema_name, test_table_name_1, cur)))
                fk_id = cur.fetchone()[0]

                self.rec_2 = { 'fk_id' : fk_id, 'age' : 99 }
                insert_record(cur, test_table_name_2, self.rec_2)


    @staticmethod
    def expected_check_streams():
        return { 'postgres-public-chicken_view'}

    @staticmethod
    def expected_sync_streams():
        return { 'chicken_view' }

    @staticmethod
    def name():
        return "tap_tester_postgres_views_full_table"

    @staticmethod
    def expected_pks():
        return {
            'chicken_view' : {'id'}
        }

    @staticmethod
    def tap_name():
        return "tap-postgres"

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
                'user' : os.getenv('TAP_POSTGRES_USER')
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

        self.assertEqual(len(found_catalogs),
                         1,
                         msg="unable to locate schemas for connection {}".format(conn_id))

        found_catalog_names = set(map(lambda c: c['tap_stream_id'], found_catalogs))
        diff = self.expected_check_streams().symmetric_difference(found_catalog_names)
        self.assertEqual(len(diff), 0, msg="discovered schemas do not match: {}".format(diff))

        # verify that persisted streams have the correct properties
        chicken_catalog = found_catalogs[0]

        self.assertEqual('chicken_view', chicken_catalog['stream_name'])
        print("discovered streams are correct")

        print('checking discoverd metadata for ROOT-CHICKEN_VIEW')
        md = menagerie.get_annotated_schema(conn_id, chicken_catalog['stream_id'])['metadata']

        self.assertEqual(
            {(): {'database-name': 'postgres', 'is-view': True, 'row-count': 0, 'schema-name': 'public', 'table-key-properties': []},
             ('properties', 'fk_id'): {'inclusion': 'available', 'sql-datatype': 'bigint', 'selected-by-default': True},
             ('properties', 'name'): {'inclusion': 'available', 'sql-datatype': 'character varying', 'selected-by-default': True},
             ('properties', 'age'): {'inclusion': 'available', 'sql-datatype': 'integer', 'selected-by-default': True},
             ('properties', 'size'): {'inclusion': 'available', 'sql-datatype': 'character varying', 'selected-by-default': True},
             ('properties', 'id'): {'inclusion': 'available', 'sql-datatype': 'integer', 'selected-by-default': True}},
            metadata.to_map(md))


        # 'ID' selected as view-key-properties
        replication_md = [{"breadcrumb": [], "metadata": {'replication-key': None, "replication-method" : "FULL_TABLE", 'view-key-properties': ["id"]}}]

        connections.select_catalog_and_fields_via_metadata(conn_id, chicken_catalog,
                                                           menagerie.get_annotated_schema(conn_id, chicken_catalog['stream_id']),
                                                           replication_md)

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


        self.assertEqual(record_count_by_stream, { 'chicken_view': 1})
        records_by_stream = runner.get_records_from_target_output()

        table_version = records_by_stream['chicken_view']['table_version']
        self.assertEqual(records_by_stream['chicken_view']['messages'][0]['action'], 'activate_version')
        self.assertEqual(records_by_stream['chicken_view']['messages'][1]['action'], 'upsert')
        self.assertEqual(records_by_stream['chicken_view']['messages'][2]['action'], 'activate_version')

        # verifications about individual records
        for stream, recs in records_by_stream.items():
            # verify the persisted schema was correct
            self.assertEqual(recs['schema'],
                             expected_schemas[stream],
                             msg="Persisted schema did not match expected schema for stream `{}`.".format(stream))

        actual_chicken_record = records_by_stream['chicken_view']['messages'][1]['data']

        expected_chicken_record = {'id': 1, 'fk_id': 1, 'name': 'fred', 'age': 99, 'size' : 'big'}
        self.assertEqual(actual_chicken_record,
                         expected_chicken_record,
                         msg="Expected `various_types` upsert record data to be {}, but target output {}".format(expected_chicken_record, actual_chicken_record))

        print("records are correct")

        # verify state and bookmarks
        state = menagerie.get_state(conn_id)

        chicken_bookmark = state['bookmarks']['postgres-public-chicken_view']
        self.assertIsNone(state['currently_syncing'], msg="expected state's currently_syncing to be None")
        self.assertEqual(chicken_bookmark['version'], table_version,
                         msg="expected bookmark for stream ROOT-CHICKEN to match version")


SCENARIOS.add(PostgresViewsFullTable)
