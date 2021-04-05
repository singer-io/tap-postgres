import os
import datetime
import unittest
import decimal
import uuid
import json

from psycopg2.extensions import quote_ident
import psycopg2.extras
import pytz
from tap_tester.scenario import (SCENARIOS)
import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner

import db_utils  # pylint: disable=import-error


test_schema_name = "public"
test_table_name = "postgres_discovery_test"
test_db = "discovery1"


class PostgresDiscovery(unittest.TestCase):
    AUTOMATIC_FIELDS = "automatic"
    REPLICATION_KEYS = "valid-replication-keys"
    PRIMARY_KEYS = "table-key-properties"
    FOREIGN_KEYS = "table-foreign-key-properties"
    REPLICATION_METHOD = "forced-replication-method"
    API_LIMIT = "max-row-limit"
    INCREMENTAL = "INCREMENTAL"
    FULL_TABLE = "FULL_TABLE"
    LOG_BASED = "LOG_BASED"

    UNSUPPORTED_TYPES = {
        "BIGSERIAL",
        "BIT VARYING",
        "BOX",
        "BYTEA",
        "CIRCLE",
        "INTERVAL",
        "LINE",
        "LSEG",
        "PATH",
        "PG_LSN",
        "POINT",
        "POLYGON",
        "SERIAL",
        "SMALLSERIAL",
        "TSQUERY",
        "TSVECTOR",
        "TXID_SNAPSHOT",
        "XML",
    }
    default_replication_method = ""

    def tearDown(self):
        pass
        # with db_utils.get_test_connection(test_db) as conn:
        #     conn.autocommit = True
        #     with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        #         cur.execute(""" SELECT pg_drop_replication_slot('stitch') """)

    def setUp(self):
        db_utils.ensure_environment_variables_set()

        db_utils.ensure_db(test_db)
        self.maxDiff = None

        with db_utils.get_test_connection(test_db) as conn:
            conn.autocommit = True
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:

                # db_utils.ensure_replication_slot(cur, test_db)

                canonicalized_table_name = db_utils.canonicalized_table_name(cur, test_schema_name, test_table_name)

                create_table_sql = """
CREATE TABLE {} (id                   SERIAL PRIMARY KEY,
                our_varchar           VARCHAR,
                our_varchar_10        VARCHAR(10),
                our_text              TEXT,
                our_text_2            TEXT,
                our_integer           INTEGER,
                our_smallint          SMALLINT,
                our_bigint            BIGINT,
                our_decimal           NUMERIC(12,2),
                "OUR TS"              TIMESTAMP WITHOUT TIME ZONE,
                "OUR TS TZ"           TIMESTAMP WITH TIME ZONE,
                "OUR TIME"            TIME WITHOUT TIME ZONE,
                "OUR TIME TZ"         TIME WITH TIME ZONE,
                "OUR DATE"            DATE,
                our_double            DOUBLE PRECISION,
                our_real              REAL,
                our_boolean           BOOLEAN,
                our_bit               BIT(1),
                our_json              JSON,
                our_jsonb             JSONB,
                our_uuid              UUID,
                our_store             HSTORE,
                our_citext            CITEXT,
                our_cidr              cidr,
                our_inet              inet,
                our_mac               macaddr,
                our_alignment_enum    ALIGNMENT,
                our_money             money,
                invalid_bigserial     BIGSERIAL,
                invalid_bit_varying   BIT VARYING,
                invalid_box           BOX,
                invalid_bytea         BYTEA,
                invalid_circle        CIRCLE,
                invalid_interval      INTERVAL,
                invalid_line          LINE,
                invalid_lseg          LSEG,
                invalid_path          PATH,
                invalid_pg_lsn        PG_LSN,
                invalid_point         POINT,
                invalid_polygon       POLYGON,
                invalid_serial        SERIAL,
                invalid_smallserial   SMALLSERIAL,
                invalid_tsquery       TSQUERY,
                invalid_tsvector      TSVECTOR,
                invalid_txid_snapshot TXID_SNAPSHOT,
                invalid_xml           XML)
                """.format(canonicalized_table_name)

                cur = db_utils.ensure_fresh_table(conn, cur, test_schema_name, test_table_name)
                cur.execute(create_table_sql)

                #insert fixture data 1
                our_ts = datetime.datetime(1997, 2, 2, 2, 2, 2, 722184)
                nyc_tz = pytz.timezone('America/New_York')
                our_ts_tz = nyc_tz.localize(our_ts)
                our_time  = datetime.time(12,11,10)
                our_time_tz = our_time.isoformat() + "-04:00"
                our_date = datetime.date(1998, 3, 4)
                my_uuid =  str(uuid.uuid1())

                self.recs = []
                for _ in range(500):
                    our_ts = datetime.datetime(1987, 3, 3, 3, 3, 3, 733184)
                    nyc_tz = pytz.timezone('America/New_York')
                    our_ts_tz = nyc_tz.localize(our_ts)
                    our_time  = datetime.time(10,9,8)
                    our_time_tz = our_time.isoformat() + "-04:00"
                    our_date = datetime.date(1964, 7, 1)
                    my_uuid =  str(uuid.uuid1())

                    record = {'our_varchar' : "our_varchar 4",
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

                    db_utils.insert_record(cur, test_table_name, record)
                    self.recs.append(record)

                cur.execute("""ANALYZE {}""".format(canonicalized_table_name))

    @staticmethod
    def expected_check_streams():
        return { 'postgres_discovery_test'}

    def expected_check_stream_ids(self):
        """A set of expected table names in <collection_name> format"""
        check_streams = self.expected_check_streams()
        return {"{}-{}-{}".format(test_db, test_schema_name, stream) for stream in check_streams}

    @staticmethod
    def expected_primary_keys():
        return {
            'postgres_discovery_test' : {'id'}
        }

    @staticmethod
    def expected_unsupported_fields():
        return {
            'invalid_bigserial',
            'invalid_bit_varying',
            'invalid_box',
            'invalid_bytea',
            'invalid_circle',
            'invalid_interval',
            'invalid_line',
            'invalid_lseg',
            'invalid_path',
            'invalid_pg_lsn',
            'invalid_point',
            'invalid_polygon',
            'invalid_serial',
            'invalid_smallserial',
            'invalid_tsquery',
            'invalid_tsvector',
            'invalid_txid_snapshot',
            'invalid_xml',
        }
    @staticmethod
    def expected_schema_types():
        return {
            'id': 'integer',  # 'serial primary key',
            'our_varchar': 'character varying',  # 'varchar'
            'our_varchar_10': 'character varying',  # 'varchar(10)',
            'our_text': 'text',
            'our_text_2': 'text',
            'our_integer': 'integer',
            'our_smallint': 'smallint',
            'our_bigint': 'bigint',
            'our_decimal': 'numeric',
            'OUR TS': 'timestamp without time zone',
            'OUR TS TZ': 'timestamp with time zone',
            'OUR TIME': 'time without time zone',
            'OUR TIME TZ': 'time with time zone',
            'OUR DATE': 'date',
            'our_double': 'double precision',
            'our_real': 'real',
            'our_boolean': 'boolean',
            'our_bit': 'bit',
            'our_json': 'json',
            'our_jsonb': 'jsonb',
            'our_uuid': 'uuid',
            'our_store': 'hstore',
            'our_citext': 'citext',
            'our_cidr': 'cidr',
            'our_inet': 'inet',
            'our_mac': 'macaddr',
            'our_alignment_enum': 'alignment',
            'our_money': 'money',
            'invalid_bigserial': 'bigint',
            'invalid_bit_varying': 'bit varying',
            'invalid_box': 'box',
            'invalid_bytea': 'bytea',
            'invalid_circle': 'circle',
            'invalid_interval': 'interval',
            'invalid_line': 'line',
            'invalid_lseg': 'lseg',
            'invalid_path': 'path',
            'invalid_pg_lsn': 'pg_lsn',
            'invalid_point': 'point',
            'invalid_polygon': 'polygon',
            'invalid_serial': 'integer',
            'invalid_smallserial': 'smallint',
            'invalid_tsquery': 'tsquery',
            'invalid_tsvector': 'tsvector',
            'invalid_txid_snapshot': 'txid_snapshot',
            'invalid_xml': 'xml',
        }

    @staticmethod
    def tap_name():
        return "tap-postgres"

    @staticmethod
    def name():
        return "tap_tester_postgres_discovery"

    @staticmethod
    def get_type():
        return "platform.postgres"

    @staticmethod
    def get_credentials():
        return {'password': os.getenv('TAP_POSTGRES_PASSWORD')}

    def get_properties(self, original_properties=True):
        return_value = {
            'host' : os.getenv('TAP_POSTGRES_HOST'),
            'dbname' : os.getenv('TAP_POSTGRES_DBNAME'),
            'port' : os.getenv('TAP_POSTGRES_PORT'),
            'user' : os.getenv('TAP_POSTGRES_USER'),
            'default_replication_method' : self.FULL_TABLE,
            'filter_dbs' : 'discovery1'
        }
        if not original_properties:
            if self.default_replication_method is self.LOG_BASED:
                return_value['wal2json_message_format'] = '1'

            return_value['default_replication_method'] = self.default_replication_method

        return return_value

    def test_run(self):
        """Parametrized discovery test running against each replicatio method."""

        self.default_replication_method = self.FULL_TABLE
        full_table_conn_id = connections.ensure_connection(self, original_properties=False)
        self.discovery_test(full_table_conn_id)

        self.default_replication_method = self.INCREMENTAL
        incremental_conn_id = connections.ensure_connection(self, original_properties=False)
        self.discovery_test(incremental_conn_id)

        # NB | We are able to generate a connection and run discovery with a default replication
        #      method of logical replication WITHOUT selecting a replication slot. This is not
        #      ideal behavior. This BUG should not be carried over into hp-postgres, but will not
        #      be fixed for this tap.
        self.default_replication_method = self.LOG_BASED
        log_based_conn_id = connections.ensure_connection(self, original_properties=False)
        self.discovery_test(log_based_conn_id)

    def discovery_test(self, conn_id):
        """
        Basic Discovery Test for a database tap.

        Test Description:
          Ensure discovery runs without exit codes and generates a catalog of the expected form

        Test Cases:
            - Verify discovery generated the expected catalogs by name.
            - Verify that the table_name is in the format <collection_name> for each stream.
            - Verify the caatalog is found for a given stream.
            - Verify there is only 1 top level breadcrumb in metadata for a given stream.
            - Verify replication key(s) match expectations for a given stream.
            - Verify primary key(s) match expectations for a given stream.
            - Verify the replication method matches our expectations for a given stream.
            - Verify that only primary keys are given the inclusion of automatic in metadata
              for a given stream.
            - Verify expected unsupported fields are given the inclusion of unsupported in
              metadata for a given stream.
            - Verify that all fields for a given stream which are not unsupported or automatic
              have inclusion of available.
            - Verify row-count metadata matches expectations for a given stream.
            - Verify selected metadata is None for all streams.
            - Verify is-view metadata is False for a given stream.
            - Verify no forced-replication-method is present in metadata for a given stream.
            - Verify schema and db match expectations for a given stream.
            - Verify schema types match expectations for a given stream.
        """
        ##########################################################################
        ### TODO
        ###   [] Generate multiple tables (streams) and maybe dbs too?
        ###   [] Investigate potential bug, see DOCS_BUG_1
        ##########################################################################

        # run discovery (check mode)
        check_job_name = runner.run_check_mode(self, conn_id)

        # Verify check exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        # Verify discovery generated a catalog
        found_catalogs = menagerie.get_catalogs(conn_id)
        self.assertGreater(len(found_catalogs), 0)

        # Verify discovery generated the expected catalogs by name
        found_catalog_names = {catalog['stream_name'] for catalog in found_catalogs}
        self.assertSetEqual(self.expected_check_streams(), found_catalog_names)

        # Verify that the table_name is in the format <collection_name> for each stream
        found_catalog_stream_ids = {catalog['tap_stream_id'] for catalog in found_catalogs}
        self.assertSetEqual(self.expected_check_stream_ids(), found_catalog_stream_ids)

        # Test by stream
        for stream in self.expected_check_streams():
            with self.subTest(stream=stream):

                # Verify the caatalog is found for a given stream
                catalog = next(iter([catalog for catalog in found_catalogs
                                     if catalog["stream_name"] == stream]))
                self.assertTrue(isinstance(catalog, dict))

                # collecting expected values
                expected_primary_keys = self.expected_primary_keys()[stream]
                expected_replication_keys = set()
                expected_unsupported_fields = self.expected_unsupported_fields()
                expected_fields_to_datatypes = self.expected_schema_types()
                expected_row_count = len(self.recs)

                # collecting actual values...
                schema_and_metadata = menagerie.get_annotated_schema(conn_id, catalog['stream_id'])
                stream_metadata = schema_and_metadata["metadata"]
                top_level_metadata = [item for item in stream_metadata if item.get("breadcrumb") == []]
                stream_properties = top_level_metadata[0]['metadata']
                actual_primary_keys = set(stream_properties.get(self.PRIMARY_KEYS, []))
                actual_replication_keys = set(stream_properties.get(self.REPLICATION_KEYS, []))
                actual_replication_method = stream_properties.get(self.REPLICATION_METHOD)
                actual_automatic_fields = set(
                    item.get("breadcrumb", ["properties", None])[1] for item in stream_metadata
                    if item.get("metadata").get("inclusion") == "automatic"
                )
                actual_unsupported_fields = set(
                    item.get("breadcrumb", ["properties", None])[1] for item in stream_metadata
                    if item.get("metadata").get("inclusion") == "unsupported"
                )
                actual_fields_to_datatypes = {
                    item['breadcrumb'][1]: item['metadata'].get('sql-datatype')
                    for item in stream_metadata[1:]
                }

                # Verify there is only 1 top level breadcrumb in metadata
                self.assertEqual(1, len(top_level_metadata))

                # Verify replication key(s) match expectations
                self.assertSetEqual(
                    expected_replication_keys, actual_replication_keys
                )

                # NB | We expect primary keys and replication keys to have inclusion automatic for
                #      key-based incremental replication. But that is only true for primary keys here.
                #      This BUG should not be carried over into hp-postgres, but will not be fixed for this tap.

                # Verify primary key(s) match expectations
                self.assertSetEqual(
                    expected_primary_keys, actual_primary_keys,
                )

                # Verify the replication method matches our expectations
                self.assertIsNone(actual_replication_method)

                # Verify that only primary keys
                # are given the inclusion of automatic in metadata.
                self.assertSetEqual(expected_primary_keys, actual_automatic_fields)


                # DOCS_BUG_1 ? | The following types were converted and selected, but docs say unsupported.
                #                Still need to investigate how the tap handles values of these datatypes
                #                during sync.
                KNOWN_MISSING = {
                    'invalid_bigserial', # BIGSERIAL -> bigint
                    'invalid_serial',  # SERIAL -> integer
                    'invalid_smallserial',  # SMALLSERIAL -> smallint
                }
                # Verify expected unsupported fields
                # are given the inclusion of unsupported in metadata.
                self.assertSetEqual(expected_unsupported_fields, actual_unsupported_fields | KNOWN_MISSING)


                # Verify that all other fields have inclusion of available
                # This assumes there are no unsupported fields for SaaS sources
                self.assertTrue(
                    all({item.get("metadata").get("inclusion") == "available"
                         for item in stream_metadata
                         if item.get("breadcrumb", []) != []
                         and item.get("breadcrumb", ["properties", None])[1]
                         not in actual_automatic_fields
                         and item.get("breadcrumb", ["properties", None])[1]
                         not in actual_unsupported_fields}),
                    msg="Not all non key properties are set to available in metadata")

                # Verify row-count metadata matches expectations
                self.assertEqual(expected_row_count, stream_properties['row-count'])

                # Verify selected metadata is None for all streams
                self.assertNotIn('selected', stream_properties.keys())

                # Verify is-view metadata is False
                self.assertFalse(stream_properties['is-view'])

                # Verify no forced-replication-method is present in metadata
                self.assertNotIn(self.REPLICATION_METHOD, stream_properties.keys())

                # Verify schema and db match expectations
                self.assertEqual(test_schema_name, stream_properties['schema-name'])
                self.assertEqual(test_db, stream_properties['database-name'])

                # Verify schema types match expectations
                self.assertDictEqual(expected_fields_to_datatypes, actual_fields_to_datatypes)

SCENARIOS.add(PostgresDiscovery)
