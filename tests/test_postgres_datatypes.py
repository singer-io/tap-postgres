import os
import sys
import datetime
import copy
import unittest
import decimal
from decimal import Decimal
import json

from psycopg2.extensions import quote_ident
import psycopg2.extras
from tap_tester.scenario import (SCENARIOS)
import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner

import db_utils  # pylint: disable=import-error

test_schema_name = "public"
test_table_name = "postgres_datatypes_test"
test_db = "dev"

# TODO_1 | Why are we chaging datatypes for time to string?
expected_schema = {'OUR DATE': {'format': 'date-time', 'type': ['null', 'string']},
                   'OUR TIME': {'type': ['null', 'string']},  # TODO_1
                   'OUR TIME TZ': {'type': ['null', 'string']},  # TODO_1
                   'OUR TS': {'format': 'date-time', 'type': ['null', 'string']},
                   'OUR TS TZ': {'format': 'date-time', 'type': ['null', 'string']},
                   'id': {'maximum': 2147483647, 'minimum': -2147483648, 'type': ['integer']},
                   'unsupported_bit': {},
                   'unsupported_bit_varying': {},
                   'unsupported_box': {},
                   'unsupported_bytea': {},
                   'unsupported_circle': {},
                   'unsupported_interval': {},
                   'unsupported_line': {},
                   'unsupported_lseg': {},
                   'unsupported_path': {},
                   'unsupported_pg_lsn': {},
                   'unsupported_point': {},
                   'unsupported_polygon': {},
                   'unsupported_tsquery': {},
                   'unsupported_tsvector': {},
                   'unsupported_txid_snapshot': {},
                   'unsupported_xml': {},
                   'our_alignment_enum': {'type': ['null', 'string']},
                   'our_bigint': {'maximum': 9223372036854775807,
                                  'minimum': -9223372036854775808,
                                  'type': ['null', 'integer']},
                   'our_bigserial': {'maximum': 9223372036854775807,
                                     'minimum': -9223372036854775808,
                                     'type': ['null', 'integer']},
                   'our_bit': {'type': ['null', 'boolean']},
                   'our_boolean': {'type': ['null', 'boolean']},
                   'our_char': {'maxLength': 1, 'type': ['null', 'string']},
                   'our_char_big': {'maxLength': 10485760, 'type': ['null', 'string']},
                   'our_cidr': {'type': ['null', 'string']}, # TODO_1
                   'our_citext': {'type': ['null', 'string']},
                   'our_decimal': {'exclusiveMaximum': True,
                                   'exclusiveMinimum': True,
                                   'maximum': 100000000000000000000000000000000000000000000000000000000000000, # 62
                                   'minimum': -100000000000000000000000000000000000000000000000000000000000000,
                                   'multipleOf': Decimal('1E-38'),
                                   'type': ['null', 'number']},
                   'our_double': {'type': ['null', 'number']},
                   'our_inet': {'type': ['null', 'string']},  # TODO_1
                   'our_integer': {'maximum': 2147483647,
                                   'minimum': -2147483648,
                                   'type': ['null', 'integer']},
                   'our_json': {'type': ['null', 'string']}, # TODO_1 object ?  see hstore
                   'our_jsonb': {'type': ['null', 'string']}, # TODO_1 object ?
                   'our_mac': {'type': ['null', 'string']}, # TODO_1
                   'our_money': {'type': ['null', 'string']}, # TODO_1 decimal(n, 2) ?
                   'our_nospec_decimal': {'exclusiveMaximum': True,
                                          'exclusiveMinimum': True,
                                          'maximum': 100000000000000000000000000000000000000000000000000000000000000,
                                          'minimum': -100000000000000000000000000000000000000000000000000000000000000,
                                          'multipleOf': Decimal('1E-38'),
                                          'type': ['null', 'number']},
                   'our_nospec_numeric': {'exclusiveMaximum': True,
                                          'exclusiveMinimum': True,
                                          'maximum': 100000000000000000000000000000000000000000000000000000000000000,
                                          'minimum': -100000000000000000000000000000000000000000000000000000000000000,
                                          'multipleOf': Decimal('1E-38'),
                                          'type': ['null', 'number']},
                   'our_numeric': {'exclusiveMaximum': True,
                                   'exclusiveMinimum': True,
                                   'maximum': 100000000000000000000000000000000000000000000000000000000000000,
                                   'minimum': -100000000000000000000000000000000000000000000000000000000000000,
                                   'multipleOf': Decimal('1E-38'),
                                   'type': ['null', 'number']},
                   'our_real': {'type': ['null', 'number']},
                   'our_serial': {'maximum': 2147483647,
                                  'minimum': -2147483648,
                                  'type': ['null', 'integer']},
                   'our_smallint': {'maximum': 32767,
                                    'minimum': -32768,
                                    'type': ['null', 'integer']},
                   'our_smallserial': {'maximum': 32767,
                                       'minimum': -32768,
                                       'type': ['null', 'integer']},
                   'our_hstore': {'properties': {}, 'type': ['null', 'object']},
                   'our_text': {'type': ['null', 'string']},
                   'our_text_2': {'type': ['null', 'string']},
                   'our_uuid': {'type': ['null', 'string']},  # TODO_1
                   'our_varchar': {'type': ['null', 'string']},
                   'our_varchar_big': {'maxLength': 10485760, 'type': ['null', 'string']}}


decimal.getcontext().prec = 131072 + 16383


class PostgresDatatypes(unittest.TestCase):

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
        with db_utils.get_test_connection(test_db) as conn:
            conn.autocommit = True
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute(""" SELECT pg_drop_replication_slot('stitch') """)

    def setUp(self):
        db_utils.ensure_environment_variables_set()

        db_utils.ensure_db(test_db)
        self.maxDiff = None

        with db_utils.get_test_connection(test_db) as conn:
            conn.autocommit = True
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:

                db_utils.ensure_replication_slot(cur, test_db)

                canonicalized_table_name = db_utils.canonicalized_table_name(cur, test_schema_name, test_table_name)

                db_utils.set_db_time_zone(cur, '+15:59')

                create_table_sql = """
CREATE TABLE {} (id                       SERIAL PRIMARY KEY,
                our_varchar               VARCHAR,
                our_varchar_big           VARCHAR(10485760),
                our_char                  CHAR,
                our_char_big              CHAR(10485760),
                our_text                  TEXT,
                our_text_2                TEXT,
                our_integer               INTEGER,
                our_smallint              SMALLINT,
                our_bigint                BIGINT,
                our_nospec_numeric        NUMERIC,
                our_numeric               NUMERIC(1000, 500),
                our_nospec_decimal        DECIMAL,
                our_decimal               DECIMAL(1000, 500),
                "OUR TS"                  TIMESTAMP WITHOUT TIME ZONE,
                "OUR TS TZ"               TIMESTAMP WITH TIME ZONE,
                "OUR TIME"                TIME WITHOUT TIME ZONE,
                "OUR TIME TZ"             TIME WITH TIME ZONE,
                "OUR DATE"                DATE,
                our_double                DOUBLE PRECISION,
                our_real                  REAL,
                our_boolean               BOOLEAN,
                our_bit                   BIT(1),
                our_json                  JSON,
                our_jsonb                 JSONB,
                our_uuid                  UUID,
                our_hstore                HSTORE,
                our_citext                CITEXT,
                our_cidr                  cidr,
                our_inet                  inet,
                our_mac                   macaddr,
                our_alignment_enum        ALIGNMENT,
                our_money                 money,
                our_bigserial             BIGSERIAL,
                our_serial                SERIAL,
                our_smallserial           SMALLSERIAL,
                unsupported_bit           BIT(80),
                unsupported_bit_varying   BIT VARYING(80),
                unsupported_box           BOX,
                unsupported_bytea         BYTEA,
                unsupported_circle        CIRCLE,
                unsupported_interval      INTERVAL,
                unsupported_line          LINE,
                unsupported_lseg          LSEG,
                unsupported_path          PATH,
                unsupported_pg_lsn        PG_LSN,
                unsupported_point         POINT,
                unsupported_polygon       POLYGON,
                unsupported_tsquery       TSQUERY,
                unsupported_tsvector      TSVECTOR,
                unsupported_txid_snapshot TXID_SNAPSHOT,
                unsupported_xml           XML)
                """.format(canonicalized_table_name)

                cur = db_utils.ensure_fresh_table(conn, cur, test_schema_name, test_table_name)
                cur.execute(create_table_sql)


                # insert fixture data and track expected records by test cases
                self.inserted_records = []
                self.expected_records = dict()


                # BUG_5 | see ticket below
                #         The target blows up with greater than 38 digits before/after the decimal.
                #         Is this a known/expected behavior or a BUG in the target?
                #         It prevents us from testing what the tap claims to be able to support
                #         (100 precision, 38 scale) without rounding AND..The postgres limits WITH rounding.


                # insert a record wtih minimum values
                test_case = 'minimum_boundary_general'
                min_date = datetime.date(1, 1, 1)
                my_absurdly_small_decimal = decimal.Decimal('-' + '9' * 38 + '.' + '9' * 38) # CURRENT LIMIT IN TARGET
                # my_absurdly_small_decimal = decimal.Decimal('-' + '9' * 62 + '.' + '9' * 37) # 131072 + 16383 BUG_5
                # my_absurdly_small_spec_decimal = decimal.Decimal('-' + '9'*500 + '.' + '9'*500) # BUG_5
                self.inserted_records.append({
                    'id': 1,
                    'our_char': "a",
                    'our_varchar': "",
                    'our_varchar_big': "",
                    'our_char_big': "a",
                    'our_text': "",
                    'our_text_2': "",
                    'our_integer': -2147483648,
                    'our_smallint': -32768,
                    'our_bigint': -9223372036854775808,
                    'our_nospec_numeric': my_absurdly_small_decimal,
                    'our_numeric': my_absurdly_small_decimal,
                    'our_nospec_decimal': my_absurdly_small_decimal,
                    'our_decimal': my_absurdly_small_decimal,
                    quote_ident('OUR TS', cur): '0001-01-01T00:00:00.000001',
                    quote_ident('OUR TS TZ', cur): '0001-01-01T00:00:00.000001-15:59',
                    quote_ident('OUR TIME', cur): '00:00:00.000001',
                    quote_ident('OUR TIME TZ', cur): '00:00:00.000001-15:59',
                    quote_ident('OUR DATE', cur): min_date,#
                    'our_double':  decimal.Decimal('-1.79769313486231e+308'),
                    'our_real': decimal.Decimal('-3.40282e+38'),
                    'our_boolean': False,
                    'our_bit': '0',
                    'our_json': json.dumps(dict()),
                    'our_jsonb': json.dumps(dict()),
                    'our_uuid': '00000000-0000-0000-0000-000000000000',
                    'our_hstore': None,
                    'our_citext': "",
                    'our_cidr': '00.000.000.000/32',
                    'our_inet': '00.000.000.000',
                    'our_mac': '00:00:00:00:00:00',
                    'our_alignment_enum': None,
                    'our_money': '-$92,233,720,368,547,758.08',
                    'our_bigserial': 1,
                    'our_serial': 1,
                    'our_smallserial': 1,
                })
                self.expected_records[test_case] = copy.deepcopy(self.inserted_records[-1])
                self.expected_records[test_case].update({
                    'our_char_big': "a" + (10485760 - 1) * " ", # padded
                    'OUR TS': '0001-01-01T00:00:00.000001+00:00',
                    'OUR TS TZ': '0001-01-01T15:59:00.000001+00:00',
                    'OUR TIME': '00:00:00.000001',
                    'OUR TIME TZ': '00:00:00.000001-15:59',
                    'OUR DATE': '0001-01-01T00:00:00+00:00',
                    'our_bit': False,
                    'our_jsonb': json.loads(self.inserted_records[-1]['our_jsonb']),
                    'our_cidr': '0.0.0.0/32',
                    'our_inet': '0.0.0.0',
                    'our_mac': '00:00:00:00:00:00',
                })
                my_keys = set(self.expected_records[test_case].keys())
                for key in my_keys:  # we need overwrite expectations for fields with spaces
                    if key.startswith('"'):
                        del self.expected_records[test_case][key]
                db_utils.insert_record(cur, test_table_name, self.inserted_records[-1])


                # BUG_6 | https://stitchdata.atlassian.net/browse/SRCE-5205
                #       target uses binary notation for storage => 20 MB != 20 * (2^20)


                # add a record with a text value that approaches the Stitch linmit ~ 20 Megabytes
                test_case = 'maximum_boundary_text'
                our_serial = 2
                single_record_limit = 19990000  # 20*(1024*1024)  # BUG_6
                self.inserted_records.append({
                    'id': our_serial,
                    'our_bigserial': our_serial,
                    'our_serial': our_serial,
                    'our_smallserial': our_serial,
                    'our_text': single_record_limit * "a",
                })
                self.expected_records[test_case] = copy.deepcopy(self.inserted_records[-1])
                self.expected_records[test_case].update(self.null_out_remaining_fields(self.inserted_records[-1]))
                db_utils.insert_record(cur, test_table_name, self.inserted_records[-1])


                # TODO | BUG_1 | We do not maintain -Infinity, Infinity, and NaN for
                #                floating-point or arbitrary-precision values


                # add a record with  -Inf for floating point types
                test_case = 'negative_infinity_floats'
                our_serial += 1
                self.inserted_records.append({
                    'id': our_serial,
                    'our_bigserial': our_serial,
                    'our_serial': our_serial,
                    'our_smallserial': our_serial,
                    'our_double': '-Inf',
                    'our_real': '-Inf',
                })
                self.expected_records[test_case] = copy.deepcopy(self.inserted_records[-1])
                self.expected_records[test_case].update({
                    'our_double': None,  # BUG_1
                    'our_real': None,  # BUG_1
                })
                self.expected_records[test_case].update(self.null_out_remaining_fields(self.inserted_records[-1]))
                db_utils.insert_record(cur, test_table_name, self.inserted_records[-1])


                # add a record with Inf for floating point types
                test_case = 'positive_infinity_floats'
                our_serial += 1
                self.inserted_records.append({
                    'id': our_serial,
                    'our_bigserial': our_serial,
                    'our_serial': our_serial,
                    'our_smallserial': our_serial,
                    'our_double': 'Inf',
                    'our_real': 'Inf',
                })
                self.expected_records[test_case] = copy.deepcopy(self.inserted_records[-1])
                self.expected_records[test_case].update({
                    'our_double': None,  # BUG_1
                    'our_real': None,  # BUG_1
                })
                self.expected_records[test_case].update(self.null_out_remaining_fields(self.inserted_records[-1]))
                db_utils.insert_record(cur, test_table_name, self.inserted_records[-1])


                # add a record with NaN for floating point types
                test_case = 'not_a_number_floats'
                our_serial += 1
                self.inserted_records.append({
                    'id': our_serial,
                    'our_bigserial': our_serial,
                    'our_serial': our_serial,
                    'our_smallserial': our_serial,
                    'our_double': 'NaN',
                    'our_real': 'NaN',
                })
                self.expected_records[test_case] = copy.deepcopy(self.inserted_records[-1])
                self.expected_records[test_case].update({
                    'our_double': None,  # BUG_1
                    'our_real': None,  # BUG_1
                })
                self.expected_records[test_case].update(self.null_out_remaining_fields(self.inserted_records[-1]))
                db_utils.insert_record(cur, test_table_name, self.inserted_records[-1])


                # add a record with NaN for arbitrary precision types
                test_case = 'not_a_number_numeric'
                our_serial += 1
                self.inserted_records.append({
                    'id': our_serial,
                    'our_bigserial': our_serial,
                    'our_serial': our_serial,
                    'our_smallserial': our_serial,
                    'our_numeric': 'NaN',
                    'our_decimal': 'NaN',
                })
                self.expected_records[test_case] = copy.deepcopy(self.inserted_records[-1])
                self.expected_records[test_case].update({
                    'our_numeric': None,  # BUG_1
                    'our_decimal': None,  # BUG_1
                })
                self.expected_records[test_case].update(self.null_out_remaining_fields(self.inserted_records[-1]))
                db_utils.insert_record(cur, test_table_name, self.inserted_records[-1])


                # add a record with cidr/inet having IPV6 addresses
                test_case = 'ipv6_cidr_inet'
                our_serial += 1
                self.inserted_records.append({
                    'id': our_serial,
                    'our_bigserial': our_serial,
                    'our_serial': our_serial,
                    'our_smallserial': our_serial,
                    'our_cidr': 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/128',
                    'our_inet': 'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff',
                })
                self.expected_records[test_case] = copy.deepcopy(self.inserted_records[-1])
                self.expected_records[test_case].update(self.null_out_remaining_fields(self.inserted_records[-1]))
                db_utils.insert_record(cur, test_table_name, self.inserted_records[-1])


                # add a record with datetimes having 1 second precision
                test_case = '0_digits_of_precision_datetimes'
                our_serial += 1
                self.inserted_records.append({
                    'id': our_serial,
                    'our_bigserial': our_serial,
                    'our_serial': our_serial,
                    'our_smallserial': our_serial,
                    quote_ident('OUR TS', cur): '1996-12-23T19:05:00',
                    quote_ident('OUR TS TZ', cur): '1996-12-23T19:05:00+00:00',
                    quote_ident('OUR TIME', cur): '19:05:00',
                    quote_ident('OUR TIME TZ', cur): '19:05:00+00:00',
                })
                self.expected_records[test_case] = copy.deepcopy(self.inserted_records[-1])
                self.expected_records[test_case].update(self.null_out_remaining_fields(self.inserted_records[-1]))
                self.expected_records[test_case].update({
                    'OUR TS': '1996-12-23T19:05:00+00:00',
                    'OUR TS TZ': '1996-12-23T19:05:00+00:00',
                    'OUR TIME': '19:05:00',
                    'OUR TIME TZ': '19:05:00+00:00',
                })
                my_keys = set(self.expected_records[test_case].keys())
                for key in my_keys:
                    if key.startswith('"'):
                        del self.expected_records[test_case][key]
                db_utils.insert_record(cur, test_table_name, self.inserted_records[-1])


                # TODO UPDATE BUG_2 to reflect BUG_2 in HP  !


                # TODO | BUG_2 | We do not preserve datetime precision.
                #                If a record has a decimal value it is padded to 6 digits of precision.
                #                This is not the expected behavior.


                # add a record with datetimes having .1 second precision
                test_case = '1_digits_of_precision_datetimes'
                our_serial += 1
                self.inserted_records.append({
                    'id': our_serial,
                    'our_bigserial': our_serial,
                    'our_serial': our_serial,
                    'our_smallserial': our_serial,
                    quote_ident('OUR TS', cur): '1996-12-23T19:05:00.1',
                    quote_ident('OUR TS TZ', cur): '1996-12-23T19:05:00.1+00:00',
                    quote_ident('OUR TIME', cur): '19:05:00.1',
                    quote_ident('OUR TIME TZ', cur): '19:05:00.1+00:00',
                })
                self.expected_records[test_case] = copy.deepcopy(self.inserted_records[-1])
                self.expected_records[test_case].update(self.null_out_remaining_fields(self.inserted_records[-1]))
                self.expected_records[test_case].update({
                    'OUR TS': '1996-12-23T19:05:00.100000+00:00',  # '1996-12-23T19:05:00.1+00:00',  # BUG_2
                    'OUR TS TZ': '1996-12-23T19:05:00.100000+00:00',  # '1996-12-23T19:05:00.1+00:00',  # BUG_2
                    'OUR TIME': '19:05:00.100000',  # '19:05:00.1',  # BUG_2
                    'OUR TIME TZ': '19:05:00.100000+00:00',  # '19:05:00.1+00:00',  # BUG_2
                })
                my_keys = set(self.expected_records[test_case].keys())
                for key in my_keys:
                    if key.startswith('"'):
                        del self.expected_records[test_case][key]
                db_utils.insert_record(cur, test_table_name, self.inserted_records[-1])


                # add a record with datetimes having .01 second precision
                test_case = '2_digits_of_precision_datetimes'
                our_serial += 1
                self.inserted_records.append({
                    'id': our_serial,
                    'our_bigserial': our_serial,
                    'our_serial': our_serial,
                    'our_smallserial': our_serial,
                    quote_ident('OUR TS', cur): '1996-12-23T19:05:00.12',
                    quote_ident('OUR TS TZ', cur): '1996-12-23T19:05:00.12+00:00',
                    quote_ident('OUR TIME', cur): '19:05:00.12',
                    quote_ident('OUR TIME TZ', cur): '19:05:00.12+00:00',
                })
                self.expected_records[test_case] = copy.deepcopy(self.inserted_records[-1])
                self.expected_records[test_case].update(self.null_out_remaining_fields(self.inserted_records[-1]))
                self.expected_records[test_case].update({
                    'OUR TS': '1996-12-23T19:05:00.120000+00:00',  # '1996-12-23T19:05:00.12+00:00',  # BUG_2
                    'OUR TS TZ': '1996-12-23T19:05:00.120000+00:00',  # '1996-12-23T19:05:00.12+00:00',  # BUG_2
                    'OUR TIME': '19:05:00.120000',  # '19:05:00.12',  # BUG_2
                    'OUR TIME TZ': '19:05:00.120000+00:00',  # '19:05:00.12+00:00',  # BUG_2
                })
                my_keys = set(self.expected_records[test_case].keys())
                for key in my_keys:
                    if key.startswith('"'):
                        del self.expected_records[test_case][key]
                db_utils.insert_record(cur, test_table_name, self.inserted_records[-1])


                # add a record with datetimes having .001 second (millisecond)  precision
                test_case = '3_digits_of_precision_datetimes'
                our_serial += 1
                self.inserted_records.append({
                    'id': our_serial,
                    'our_bigserial': our_serial,
                    'our_serial': our_serial,
                    'our_smallserial': our_serial,
                    quote_ident('OUR TS', cur): '1996-12-23T19:05:00.123',
                    quote_ident('OUR TS TZ', cur): '1996-12-23T19:05:00.123+00:00',
                    quote_ident('OUR TIME', cur): '19:05:00.123',
                    quote_ident('OUR TIME TZ', cur): '19:05:00.123+00:00',
                })
                self.expected_records[test_case] = copy.deepcopy(self.inserted_records[-1])
                self.expected_records[test_case].update(self.null_out_remaining_fields(self.inserted_records[-1]))
                self.expected_records[test_case].update({
                    'OUR TS': '1996-12-23T19:05:00.123000+00:00',  # '1996-12-23T19:05:00.123+00:00',  # BUG_2
                    'OUR TS TZ': '1996-12-23T19:05:00.123000+00:00',  # '1996-12-23T19:05:00.123+00:00',  # BUG_2
                    'OUR TIME': '19:05:00.123000',  # '19:05:00.123',  # BUG_2
                    'OUR TIME TZ': '19:05:00.123000+00:00',  # '19:05:00.123+00:00',  # BUG_2
                })
                my_keys = set(self.expected_records[test_case].keys())
                for key in my_keys:
                    if key.startswith('"'):
                        del self.expected_records[test_case][key]
                db_utils.insert_record(cur, test_table_name, self.inserted_records[-1])


                # add a record with datetimes having .0001 secondprecision
                test_case = '4_digits_of_precision_datetimes'
                our_serial += 1
                self.inserted_records.append({
                    'id': our_serial,
                    'our_bigserial': our_serial,
                    'our_serial': our_serial,
                    'our_smallserial': our_serial,
                    quote_ident('OUR TS', cur): '1996-12-23T19:05:00.1234',
                    quote_ident('OUR TS TZ', cur): '1996-12-23T19:05:00.1234+00:00',
                    quote_ident('OUR TIME', cur): '19:05:00.1234',
                    quote_ident('OUR TIME TZ', cur): '19:05:00.1234+00:00',
                })
                self.expected_records[test_case] = copy.deepcopy(self.inserted_records[-1])
                self.expected_records[test_case].update(self.null_out_remaining_fields(self.inserted_records[-1]))
                self.expected_records[test_case].update({
                    'OUR TS': '1996-12-23T19:05:00.123400+00:00',  # '1996-12-23T19:05:00.1234+00:00',  # BUG_2
                    'OUR TS TZ': '1996-12-23T19:05:00.123400+00:00',  # '1996-12-23T19:05:00.1234+00:00',  # BUG_2
                    'OUR TIME': '19:05:00.123400',  # '19:05:00.1234',  # BUG_2
                    'OUR TIME TZ': '19:05:00.123400+00:00',  # '19:05:00.1234+00:00',  # BUG_2
                })
                my_keys = set(self.expected_records[test_case].keys())
                for key in my_keys:
                    if key.startswith('"'):
                        del self.expected_records[test_case][key]
                db_utils.insert_record(cur, test_table_name, self.inserted_records[-1])


                # add a record with datetimes having .00001 second precision
                test_case = '5_digits_of_precision_datetimes'
                our_serial += 1
                self.inserted_records.append({
                    'id': our_serial,
                    'our_bigserial': our_serial,
                    'our_serial': our_serial,
                    'our_smallserial': our_serial,
                    quote_ident('OUR TS', cur): '1996-12-23T19:05:00.12345',
                    quote_ident('OUR TS TZ', cur): '1996-12-23T19:05:00.12345+00:00',
                    quote_ident('OUR TIME', cur): '19:05:00.12345',
                    quote_ident('OUR TIME TZ', cur): '19:05:00.12345+00:00',
                })
                self.expected_records[test_case] = copy.deepcopy(self.inserted_records[-1])
                self.expected_records[test_case].update(self.null_out_remaining_fields(self.inserted_records[-1]))
                self.expected_records[test_case].update({
                    'OUR TS': '1996-12-23T19:05:00.123450+00:00',  # '1996-12-23T19:05:00.12345+00:00',  # BUG_2
                    'OUR TS TZ': '1996-12-23T19:05:00.123450+00:00',  # '1996-12-23T19:05:00.12345+00:00',  # BUG_2
                    'OUR TIME': '19:05:00.123450',  # '19:05:00.12345',  # BUG_2
                    'OUR TIME TZ': '19:05:00.123450+00:00',  # '19:05:00.12345+00:00',  # BUG_2
                })
                my_keys = set(self.expected_records[test_case].keys())
                for key in my_keys:
                    if key.startswith('"'):
                        del self.expected_records[test_case][key]
                db_utils.insert_record(cur, test_table_name, self.inserted_records[-1])


                # add a record with datetimes having .000001 second (microsecond) precision
                test_case = '6_digits_of_precision_datetimes'
                our_serial += 1
                self.inserted_records.append({
                    'id': our_serial,
                    'our_bigserial': our_serial,
                    'our_serial': our_serial,
                    'our_smallserial': our_serial,
                    quote_ident('OUR TS', cur): '1996-12-23T19:05:00.123456',
                    quote_ident('OUR TS TZ', cur): '1996-12-23T19:05:00.123456+00:00',
                    quote_ident('OUR TIME', cur): '19:05:00.123456',
                    quote_ident('OUR TIME TZ', cur): '19:05:00.123456+00:00',
                })
                self.expected_records[test_case] = copy.deepcopy(self.inserted_records[-1])
                self.expected_records[test_case].update(self.null_out_remaining_fields(self.inserted_records[-1]))
                self.expected_records[test_case].update({
                    'OUR TS': '1996-12-23T19:05:00.123456+00:00',
                    'OUR TS TZ': '1996-12-23T19:05:00.123456+00:00',
                    'OUR TIME': '19:05:00.123456',
                    'OUR TIME TZ': '19:05:00.123456+00:00',
                })
                my_keys = set(self.expected_records[test_case].keys())
                for key in my_keys:
                    if key.startswith('"'):
                        del self.expected_records[test_case][key]
                db_utils.insert_record(cur, test_table_name, self.inserted_records[-1])


                # TODO | BUG_3 | floating-point precisions can't handle expected
                #                negative value nearest zero boundary


                # add a record with a negative value nearest zero for double and real
                test_case = 'near_zero_negative_floats'
                our_serial += 1
                self.inserted_records.append({
                    'id': our_serial,
                    'our_bigserial': our_serial,
                    'our_serial': our_serial,
                    'our_smallserial': our_serial,
                    'our_double': decimal.Decimal('-2.22507385850720e-308'),  # -2.2250738585072014e-308, # BUG_3
                    'our_real': decimal.Decimal('-1.17549E-38'),  #-1.175494351e-38 BUG_3
                })
                self.expected_records[test_case] = copy.deepcopy(self.inserted_records[-1])
                self.expected_records[test_case].update(self.null_out_remaining_fields(self.inserted_records[-1]))
                db_utils.insert_record(cur, test_table_name, self.inserted_records[-1])


                # TODO | BUG_4 | floating-point precisions can't handle expected
                #                positive value nearest zero boundary


                # add a record with a positive value nearest zero for double and real
                test_case = 'near_zero_positive_floats'
                our_serial += 1
                self.inserted_records.append({
                    'id': our_serial,
                    'our_bigserial': our_serial,
                    'our_serial': our_serial,
                    'our_smallserial': our_serial,
                    'our_double': decimal.Decimal('2.22507385850720e-308'),  # 2.2250738585072014e-308  BUG_4
                    'our_real': decimal.Decimal('1.17549e-38'),  # 1.175494351e-38  BUG_4
                })
                self.expected_records[test_case] = copy.deepcopy(self.inserted_records[-1])
                self.expected_records[test_case].update(self.null_out_remaining_fields(self.inserted_records[-1]))
                db_utils.insert_record(cur, test_table_name, self.inserted_records[-1])


                # add a record with the value 0 for double and real
                test_case = 'zero_floats'
                our_serial += 1
                self.inserted_records.append({
                    'id': our_serial,
                    'our_bigserial': our_serial,
                    'our_serial': our_serial,
                    'our_smallserial': our_serial,
                    'our_double': 0,
                    'our_real': 0,
                })
                self.expected_records[test_case] = copy.deepcopy(self.inserted_records[-1])
                self.expected_records[test_case].update(self.null_out_remaining_fields(self.inserted_records[-1]))
                db_utils.insert_record(cur, test_table_name, self.inserted_records[-1])


                # add a record with an hstore that has spaces, commas, arrows, quotes, and  escapes
                test_case = 'special_characters_hstore'
                our_serial += 1
                self.inserted_records.append({
                    'id': our_serial,
                    'our_bigserial': our_serial,
                    'our_serial': our_serial,
                    'our_smallserial': our_serial,
                    'our_hstore': r' "spaces" => " b a r ", "commas" => "f,o,o,", "arrow" => "=>", "backslash" => "\\",  "double_quote" => "\"" '
                })
                self.expected_records[test_case] = copy.deepcopy(self.inserted_records[-1])
                self.expected_records[test_case].update(self.null_out_remaining_fields(self.inserted_records[-1]))
                self.expected_records[test_case].update({
                    'our_hstore': {
                        "spaces": " b a r ",
                        "commas": "f,o,o,",
                        "arrow": "=>",
                        "double_quote": "\"",
                        "backslash": "\\"
                    }
                })
                db_utils.insert_record(cur, test_table_name, self.inserted_records[-1])


                # add a record with Null for every field
                test_case = 'null_for_all_fields_possible'
                our_serial += 1
                self.inserted_records.append({
                    'id': our_serial,
                    'our_bigserial': our_serial,
                    'our_serial': our_serial,
                    'our_smallserial': our_serial,
                    'our_char': None,
                    'our_varchar': None,
                    'our_varchar_big': None,
                    'our_char_big': None,
                    'our_text':  None,
                    'our_text_2': None,
                    'our_integer': None,
                    'our_smallint': None,
                    'our_bigint': None,
                    'our_nospec_numeric': None,
                    'our_numeric': None,
                    'our_nospec_decimal': None,
                    'our_decimal': None,
                    quote_ident('OUR TS', cur): None,
                    quote_ident('OUR TS TZ', cur): None,
                    quote_ident('OUR TIME', cur): None,
                    quote_ident('OUR TIME TZ', cur): None,
                    quote_ident('OUR DATE', cur): None,
                    'our_double': None,
                    'our_real': None,
                    'our_boolean': None,
                    'our_bit': None,
                    'our_json': None,
                    'our_jsonb': None,
                    'our_uuid': None,
                    'our_hstore': None,
                    'our_citext': None,
                    'our_cidr': None,
                    'our_inet': None,
                    'our_mac': None,
                    'our_alignment_enum': None,
                    'our_money': None,
                })
                self.expected_records[test_case] = copy.deepcopy(self.inserted_records[-1])
                self.expected_records[test_case].update({
                    'OUR TS': None,
                    'OUR TS TZ': None,
                    'OUR TIME': None,
                    'OUR TIME TZ': None,
                    'OUR DATE': None,
                })
                my_keys = set(self.expected_records[test_case].keys())
                for key in my_keys:
                    if key.startswith('"'):
                        del self.expected_records[test_case][key]
                db_utils.insert_record(cur, test_table_name, self.inserted_records[-1])


                # BUG_5 | https://stitchdata.atlassian.net/browse/SRCE-5226
                #         The target prevents us from sending a record with numeric/decimal
                #         values that are out of the max precision of 6 decimal digits.
                #         The expectation is that values with higher precision than the allowed
                #         limit, would be rounded and handled.


                # add a record with out-of-bounds precision for DECIMAL/NUMERIC
                # test_case = 'out_of_bounds_precision_decimal_and_numeric'
                # our_serial += 1
                # our_precision_too_high_decimal = decimal.Decimal('12345.' + '6' * 39)
                # self.inserted_records.append({
                #     'id': our_serial,
                #     'our_bigserial': our_serial,
                #     'our_serial': our_serial,
                #     'our_smallserial': our_serial,
                #     'our_decimal': our_precision_too_high_decimal,
                #     'our_nospec_decimal': our_precision_too_high_decimal,
                #     'our_numeric': our_precision_too_high_decimal,
                #     'our_nospec_numeric': our_precision_too_high_decimal,
                # })
                # self.expected_records[test_case] = copy.deepcopy(self.inserted_records[-1])
                # self.expected_records[test_case].update(self.null_out_remaining_fields(self.inserted_records[-1]))
                # self.expected_records[test_case].update({
                #     'our_decimal': decimal.Decimal('12345.' + '6' * 37 + '7'),
                #     'our_nospec_decimal': decimal.Decimal('12345.' + '6' * 37 + '7'),
                #     'our_numeric': decimal.Decimal('12345.' + '6' * 37 + '7'),
                #     'our_nospec_numeric': decimal.Decimal('12345.' + '6' * 37 + '7'),
                # })
                # db_utils.insert_record(cur, test_table_name, self.inserted_records[-1])


                # add a record with all extended ascii characters
                test_case = 'all_ascii_text'
                our_serial += 1
                our_ascii = ''.join(chr(x) for x in range(128) if chr(x) != '\x00')
                our_extended_ascii = ''.join(chr(x) for x in range(256) if chr(x) != '\x00')
                self.inserted_records.append({
                    'id': our_serial,
                    'our_bigserial': our_serial,
                    'our_serial': our_serial,
                    'our_smallserial': our_serial,
                    'our_text': our_ascii,
                    'our_text_2': our_extended_ascii,
                })
                self.expected_records[test_case] = copy.deepcopy(self.inserted_records[-1])
                self.expected_records[test_case].update(self.null_out_remaining_fields(self.inserted_records[-1]))
                db_utils.insert_record(cur, test_table_name, self.inserted_records[-1])


                # add a record with all unicode characters
                test_case = 'all_unicode_text'
                our_serial += 1
                our_unicode = ''
                chars = list(range(1, 55296))  # skip 0 because 'null' is not supported
                chars.extend(range(57344, sys.maxunicode + 1))
                for x in chars:
                    our_unicode += chr(x)
                self.inserted_records.append({
                    'id': our_serial,
                    'our_bigserial': our_serial,
                    'our_serial': our_serial,
                    'our_smallserial': our_serial,
                    'our_text': our_unicode,
                })
                self.expected_records[test_case] = copy.deepcopy(self.inserted_records[-1])
                self.expected_records[test_case].update(self.null_out_remaining_fields(self.inserted_records[-1]))
                db_utils.insert_record(cur, test_table_name, self.inserted_records[-1])


                # add a record with a non-specified varchar value that approaches the Stitch linmit ~ 20 Megabytes
                test_case = 'maximum_boundary_varchar'
                our_serial += 1
                single_record_limit = 19990000  # 20*(1024*1024)  # BUG_6
                self.inserted_records.append({
                    'id': our_serial,
                    'our_bigserial': our_serial,
                    'our_serial': our_serial,
                    'our_smallserial': our_serial,
                    'our_varchar': single_record_limit * "a",
                })
                self.expected_records[test_case] = copy.deepcopy(self.inserted_records[-1])
                self.expected_records[test_case].update(self.null_out_remaining_fields(self.inserted_records[-1]))
                db_utils.insert_record(cur, test_table_name, self.inserted_records[-1])


                # insert a record with valid values for unsupported types
                test_case = 'unsupported_types'
                our_serial = 9999
                self.inserted_records.append({
                    'id': our_serial,
                    'our_bigserial': our_serial,
                    'our_serial': our_serial,
                    'our_smallserial': our_serial,
                    'unsupported_bit_varying': '01110100011000010111000000101101011101000110010101110011011101000110010101110010',
                    'unsupported_bit': '01110100011000010111000000101101011101000110010101110011011101000110010101110010',
                    'unsupported_box': '((50, 50), (0, 0))',
                    'unsupported_bytea': "E'\\255'",
                    'unsupported_circle': '< (3, 1), 4 >',
                    'unsupported_interval': '178000000 years',
                    'unsupported_line': '{6, 6, 6}',
                    'unsupported_lseg': '(0 , 45), (45, 90)',
                    'unsupported_path': '((0, 0), (45, 90), (2, 56))',
                    'unsupported_pg_lsn': '16/B374D848',
                    'unsupported_point': '(1, 2)',
                    'unsupported_polygon': '((0, 0), (0, 10), (10, 0), (4, 5), (6, 7))',
                    'unsupported_tsquery': "'fat' & 'rat'",
                    'unsupported_tsvector':  "'fat':2 'rat':3",
                    'unsupported_txid_snapshot': '10:20:10,14,15',
                    'unsupported_xml': '<foo>bar</foo>',
                })
                self.expected_records[test_case] = {
                    'id': self.inserted_records[-1]['id'],
                    'our_bigserial': self.inserted_records[-1]['our_bigserial'],
                    'our_serial': self.inserted_records[-1]['our_serial'],
                    'our_smallserial': self.inserted_records[-1]['our_smallserial'],
                }
                self.expected_records[test_case].update(self.null_out_remaining_fields(self.inserted_records[-1]))
                db_utils.insert_record(cur, test_table_name, self.inserted_records[-1])


                # insert a record wtih maximum values
                test_case = 'maximum_boundary_general'
                max_ts = datetime.datetime(9999, 12, 31, 23, 59, 59, 999999)
                base_string = "Bread Sticks From Olive Garden 🥖"
                my_absurdly_large_decimal = decimal.Decimal('9' * 38 + '.' + '9' * 38)
                self.inserted_records.append({
                    'id': 2147483647,
                    'our_char': "🥖",
                    'our_varchar': "a",
                    'our_varchar_big': "🥖" + base_string,
                    'our_char_big': "🥖",
                    'our_text': "apples",
                    'our_text_2': None,
                    'our_integer': 2147483647,
                    'our_smallint': 32767,
                    'our_bigint': 9223372036854775807,
                    'our_nospec_numeric': my_absurdly_large_decimal,
                    'our_numeric': my_absurdly_large_decimal,
                    'our_nospec_decimal': my_absurdly_large_decimal,
                    'our_decimal': my_absurdly_large_decimal,
                    quote_ident('OUR TS', cur): max_ts,
                    quote_ident('OUR TS TZ', cur): '9999-12-31T08:00:59.999999-15:59',
                    quote_ident('OUR TIME', cur): '23:59:59.999999',
                    quote_ident('OUR TIME TZ', cur): '23:59:59.999999+1559',
                    quote_ident('OUR DATE', cur): '5874897-12-31',
                    'our_double': decimal.Decimal('1.79769313486231e+308'),
                    'our_real': decimal.Decimal('3.40282e+38'),
                    'our_boolean': True,
                    'our_bit': '1',
                    'our_json': json.dumps({
                        'our_json_string': 'This is our JSON string type.',
                        'our_json_number': 666,
                        'our_json_object': {
                            'our_json_string': 'This is our JSON string type.',
                            'our_json_number': 666,
                            'our_json_object': {'calm': 'down'},
                            'our_json_array': ['our_json_arrary_string', 6, {'calm': 'down'}, False, None],
                            'our_json_boolean': True,
                            'our_json_null': None,
                        },
                        'our_json_array': ['our_json_arrary_string', 6, {'calm': 'down'}, False, None, ['apples', 6]],
                        'our_json_boolean': True,
                        'our_json_null': None,
                    }),
                    'our_jsonb': json.dumps({
                        'our_jsonb_string': 'This is our JSONB string type.',
                        'our_jsonb_number': 666,
                        'our_jsonb_object': {
                            'our_jsonb_string': 'This is our JSONB string type.',
                            'our_jsonb_number': 666,
                            'our_jsonb_object': {'calm': 'down'},
                            'our_jsonb_array': ['our_jsonb_arrary_string', 6, {'calm': 'down'}, False, None],
                            'our_jsonb_boolean': True,
                            'our_jsonb_null': None,
                        },
                        'our_jsonb_array': ['our_jsonb_arrary_string', 6, {'calm': 'down'}, False, None, ['apples', 6]],
                        'our_jsonb_boolean': True,
                        'our_jsonb_null': None,
                    }),
                    'our_uuid':'ffffffff-ffff-ffff-ffff-ffffffffffff',
                    'our_hstore': '"foo"=>"bar","bar"=>"foo","dumdum"=>Null',
                    'our_citext': "aPpLeS",
                    'our_cidr': '199.199.199.128/32',
                    'our_inet': '199.199.199.128',
                    'our_mac': 'ff:ff:ff:ff:ff:ff',
                    'our_alignment_enum': 'u g l y',
                    'our_money': "$92,233,720,368,547,758.07",
                    'our_bigserial': 9223372036854775807,
                    'our_serial': 2147483647,
                    'our_smallserial': 32767,
                })
                self.expected_records[test_case] = copy.deepcopy(self.inserted_records[-1])
                self.expected_records[test_case].update({
                    'OUR TS': '9999-12-31T23:59:59.999999+00:00',
                    'OUR TS TZ': '9999-12-31T23:59:59.999999+00:00',
                    'OUR TIME': '23:59:59.999999',
                    'OUR TIME TZ': '23:59:59.999999+15:59',
                    'OUR DATE': '9999-12-31T00:00:00+00:00',
                    'our_char_big': "🥖" + " " * 10485759,
                    'our_bit': True,
                    'our_jsonb': json.loads(self.inserted_records[-1]['our_jsonb']),
                    'our_hstore': {'foo': 'bar', 'bar': 'foo', 'dumdum': None},
                })
                my_keys = set(self.expected_records[test_case].keys())
                for key in my_keys:
                    if key.startswith('"'):
                        del self.expected_records[test_case][key]
                db_utils.insert_record(cur, test_table_name, self.inserted_records[-1])


                # MANUAL TESTING
                #      EXCEED THE PYTHON LIMITATIONS FOR
                #       [] datetimes


                # FUTURE TEST GOALS
                #     ERROR MESSAGE TESTING
                #       [] 'OUR TS':      '4713-01-01 00:00:00.000000 BC'       TIMESTAMP WITHOUT TIME ZONE,
                #       [] 'OUR TS TZ':   '4713-01-01 00:00:00.000000 BC',      TIMESTAMP WITH TIME ZONE,
                #       [] 'OUR TIME':    '00:00:00.000001',                    TIME WITHOUT TIME ZONE,
                #       [] 'OUR TIME TZ': '00:00:00.000001-15:59', #            TIME WITH TIME ZONE,
                #       [] 'OUR DATE':    '4713-01-01 BC',                      DATE
                #       [] 'our_double':  decimal.Decimal('-1.79769313486231e+308'),   DOUBLE PRECISION
                #       [] 'our_real':    decimal.Decimal('-3.40282e+38'),             REAL,
                #       [] 'OUR TS':      '294276-12-31 24:00:00.000000',              TIMESTAMP WITHOUT TIME ZONE,
                #       [] 'OUR TS TZ':   '294276-12-31 24:00:00.000000',              TIMESTAMP WITH TIME ZONE,
                #       [] 'OUR TIME':    '23:59:59.999999',# '24:00:00.000000'        TIME WITHOUT TIME ZONE,
                #       [] 'OUR TIME TZ': '23:59:59.999999+1559',                      TIME WITH TIME ZONE,
                #       [] 'OUR DATE':    '5874897-12-31',                             DATE,


    def null_out_remaining_fields(self, inserted_record):
        all_fields = self.expected_fields()
        unsupported_fields = self.expected_unsupported_fields()
        set_fields = set(inserted_record.keys())

        remaining_fields = all_fields.difference(set_fields).difference(unsupported_fields)
        remaining_valid_fields_to_null = {field: None for field in remaining_fields}

        return remaining_valid_fields_to_null

    @staticmethod
    def expected_check_streams():
        return {
            'postgres_datatypes_test',
        }

    @staticmethod
    def expected_sync_streams():
        return {
            'postgres_datatypes_test',
        }

    def expected_check_stream_ids(self):
        """A set of expected table names in <collection_name> format"""
        check_streams = self.expected_check_streams()
        return {"{}-{}-{}".format(test_db, test_schema_name, stream) for stream in check_streams}

    @staticmethod
    def expected_primary_keys():
        return {
            'postgres_datatypes_test' : {'id'}
        }

    def expected_unsupported_fields(self):
        expected_fields = self.expected_fields()
        return {field
                for field in expected_fields
                if field.startswith("unsupported")}

    @staticmethod
    def expected_fields():
        return {
            'id',
            'our_varchar',  #               VARCHAR,
            'our_varchar_big',  #           VARCHAR(10485760),
            'our_char',  #                  CHAR,
            'our_char_big',  #              CHAR(10485760),
            'our_text',  #                  TEXT,
            'our_text_2',  #                TEXT,
            'our_integer',  #               INTEGER,
            'our_smallint',  #              SMALLINT,
            'our_bigint',  #                BIGINT,
            'our_nospec_numeric',  #        NUMERIC,
            'our_numeric',  #               NUMERIC(1000, 500),
            'our_nospec_decimal',  #        DECIMAL,
            'our_decimal',  #               DECIMAL(1000, 500),
            'OUR TS',  #                  TIMESTAMP WITHOUT TIME ZONE,
            'OUR TS TZ',  #               TIMESTAMP WITH TIME ZONE,
            'OUR TIME',  #                TIME WITHOUT TIME ZONE,
            'OUR TIME TZ',  #             TIME WITH TIME ZONE,
            'OUR DATE',  #                DATE,
            'our_double',  #                DOUBLE PRECISION,
            'our_real',  #                  REAL,
            'our_boolean',  #               BOOLEAN,
            'our_bit',  #                   BIT(1),
            'our_json',  #                  JSON,
            'our_jsonb',  #                 JSONB,
            'our_uuid',  #                  UUID,
            'our_hstore',  #                HSTORE,
            'our_citext',  #                CITEXT,
            'our_cidr',  #                  cidr,
            'our_inet',  #                  inet,
            'our_mac',  #                   macaddr,
            'our_alignment_enum',  #        ALIGNMENT,
            'our_money',  #                 money,
            'our_bigserial',  #             BIGSERIAL,
            'our_serial',  #                SERIAL,
            'our_smallserial',  #           SMALLSERIAL,
            'unsupported_bit',  #           BIT(80),
            'unsupported_bit_varying',  #   BIT VARYING(80),
            'unsupported_box',  #           BOX,
            'unsupported_bytea',  #         BYTEA,
            'unsupported_circle',  #        CIRCLE,
            'unsupported_interval',  #      INTERVAL,
            'unsupported_line',  #          LINE,
            'unsupported_lseg',  #          LSEG,
            'unsupported_path',  #          PATH,
            'unsupported_pg_lsn',  #        PG_LSN,
            'unsupported_point',  #         POINT,
            'unsupported_polygon',  #       POLYGON,
            'unsupported_tsquery',  #       TSQUERY,
            'unsupported_tsvector',  #      TSVECTOR,
            'unsupported_txid_snapshot',  # TXID_SNAPSHOT,
            'unsupported_xml',  #           XML
        }

    @staticmethod
    def tap_name():
        return "tap-postgres"

    @staticmethod
    def name():
        return "tap_tester_postgres_datatypes"

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
            'filter_dbs' : 'dev'
        }
        if not original_properties:
            if self.default_replication_method is self.LOG_BASED:
                return_value['wal2json_message_format'] = '1'

            return_value['default_replication_method'] = self.default_replication_method

        return return_value


    def select_streams_and_fields(self, conn_id, catalog, select_all_fields: bool = True):
        """Select all streams and all fields within streams or all streams and no fields."""

        schema = menagerie.get_annotated_schema(conn_id, catalog['stream_id'])

        if self.default_replication_method is self.FULL_TABLE:
            additional_md = [{
                "breadcrumb": [], "metadata": {"replication-method": self.FULL_TABLE}
            }]

        elif self.default_replication_method is self.INCREMENTAL:
            additional_md = [{
                "breadcrumb": [], "metadata": {
                    "replication-method": self.INCREMENTAL, "replication-key": "id"
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
        """Parametrized datatypes test running against each replication method."""

        for replication_method in {self.FULL_TABLE, self.LOG_BASED, self.INCREMENTAL}:
            with self.subTest(replication_method=replication_method):

                # set default replication
                self.default_replication_method = replication_method

                # grab a new connection
                conn_id = connections.ensure_connection(self, original_properties=False)

                # run the test against the new connection
                self.datatypes_test(conn_id)

                print(f"{self.name()} passed using {replication_method} replication.")


    def datatypes_test(self, conn_id):
        """
        Test Description:
          Testing boundary values for all postgres-supported datatypes. Negative testing
          for tap-unsupported types. Partition testing for datetime precision. Testing edge
          cases for text, numeric/decimal, and datetimes.

        Test Cases:
          - 'minimum_boundary_general'
          - 'maximum_boundary_text'
          - 'negative_infinity_floats
          - 'positive_infinity_floats'
          - 'not_a_number_floats'
          - 'not_a_number_numeric'
          - 'ipv6_cidr_inet'
          - '0_digits_of_precision_datetimes'
          - '1_digits_of_precision_datetimes'
          - '2_digits_of_precision_datetimes'
          - '3_digits_of_precision_datetimes'
          - '4_digits_of_precision_datetimes'
          - '5_digits_of_precision_datetimes'
          - '6_digits_of_precision_datetimes'
          - 'near_zero_negative_floats'
          - 'near_zero_positive_floats'
          - 'zero_floats'
          - 'special_characters_hstore'
          - 'null_for_all_fields_possible'
          - 'out_of_bounds_precision_decimal_and_numeric'
          - 'all_ascii_text'
          - 'all_unicode_text'
          - 'maximum_boundary_varchar'
          - 'unsupported_types'
          - 'maximum_boundary_general'
        """

        # run discovery (check mode)
        check_job_name = runner.run_check_mode(self, conn_id)

        # Verify check exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        # Verify discovery generated a catalog
        found_catalogs = [found_catalog for found_catalog in menagerie.get_catalogs(conn_id)
                          if found_catalog['tap_stream_id'] in self.expected_check_stream_ids()]
        self.assertGreaterEqual(len(found_catalogs), 1)

        # Verify discovery generated the expected catalogs by name
        found_catalog_names = {catalog['stream_name'] for catalog in found_catalogs}
        self.assertSetEqual(self.expected_check_streams(), found_catalog_names)

        # verify that persisted streams have the correct properties
        test_catalog = found_catalogs[0]
        self.assertEqual(test_table_name, test_catalog['stream_name'])
        print("discovered streams are correct")

        # perform table selection
        print('selecting {} and all fields within the table'.format(test_table_name))
        self.select_streams_and_fields(conn_id, test_catalog, select_all_fields=True)

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

        # verify the persisted schema matches expectations
        actual_schema = records_by_stream[test_table_name]['schema']['properties']
        self.assertEqual(expected_schema, actual_schema)

        # verify the number of records and number of messages match our expectations
        expected_record_count = len(self.expected_records)
        expected_activate_version_count = 1 if self.default_replication_method is self.INCREMENTAL else 2
        expected_message_count = expected_record_count + expected_activate_version_count
        self.assertEqual(expected_record_count, record_count_by_stream[test_table_name])
        self.assertEqual(expected_message_count, len(messages))

        # verify we start and end syncs with an activate version message
        self.assertEqual('activate_version', messages[0]['action'])
        if self.default_replication_method in {self.FULL_TABLE, self.LOG_BASED}:
            self.assertEqual('activate_version', messages[-1]['action'])

        # verify the remaining messages are upserts
        actions = {message['action'] for message in messages if message['action'] != 'activate_version'}
        self.assertSetEqual({'upsert'}, actions)


        # Each record was inserted with a specific test case in mind
        for test_case, message in zip(self.expected_records.keys(), messages[1:]):
            with self.subTest(test_case=test_case):

                # grab our expected record
                expected_record = self.expected_records[test_case]


                # Verify replicated records match our expectations
                for field in self.expected_fields():
                    with self.subTest(field=field):

                        # unsupported fields should not be present in expected or actual records
                        if field.startswith("unsupported"):

                            expected_field_value = expected_record.get(field, "FIELD MISSING AS EXPECTED")
                            actual_field_value = message['data'].get(field, "FIELD MISSING AS EXPECTED")

                            self.assertEqual(expected_field_value, actual_field_value)


                        # some data types require adjustments to actual values to make valid comparison...
                        elif field == 'our_jsonb':

                            expected_field_value = expected_record.get(field, '{"MISSING": "EXPECTED FIELD"}')
                            actual_field_value = message['data'].get(field, '{"MISSING": "ACTUAL FIELD"}')

                            if actual_field_value is None:

                                self.assertIsNone(expected_field_value)

                            else:

                                actual_field_value = json.loads(actual_field_value)
                                self.assertDictEqual(expected_field_value, actual_field_value)


                        # but most types do not
                        else:

                            expected_field_value = expected_record.get(field, "MISSING EXPECTED FIELD")
                            actual_field_value = message['data'].get(field, "MISSING ACTUAL FIELD")

                            self.assertEqual(expected_field_value, actual_field_value)


SCENARIOS.add(PostgresDatatypes)
