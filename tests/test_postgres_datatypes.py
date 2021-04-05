import os
import datetime
import copy
import unittest
import decimal
from decimal import Decimal
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
import datatype_file_reader as dfr  # pylint: disable=import-error

test_schema_name = "public"
test_table_name = "postgres_datatypes_test"
test_db = "dev"

# TODO manually verify this schema meets our expectations
expected_schema = {'OUR DATE': {'format': 'date-time', 'type': ['null', 'string']},
                   'OUR TIME': {'type': ['null', 'string']},
                   'OUR TIME TZ': {'type': ['null', 'string']},
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
                   'our_cidr': {'type': ['null', 'string']},
                   'our_citext': {'type': ['null', 'string']},
                   'our_decimal': {'exclusiveMaximum': True,
                                   'exclusiveMinimum': True,
                                   'maximum': 100000000000000000000000000000000000000000000000000000000000000,
                                   'minimum': -100000000000000000000000000000000000000000000000000000000000000,
                                   'multipleOf': Decimal('1E-38'),
                                   'type': ['null', 'number']},
                   'our_double': {'type': ['null', 'number']},
                   'our_inet': {'type': ['null', 'string']},
                   'our_integer': {'maximum': 2147483647,
                                   'minimum': -2147483648,
                                   'type': ['null', 'integer']},
                   'our_json': {'type': ['null', 'string']}, # TODO Should this have a format??
                   'our_jsonb': {'type': ['null', 'string']},
                   'our_mac': {'type': ['null', 'string']},
                   'our_money': {'type': ['null', 'string']},
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
                   'our_uuid': {'type': ['null', 'string']},
                   'our_varchar': {'type': ['null', 'string']},
                   'our_varchar_big': {'maxLength': 10485760, 'type': ['null', 'string']}}


decimal.getcontext().prec = 131072 + 16383

whitespace = ' \t\n\r\v\f'
ascii_lowercase = 'abcdefghijklmnopqrstuvwxyz'
ascii_uppercase = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
ascii_letters = ascii_lowercase + ascii_uppercase
digits = '0123456789'
punctuation = r"""!"#$%&'()*+,-./:;<=>?@[\]^_`{|}~"""
our_ascii = ascii_letters + digits + punctuation + whitespace

class PostgresDatatypes(unittest.TestCase):
    """
    TODO | My Running list


    Arbitrary Precision Numbers
    Numeric | exact	up to 131072 digits before the decimal point; up to 16383 digits after the decimal point
              when precision is explicitly stated, maximum is 1000 digits
    TODOs
      [x] Generate 3 different fields with NUMERIC,
      []                                  NUMERIC(precision, scale),
      [x]                                  NUMERIC(precision).
      [x] Cover Maximum precision and scale
      [x] Cover Minimum precision and scale
      [x] Cover NaN


    Floating-Point Types
      - usually implementations of IEEE Standard 754 for Binary Floating-Point Arithmetic
      - on most platforms, the real type has a range of at least 1E-37 to 1E+37 with a precision of at least 6 decimal digits
      - double precision type typically has a range of around 1E-307 to 1E+308 with a precision of at least 15 digits
      - numbers too close to zero that are not representable as distinct from zero will cause an underflow error.
    TODOs
      [x] Cover NaN, -Inf, Inf
      [x] Zero


    Character
    TODOS
      [x] Generate different fields with VARCHAR,  VARCHAR(n),  CHAR,  CHAR(n)
      [x] VARCHAR(10485760)
      [] Generate a 1 GB string?? -- Not Possbile, but we can approach 20 MB
      [] text - cover one of every character that is allowed
            [] UTF8	Unicode, 8-bit	all	Yes	1-4	Unicode
            [] LATIN1	ISO 8859-1, ECMA 94	Western European	Yes	1	ISO88591
            [] LATIN2	ISO 8859-2, ECMA 94	Central European	Yes	1	ISO88592
            [] LATIN3	ISO 8859-3, ECMA 94	South European	Yes	1	ISO88593
            [] LATIN4	ISO 8859-4, ECMA 94	North European	Yes	1	ISO88594
            [] LATIN5	ISO 8859-9, ECMA 128	Turkish	Yes	1	ISO88599
            [] LATIN6	ISO 8859-10, ECMA 144	Nordic	Yes	1	ISO885910
            [] LATIN7	ISO 8859-13	Baltic	Yes	1	ISO885913
            [] LATIN8	ISO 8859-14	Celtic	Yes	1	ISO885914
            [] LATIN9	ISO 8859-15	LATIN1 with Euro and accents	Yes	1	ISO885915
            [] LATIN10	ISO 8859-16, ASRO SR 14111	Romanian	Yes	1	ISO885916
     [] investigate if we need to change COLLATION in order to accomplish ALL POSSIBLE CHARACTERS


    Network Address Types
    TODOs
      [x] min and max for cider/inet 000's fff's
      [x] ipv6 and ipv4


    Datetimes
    TODOs
      [x] Test all precisions 0..6 fractional seconds

    """

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

                db_utils.set_db_time_zone(cur, '+15:59')  #'America/New_York')

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



                # insert a record wtih minimum values
                test_case = 'minimum_boundary_general'
                our_tz = pytz.timezone('Singapore')  # GMT+8
                min_date = datetime.date(1, 1, 1)
                my_absurdly_small_decimal = decimal.Decimal('-' + '9' * 38 + '.' + '9' * 38) # THIS IS OUR LIMIT IN THE TARGET}
                # TODO |  BUG ? | The target blows up with greater than 38 digits before/after the decimal.
                #                 Is this a known/expected behavior or a BUG in the target?
                #                 It prevents us from testing what the tap claims to be able to support (100 precision, 38 scale) without rounding AND..
                #                 The postgres limits WITH rounding.
                # my_absurdly_small_decimal = decimal.Decimal('-' + '9' * 38 + '.' + '9' * 38) # THIS IS OUR LIMIT IN THE TARGET
                # my_absurdly_small_decimal = decimal.Decimal('-' + '9' * 62 + '.' + '9' * 37) # 131072 + 16383
                # my_absurdly_small_spec_decimal = decimal.Decimal('-' + '9'*500 + '.' + '9'*500)
                self.inserted_records.append({
                    'id': 1,# SERIAL PRIMARY KEY,
                    'our_char': "a",  #    CHAR,
                    'our_varchar': "",  #    VARCHAR,
                    'our_varchar_big': "",  #   VARCHAR(10485760),
                    'our_char_big': "a",  #   CHAR(10485760),
                    'our_text': "",  #   TEXT
                    'our_text_2': "",  #   TEXT,
                    'our_integer': -2147483648,  #    INTEGER,
                    'our_smallint': -32768,  #   SMALLINT,
                    'our_bigint': -9223372036854775808,  #       BIGINT,
                    'our_nospec_numeric': my_absurdly_small_decimal,  #    NUMERIC,
                    'our_numeric': my_absurdly_small_decimal,  #           NUMERIC(1000, 500),
                    'our_nospec_decimal': my_absurdly_small_decimal,  #    DECIMAL,
                    'our_decimal': my_absurdly_small_decimal,  #           DECIMAL(1000, 500),
                    quote_ident('OUR TS', cur): '0001-01-01T00:00:00.000001', # '4713-01-01 00:00:00.000000 BC',  #    TIMESTAMP WITHOUT TIME ZONE,
                    quote_ident('OUR TS TZ', cur): '0001-01-01T00:00:00.000001-15:59',#_tz, #'4713-01-01 00:00:00.000000 BC',  #    TIMESTAMP WITH TIME ZONE,
                    quote_ident('OUR TIME', cur): '00:00:00.000001',  #   TIME WITHOUT TIME ZONE,
                    quote_ident('OUR TIME TZ', cur): '00:00:00.000001-15:59',  #    TIME WITH TIME ZONE,
                    quote_ident('OUR DATE', cur): min_date,# '4713-01-01 BC',  #   DATE,
                    'our_double':  decimal.Decimal('-1.79769313486231e+308'), # DOUBLE PRECISION
                    'our_real': decimal.Decimal('-3.40282e+38'), #   REAL,
                    'our_boolean': False,  #    BOOLEAN,
                    'our_bit': '0',  #    BIT(1),
                    'our_json': json.dumps(dict()),  #       JSON,
                    'our_jsonb': json.dumps(dict()),  #    JSONB,
                    'our_uuid': '00000000-0000-0000-0000-000000000000', # str(uuid.uuid1())
                    'our_hstore': None,  # HSTORE,
                    'our_citext': "",  # CITEXT,
                    'our_cidr': '00.000.000.000/32',  # cidr,
                    'our_inet': '00.000.000.000',  # inet,
                    'our_mac': '00:00:00:00:00:00',  ## macaddr
                    'our_alignment_enum': None,  # ALIGNMENT,
                    'our_money': '-$92,233,720,368,547,758.08',  # money,
                    'our_bigserial': 1,  # BIGSERIAL,
                    'our_serial': 1,  # SERIAL,
                    'our_smallserial': 1,  #  SMALLSERIAL,
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
                for key in my_keys:
                    if key.startswith('"'):
                        del self.expected_records[test_case][key]
                db_utils.insert_record(cur, test_table_name, self.inserted_records[-1])


                # insert a record wtih maximum values
                test_case = 'maximum_boundary_general'
                max_ts = datetime.datetime(9999, 12, 31, 23, 59, 59, 999999)
                max_date = datetime.date(9999, 12, 31)
                base_string = "Bread Sticks From Olive Garden"
                my_absurdly_large_decimal = decimal.Decimal('9' * 38 + '.' + '9' * 38) # THIS IS OUR LIMIT IN THE TARGET}
                self.inserted_records.append({
                    'id': 2147483647,  # SERIAL PRIMARY KEY,
                    'our_char': "🥖",  #    CHAR,
                    'our_varchar': "a", #* 20971520,  #    VARCHAR,
                    'our_varchar_big': "🥖" + base_string,  #   VARCHAR(10485714),
                    'our_char_big': "🥖",  #   CHAR(10485760),
                    'our_text': "apples", #dfr.read_in("text"),  #   TEXT,
                    'our_text_2': None,  #   TEXT,
                    'our_integer': 2147483647,  #    INTEGER,
                    'our_smallint': 32767,  #   SMALLINT,
                    'our_bigint': 9223372036854775807,  #       BIGINT,
                    'our_nospec_numeric': my_absurdly_large_decimal,  #    NUMERIC,
                    'our_numeric': my_absurdly_large_decimal,  #           NUMERIC(1000, 500),
                    'our_nospec_decimal': my_absurdly_large_decimal,  #    DECIMAL,
                    'our_decimal': my_absurdly_large_decimal,  #           NUMERIC(1000, 500),
                    quote_ident('OUR TS', cur): max_ts,# '9999-12-31 24:00:00.000000',# '294276-12-31 24:00:00.000000',  #   TIMESTAMP WITHOUT TIME ZONE,
                    quote_ident('OUR TS TZ', cur): '9999-12-31T08:00:59.999999-15:59', #max_ts, #'294276-12-31 24:00:00.000000',  #    TIMESTAMP WITH TIME ZONE,
                    quote_ident('OUR TIME', cur): '23:59:59.999999',# '24:00:00.000000' ->,  #   TIME WITHOUT TIME ZONE,
                    quote_ident('OUR TIME TZ', cur): '23:59:59.999999+1559',  #    TIME WITH TIME ZONE,
                    quote_ident('OUR DATE', cur): '5874897-12-31',  #   DATE,
                    'our_double': decimal.Decimal('1.79769313486231e+308'),  # DOUBLE PRECISION,
                    'our_real': decimal.Decimal('3.40282e+38'), # '1E308',  #   REAL,
                    'our_boolean': True,  #    BOOLEAN
                    'our_bit': '1',  #    BIT(1),
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
                    }),  #       JSON,
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
                    }),  #    JSONB,
                    'our_uuid':'ffffffff-ffff-ffff-ffff-ffffffffffff', # UUID,
                    'our_hstore': '"foo"=>"bar","bar"=>"foo","dumdum"=>Null',  # HSTORE,
                    'our_citext': "aPpLeS",  # CITEXT,
                    'our_cidr': '199.199.199.128/32',  #  # cidr,
                    'our_inet': '199.199.199.128',  # inet,
                    'our_mac': 'ff:ff:ff:ff:ff:ff',  # macaddr
                    'our_alignment_enum': 'u g l y',  # ALIGNMENT,
                    'our_money': "$92,233,720,368,547,758.07",  # money,
                    'our_bigserial': 9223372036854775807,  # BIGSERIAL,
                    'our_serial': 2147483647,  # SERIAL,
                    'our_smallserial': 32767, #2147483647,  #  SMALLSERIAL,
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


                # insert a record with valid values for unsupported types
                test_case = 'unsupported_types'
                our_serial = 9999
                self.inserted_records.append({
                    'id': our_serial,
                    'our_bigserial': our_serial,
                    'our_serial': our_serial,
                    'our_smallserial': our_serial,
                    'unsupported_bit_varying': '01110100011000010111000000101101011101000110010101110011011101000110010101110010',  # BIT VARYING(80),
                    'unsupported_bit': '01110100011000010111000000101101011101000110010101110011011101000110010101110010',  #    BIT(80),
                    'unsupported_box': '((50, 50), (0, 0))',  # BOX,
                    'unsupported_bytea': "E'\\255'",  # BYTEA,
                    'unsupported_circle': '< (3, 1), 4 >',  # CIRCLE,
                    'unsupported_interval': '178000000 years',  # INTERVAL,
                    'unsupported_line': '{6, 6, 6}',  # LINE,
                    'unsupported_lseg': '(0 , 45), (45, 90)',  # LSEG,
                    'unsupported_path': '((0, 0), (45, 90), (2, 56))',  # PATH,
                    'unsupported_pg_lsn': '16/B374D848',  # PG_LSN,
                    'unsupported_point': '(1, 2)',  # POINT,
                    'unsupported_polygon': '((0, 0), (0, 10), (10, 0), (4, 5), (6, 7))',  #  POLYGON,
                    'unsupported_tsquery': "'fat' & 'rat'",  #  TSQUERY,
                    'unsupported_tsvector':  "'fat':2 'rat':3",  # TSVECTOR,
                    'unsupported_txid_snapshot': '10:20:10,14,15',  # TXID_SNAPSHOT,
                    'unsupported_xml': '<foo>bar</foo>',  # XML)
                })
                self.expected_records[test_case] = {
                    'id': self.inserted_records[-1]['id'],
                    'our_bigserial': self.inserted_records[-1]['our_bigserial'],
                    'our_serial': self.inserted_records[-1]['our_serial'],
                    'our_smallserial': self.inserted_records[-1]['our_smallserial'],
                }
                self.expected_records[test_case].update(self.null_out_remaining_fields(self.inserted_records[-1]))
                db_utils.insert_record(cur, test_table_name, self.inserted_records[-1])


                # TODO investigate how large our Nulls actually are ie. varchar how big?
                #      Don't need to be exact but we should get a rough idea of how large the record is.
                #      There is slight overhead in the record so it would be just undwer 20 megs.
                # text ~                     6.36 megabytes why can't we get any larger?


                # add a record with a text value that approaches the Stitch linmit ~ 20 Megabytes
                test_case = 'maximum_boundary_text'
                our_serial = 6
                single_record_limit = int((1024 * 1024 * 6.35) / 4 ) # 6.36 fails
                self.inserted_records.append({
                    'id': our_serial,
                    'our_bigserial': our_serial,
                    'our_serial': our_serial,
                    'our_smallserial': our_serial,
                    'our_text': single_record_limit * "🥖", # ~ 6 MB
                })
                self.expected_records[test_case] = copy.deepcopy(self.inserted_records[-1])
                self.expected_records[test_case].update(self.null_out_remaining_fields(self.inserted_records[-1]))
                db_utils.insert_record(cur, test_table_name, self.inserted_records[-1])


                # TODO | BUG_1 | We do not maintain -Infinity, Infinity, and NaN for
                #                floating-point or arbitrary-precision values


                # add a record with  -Inf for floating point types
                test_case = 'negative_infinity_floats'
                our_serial = 7
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
                our_serial = 8
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
                our_serial = 9
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
                our_serial = 10
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
                our_serial = 11
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
                our_serial = 12
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


                # TODO | BUG_2 | We do not preserve datetime precision.
                #                If a record has a decimal value it is padded to 6 digits of precision.
                #                This is not the expected behavior.


                # add a record with datetimes having .1 second precision
                test_case = '1_digits_of_precision_datetimes'
                our_serial = 13
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
                our_serial = 14
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


                # add a record with datetimes having .001 second precision
                test_case = '3_digits_of_precision_datetimes'
                our_serial = 15
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
                our_serial = 16
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
                our_serial = 17
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


                # add a record with datetimes having .000001 second precision
                test_case = '6_digits_of_precision_datetimes'
                our_serial = 18
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
                our_serial = 19
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
                our_serial = 20
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
                our_serial = 21
                self.inserted_records.append({
                    'id': our_serial,
                    'our_bigserial': our_serial,
                    'our_serial': our_serial,
                    'our_smallserial': our_serial,
                    'our_double': 0,
                    'our_real': 0, #   REAL,
                })
                self.expected_records[test_case] = copy.deepcopy(self.inserted_records[-1])
                self.expected_records[test_case].update(self.null_out_remaining_fields(self.inserted_records[-1]))
                db_utils.insert_record(cur, test_table_name, self.inserted_records[-1])


                # add a record with an hstore that has spaces, commas, arrows, quotes, and  escapes
                test_case = 'special_characters_hstore'
                our_serial = 22
                self.inserted_records.append({
                    'id': our_serial,
                    'our_bigserial': our_serial,
                    'our_serial': our_serial,
                    'our_smallserial': our_serial,
                    # psycopg2  does not let us insert: ' "backslash" => "\\",  "double_quote" => "\"" '
                    'our_hstore': ' "spaces" => " b a r ", "commas" => "f,o,o,", "arrow" => "=>", '
                })
                self.expected_records[test_case] = copy.deepcopy(self.inserted_records[-1])
                self.expected_records[test_case].update(self.null_out_remaining_fields(self.inserted_records[-1]))
                self.expected_records[test_case].update({
                    'our_hstore': {
                        "spaces": " b a r ",
                        "commas": "f,o,o,",
                        "arrow": "=>",
                        # "double_quote": "\"",
                        # "backslash": "\\"
                    }
                })
                db_utils.insert_record(cur, test_table_name, self.inserted_records[-1])


                # add a record with Null for every field
                test_case = 'null_for_all_fields_possible'
                our_serial = 23
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


                # TODO BUG_5 | The target prevents us from sending this record.
                #              The expectation was that values with higher precision than
                #              allowed, would be rounded and handled.


                # add a record with out-of-bounds precision for DECIMAL/NUMERIC
                # test_case = 'out_of_bounds_precision_decimal_and_numeric'
                # our_serial = 24
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
                our_serial = 26
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
                our_serial = 27
                our_unicode = ''
                for x in range(1114112):
                    if x == 0:  # skip 'null' because "ValueError: A string literal cannot contain NUL (0x00) characters."
                        continue

                    unicode_char = chr(x)
                    try:
                        _ = unicode_char.encode()
                    except UnicodeEncodeError: # there are a range of unicode chars that cannot be utf-8 encoded
                        continue

                    our_unicode += unicode_char

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

                # TODO MANUAL TESTING
                #      EXCEED THE PYTHON LIMITATIONS FOR
                #       [] datetimes
                #       [] hstore
                #           psycopg2  does not let us insert escaped characters:
                #           try this manually: ' "backslash" => "\\",  "double_quote" => "\"" '
                #       [] null text ie. '\x00', we can't input with psycopg2

    def null_out_remaining_fields(self, inserted_record):
        all_fields = self.expected_fields()
        unsupported_fields = self.expected_unsupported_fields()
        set_fields = set(inserted_record.keys())

        remaining_fields = all_fields.difference(set_fields).difference(unsupported_fields)
        remaining_valid_fields_to_null = {field: None for field in remaining_fields}

        return remaining_valid_fields_to_null

    @staticmethod
    def expected_check_streams():
        return { 'postgres_datatypes_test'}

    @staticmethod
    def expected_sync_streams():
        return { 'postgres_datatypes_test'}

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

    def test_run(self):
        """Parametrized datatypes test running against each replication method."""

        self.default_replication_method = self.FULL_TABLE
        full_table_conn_id = connections.ensure_connection(self, original_properties=False)
        self.datatypes_test(full_table_conn_id)

        # TODO Parametrize tests to also run against multiple local (db) timezones
        # with db_utils.get_test_connection(test_db) as conn:
        #     conn.autocommit = True
        #     with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:

        #         db_utils.set_db_time_zone('America/New_York')


        # self.default_replication_method = self.INCREMENTAL
        # incremental_conn_id = connections.ensure_connection(self, original_properties=False)
        # self.datatypes_test(incremental_conn_id)

        # self.default_replication_method = self.LOG_BASED
        # log_based_conn_id = connections.ensure_connection(self, original_properties=False)
        # self.datatypes_test(log_based_conn_id)


    def datatypes_test(self, conn_id):
        """
        Test Description:
          Basic Datatypes Test for a database tap.

        Test Cases:

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
        schema_and_metadata = menagerie.get_annotated_schema(conn_id, test_catalog['stream_id'])
        # TODO need to enable multiple replication methods (see auto fields test)
        additional_md = [{ "breadcrumb" : [], "metadata" : {'replication-method' : self.default_replication_method}}]
        _ = connections.select_catalog_and_fields_via_metadata(conn_id, test_catalog, schema_and_metadata, additional_md)

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

        # verify the persisted schema matches expectations TODO NEED TO GO TRHOUGH SCHEMA MANUALLY STILL
        actual_schema = records_by_stream[test_table_name]['schema']['properties']
        self.assertEqual(expected_schema, actual_schema)

        # verify the number of records and number of messages match our expectations
        expected_record_count = len(self.expected_records)
        expected_message_count = expected_record_count + 2 # activate versions
        self.assertEqual(expected_record_count, record_count_by_stream[test_table_name])
        self.assertEqual(expected_message_count, len(messages))

        # verify we start and end syncs with an activate version message
        self.assertEqual('activate_version', messages[0]['action'])
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


                        # but most type do not
                        else:

                            expected_field_value = expected_record.get(field, "MISSING EXPECTED FIELD")
                            actual_field_value = message['data'].get(field, "MISSING ACTUAL FIELD")

                            self.assertEqual(expected_field_value, actual_field_value)


SCENARIOS.add(PostgresDatatypes)
