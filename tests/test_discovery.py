import unittest
import psycopg2
import psycopg2.extras
import tap_postgres
import os
import pdb
from singer import get_logger, metadata
from tests.utils import get_test_connection, ensure_test_table

LOGGER = get_logger()

def do_not_dump_catalog(catalog):
    pass

tap_postgres.dump_catalog = do_not_dump_catalog

class TestStringTableWithPK(unittest.TestCase):
    maxDiff = None
    table_name = 'CHICKEN TIMES'
    def setUp(self):
       table_spec = {"columns": [{"name" : "id", "type" : "integer", "primary_key" : True, "serial" : True},
                                 {"name" : '"character-varying_name"',  "type": "character varying"},
                                 {"name" : '"varchar-name"',            "type": "varchar(28)"},
                                 {"name" :  'char_name',                "type": "char(10)"},
                                 {"name" : '"text-name"',               "type": "text"}],
                     "name" : TestStringTableWithPK.table_name}
       ensure_test_table(table_spec)

    def test_catalog(self):
        with get_test_connection() as conn:
            catalog = tap_postgres.do_discovery(conn)

            chicken_streams = [s for s in catalog.streams if s.table == TestStringTableWithPK.table_name]
            self.assertEqual(len(chicken_streams), 1)
            stream_dict = chicken_streams[0].to_dict()
            self.assertEqual(TestStringTableWithPK.table_name, stream_dict.get('table_name'))
            self.assertEqual(TestStringTableWithPK.table_name, stream_dict.get('stream'))
            self.assertEqual('public-{}'.format(TestStringTableWithPK.table_name), stream_dict.get('tap_stream_id'))

            stream_dict.get('metadata').sort(key=lambda md: md['breadcrumb'])


            self.assertEqual(metadata.to_map(stream_dict.get('metadata')),
                             {() : {'key-properties': ['id'], 'database-name': 'vagrant',
                                    'schema-name': 'public', 'is-view': False, 'row-count': 0},
                              ('properties', 'character-varying_name') : {'inclusion': 'available', 'sql-datatype' : 'character varying', 'selected-by-default' : True},
                              ('properties', 'id')                     : {'inclusion': 'automatic', 'sql-datatype' : 'integer', 'selected-by-default' : True},
                              ('properties', 'varchar-name')           : {'inclusion': 'available', 'sql-datatype' : 'character varying', 'selected-by-default' : True},
                              ('properties', 'text-name')              : {'inclusion': 'available', 'sql-datatype' : 'text', 'selected-by-default' : True},
                              ('properties', 'char_name'):               {'selected-by-default': True, 'inclusion': 'available', 'sql-datatype': 'character'}})

            self.assertEqual({'properties': {'id':                      {'type': ['integer'],
                                                                         'maximum':  2147483647,
                                                                         'minimum': -2147483648},
                                             'character-varying_name': {'type': ['null', 'string']},
                                             'varchar-name':           {'type': ['null', 'string'], 'maxLength': 28},
                                             'char_name':              {'type': ['null', 'string'], 'maxLength': 10, 'minLength': 10},
                                             'text-name':              {'type': ['null', 'string']}},
                                             'type': 'object'},  stream_dict.get('schema'))


class TestIntegerTable(unittest.TestCase):
    maxDiff = None
    table_name = 'CHICKEN TIMES'

    def setUp(self):
       table_spec = {"columns": [{"name" : "id",                "type" : "integer",  "serial" : True},
                                 {"name" : 'size integer',      "type" : "integer",  "quoted" : True},
                                 {"name" : 'size smallint',     "type" : "smallint", "quoted" : True},
                                 {"name" : 'size bigint',       "type" : "bigint",   "quoted" : True}],
                     "name" : TestIntegerTable.table_name}
       ensure_test_table(table_spec)

    def test_catalog(self):
        with get_test_connection() as conn:
            catalog = tap_postgres.do_discovery(conn)
            chicken_streams = [s for s in catalog.streams if s.table == TestIntegerTable.table_name]
            self.assertEqual(len(chicken_streams), 1)
            stream_dict = chicken_streams[0].to_dict()

            self.assertEqual(TestStringTableWithPK.table_name, stream_dict.get('table_name'))
            self.assertEqual(TestStringTableWithPK.table_name, stream_dict.get('stream'))
            self.assertEqual('public-{}'.format(TestStringTableWithPK.table_name), stream_dict.get('tap_stream_id'))


            stream_dict.get('metadata').sort(key=lambda md: md['breadcrumb'])

            self.assertEqual(metadata.to_map(stream_dict.get('metadata')),
                             {() : {'key-properties': [], 'database-name': os.getenv('TAP_POSTGRES_DATABASE'), 'schema-name': 'public', 'is-view': False, 'row-count': 0},
                              ('properties', 'id')                     : {'inclusion': 'available', 'sql-datatype' : 'integer', 'selected-by-default' : True},
                              ('properties', 'size integer')         : {'inclusion': 'available', 'sql-datatype' : 'integer', 'selected-by-default' : True},
                              ('properties', 'size smallint')        : {'inclusion': 'available', 'sql-datatype' : 'smallint', 'selected-by-default' : True},
                              ('properties', 'size bigint')          : {'inclusion': 'available', 'sql-datatype' : 'bigint',   'selected-by-default' : True}})

            self.assertEqual({'type': 'object',
                              'properties': {'id': {'type': ['null', 'integer'], 'minimum': -2147483648, 'maximum': 2147483647},
                                             'size smallint': {'type': ['null', 'integer'], 'minimum': -32768, 'maximum': 32767},
                                             'size integer': {'type': ['null', 'integer'], 'minimum': -2147483648, 'maximum': 2147483647},
                                             'size bigint': {'type': ['null', 'integer'], 'minimum': -9223372036854775808, 'maximum': 9223372036854775807}}},
                             stream_dict.get('schema'))



class TestDecimalPK(unittest.TestCase):
    maxDiff = None
    table_name = 'CHICKEN TIMES'

    def setUp(self):
       table_spec = {"columns": [{"name" : 'our_decimal',                "type" : "numeric", "primary_key": True},
                                 {"name" : 'our_decimal_10_2',           "type" : "decimal(10,2)"},
                                 {"name" : 'our_decimal_38_4',           "type" : "decimal(38,4)"}],
                     "name" : TestDecimalPK.table_name}
       ensure_test_table(table_spec)

    def test_catalog(self):
        with get_test_connection() as conn:
            catalog = tap_postgres.do_discovery(conn)
            chicken_streams = [s for s in catalog.streams if s.table == TestDecimalPK.table_name]
            self.assertEqual(len(chicken_streams), 1)
            stream_dict = chicken_streams[0].to_dict()

            stream_dict.get('metadata').sort(key=lambda md: md['breadcrumb'])

            self.assertEqual(metadata.to_map(stream_dict.get('metadata')),
                             {() : {'key-properties': ['our_decimal'], 'database-name': os.getenv('TAP_POSTGRES_DATABASE'), 'schema-name': 'public', 'is-view': False, 'row-count': 0},
                              ('properties', 'our_decimal')             : {'inclusion': 'automatic', 'sql-datatype' : 'numeric', 'selected-by-default' : True},
                              ('properties', 'our_decimal_38_4')        : {'inclusion': 'available', 'sql-datatype' : 'numeric', 'selected-by-default' : True},
                              ('properties', 'our_decimal_10_2')        : {'inclusion': 'available', 'sql-datatype' : 'numeric', 'selected-by-default' : True}})

            self.assertEqual({'properties': {'our_decimal': {'exclusiveMaximum': True,
                                                             'exclusiveMinimum': True,
                                                             'multipleOf': 10 ** (0 - tap_postgres.MAX_SCALE),
                                                             'maximum': 10 ** (tap_postgres.MAX_PRECISION - tap_postgres.MAX_SCALE),
                                                             'minimum': -10 ** (tap_postgres.MAX_PRECISION - tap_postgres.MAX_SCALE),
                                                             'type': ['number']},
                                                        'our_decimal_10_2': {'exclusiveMaximum': True,
                                                                             'exclusiveMinimum': True,
                                                                            'maximum': 100000000,
                                                                            'minimum': -100000000,
                                                                            'multipleOf': 0.01,
                                                                            'type': ['null', 'number']},
                                                        'our_decimal_38_4': {'exclusiveMaximum': True,
                                                                             'exclusiveMinimum': True,
                                                                             'maximum': 10000000000000000000000000000000000,
                                                                             'minimum': -10000000000000000000000000000000000,
                                                                             'multipleOf': 0.0001,
                                                                             'type': ['null', 'number']}},
                                         'type': 'object'},
                             stream_dict.get('schema'))



class TestDatesTablePK(unittest.TestCase):
    maxDiff = None
    table_name = 'CHICKEN TIMES'

    def setUp(self):
       table_spec = {"columns": [{"name" : 'our_date',                   "type" : "DATE", "primary_key": True },
                                 {"name" : 'our_ts',                     "type" : "TIMESTAMP"},
                                 {"name" : 'our_ts_tz',                  "type" : "TIMESTAMP WITH TIME ZONE"},
                                 {"name" : 'our_time',                   "type" : "TIME"},
                                 {"name" : 'our_time_tz',                "type" : "TIME WITH TIME ZONE"}],
                     "name" : TestDatesTablePK.table_name}
       ensure_test_table(table_spec)

    def test_catalog(self):
        with get_test_connection() as conn:
            catalog = tap_postgres.do_discovery(conn)
            chicken_streams = [s for s in catalog.streams if s.table == TestDatesTablePK.table_name]
            self.assertEqual(len(chicken_streams), 1)
            stream_dict = chicken_streams[0].to_dict()

            stream_dict.get('metadata').sort(key=lambda md: md['breadcrumb'])

            self.assertEqual(metadata.to_map(stream_dict.get('metadata')),
                             {() : {'key-properties': ['our_date'], 'database-name': os.getenv('TAP_POSTGRES_DATABASE'), 'schema-name': 'public', 'is-view': False, 'row-count': 0},
                              ('properties', 'our_date')           : {'inclusion': 'automatic', 'sql-datatype' : 'date', 'selected-by-default' : True},
                              ('properties', 'our_ts')             : {'inclusion': 'available', 'sql-datatype' : 'timestamp without time zone', 'selected-by-default' : True},
                              ('properties', 'our_ts_tz')          : {'inclusion': 'available', 'sql-datatype' : 'timestamp with time zone', 'selected-by-default' : True},
                              ('properties', 'our_time')           : {'inclusion': 'available', 'sql-datatype' : 'time without time zone', 'selected-by-default' : True},
                              ('properties', 'our_time_tz')        : {'inclusion': 'available', 'sql-datatype' : 'time with time zone', 'selected-by-default' : True}})

            self.assertEqual({'properties': {'our_date':               {'type': ['string'], 'format' : 'date-time'},
                                             'our_ts':                 {'type': ['null', 'string'], 'format' : 'date-time'},
                                             'our_ts_tz':              {'type': ['null', 'string'], 'format' : 'date-time'},
                                             'our_time':            {'type': ['null', 'string'], 'format' : 'date-time'},
                                             'our_time_tz':            {'type': ['null', 'string'], 'format' : 'date-time'}},
                                         'type': 'object'},
                             stream_dict.get('schema'))



class TestFloatTablePK(unittest.TestCase):
    maxDiff = None
    table_name = 'CHICKEN TIMES'

    def setUp(self):
       table_spec = {"columns": [{"name" : 'our_float',     "type" : "float", "primary_key": True },
                                 {"name" : 'our_real',      "type" : "real"},
                                 {"name" : 'our_double',    "type" : "double precision"}],
                     "name" : TestFloatTablePK.table_name}
       ensure_test_table(table_spec)

    def test_catalog(self):
        with get_test_connection() as conn:
            catalog = tap_postgres.do_discovery(conn)
            chicken_streams = [s for s in catalog.streams if s.table == TestFloatTablePK.table_name]
            self.assertEqual(len(chicken_streams), 1)
            stream_dict = chicken_streams[0].to_dict()

            stream_dict.get('metadata').sort(key=lambda md: md['breadcrumb'])
            self.assertEqual(metadata.to_map(stream_dict.get('metadata')),
                             {() : {'key-properties': ['our_float'], 'database-name': os.getenv('TAP_POSTGRES_DATABASE'), 'schema-name': 'public', 'is-view': False, 'row-count': 0},
                              ('properties', 'our_float')          : {'inclusion': 'automatic', 'sql-datatype' : 'double precision', 'selected-by-default' : True},
                              ('properties', 'our_real')           : {'inclusion': 'available', 'sql-datatype' : 'real', 'selected-by-default' : True},
                              ('properties', 'our_double')         : {'inclusion': 'available', 'sql-datatype' : 'double precision', 'selected-by-default' : True}})


            self.assertEqual({'properties': {'our_float':               {'type': ['number']},
                                             'our_real':                {'type': ['null', 'number']},
                                             'our_double':       {'type': ['null', 'number']}},
                                         'type': 'object'},
                             stream_dict.get('schema'))

class TestBoolsAndBits(unittest.TestCase):
    maxDiff = None
    table_name = 'CHICKEN TIMES'

    def setUp(self):
       table_spec = {"columns": [{"name" : 'our_bool',     "type" : "boolean" },
                                 {"name" : 'our_bit',     "type" : "bit" }],
                     "name" : TestBoolsAndBits.table_name}
       ensure_test_table(table_spec)

    def test_catalog(self):
        with get_test_connection() as conn:
            catalog = tap_postgres.do_discovery(conn)
            chicken_streams = [s for s in catalog.streams if s.table == TestFloatTablePK.table_name]
            self.assertEqual(len(chicken_streams), 1)
            stream_dict = chicken_streams[0].to_dict()

            stream_dict.get('metadata').sort(key=lambda md: md['breadcrumb'])
            self.assertEqual(metadata.to_map(stream_dict.get('metadata')),
                             {() : {'key-properties': [], 'database-name': os.getenv('TAP_POSTGRES_DATABASE'), 'schema-name': 'public', 'is-view': False, 'row-count': 0},
                              ('properties', 'our_bool')          : {'inclusion': 'available', 'sql-datatype' : 'boolean', 'selected-by-default' : True},
                              ('properties', 'our_bit')           : {'inclusion': 'available', 'sql-datatype' : 'bit', 'selected-by-default' : True}})


            self.assertEqual({'properties': {'our_bool':               {'type': ['null', 'boolean']},
                                             'our_bit':                {'type': ['null', 'boolean']}},
                              'type': 'object'},
                             stream_dict.get('schema'))

class TestJsonTables(unittest.TestCase):
    maxDiff = None
    table_name = 'CHICKEN TIMES'

    def setUp(self):
       table_spec = {"columns": [{"name" : 'our_secrets',        "type" : "json" },
                                 {"name" : 'our_secrets_b',     "type" : "jsonb" }],
                     "name" : TestJsonTables.table_name}
       ensure_test_table(table_spec)

    def test_catalog(self):
        with get_test_connection() as conn:
            catalog = tap_postgres.do_discovery(conn)
            chicken_streams = [s for s in catalog.streams if s.table == TestJsonTables.table_name]
            self.assertEqual(len(chicken_streams), 1)
            stream_dict = chicken_streams[0].to_dict()

            stream_dict.get('metadata').sort(key=lambda md: md['breadcrumb'])

            self.assertEqual(metadata.to_map(stream_dict.get('metadata')),
                             {() : {'key-properties': [], 'database-name': os.getenv('TAP_POSTGRES_DATABASE'), 'schema-name': 'public', 'is-view': False, 'row-count': 0},
                              ('properties', 'our_secrets')          : {'inclusion': 'available', 'sql-datatype' : 'json',  'selected-by-default' : True},
                              ('properties', 'our_secrets_b')        : {'inclusion': 'available', 'sql-datatype' : 'jsonb', 'selected-by-default' : True}})


            self.assertEqual({'properties': {'our_secrets':                  {'type': ['null', 'string']},
                                             'our_secrets_b':                {'type': ['null', 'string']}},
                              'type': 'object'},
                             stream_dict.get('schema'))


class TestUUIDTables(unittest.TestCase):
    maxDiff = None
    table_name = 'CHICKEN TIMES'

    def setUp(self):
       table_spec = {"columns": [{"name" : 'our_pk',        "type" : "uuid", "primary_key" : True },
                                 {"name" : 'our_uuid',      "type" : "uuid" }],
                     "name" : TestUUIDTables.table_name}
       ensure_test_table(table_spec)

    def test_catalog(self):
        with get_test_connection() as conn:
            catalog = tap_postgres.do_discovery(conn)
            chicken_streams = [s for s in catalog.streams if s.table == TestJsonTables.table_name]
            self.assertEqual(len(chicken_streams), 1)
            stream_dict = chicken_streams[0].to_dict()

            stream_dict.get('metadata').sort(key=lambda md: md['breadcrumb'])

            self.assertEqual(metadata.to_map(stream_dict.get('metadata')),
                             {() : {'key-properties': ['our_pk'], 'database-name': os.getenv('TAP_POSTGRES_DATABASE'), 'schema-name': 'public', 'is-view': False, 'row-count': 0},
                              ('properties', 'our_pk') : {'inclusion': 'automatic', 'sql-datatype' : 'uuid',  'selected-by-default' : True},
                              ('properties', 'our_uuid') : {'inclusion': 'available', 'sql-datatype' : 'uuid',  'selected-by-default' : True}})


            self.assertEqual({'properties': {'our_uuid':                  {'type': ['null', 'string']},
                                             'our_pk':                    {'type': ['string']}},
                              'type': 'object'},
                             stream_dict.get('schema'))

class TestHStoreTable(unittest.TestCase):
    maxDiff = None
    table_name = 'CHICKEN TIMES'

    def setUp(self):
       table_spec = {"columns": [{"name" : 'our_pk',          "type" : "hstore", "primary_key" : True },
                                 {"name" : 'our_hstore',      "type" : "hstore" }],
                     "name" : TestHStoreTable.table_name}
       with get_test_connection() as conn:
           cur = conn.cursor()
           cur.execute(""" SELECT installed_version FROM pg_available_extensions WHERE name = 'hstore' """)
           if cur.fetchone()[0] is None:
               cur.execute(""" CREATE EXTENSION hstore; """)


       ensure_test_table(table_spec)

    def test_catalog(self):
        with get_test_connection() as conn:
            catalog = tap_postgres.do_discovery(conn)
            chicken_streams = [s for s in catalog.streams if s.table == TestHStoreTable.table_name]
            self.assertEqual(len(chicken_streams), 1)
            stream_dict = chicken_streams[0].to_dict()
            stream_dict.get('metadata').sort(key=lambda md: md['breadcrumb'])

            # with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            #     cur.execute("""INSERT INTO "CHICKEN TIMES" (our_pk, our_hstore) VALUES ('size=>"small",name=>"betty"', 'size=>"big",name=>"fred"')""")
            #     cur.execute("""SELECT * FROM  "CHICKEN TIMES" """)
            #     wtf = cur.fetchall()


            self.assertEqual(metadata.to_map(stream_dict.get('metadata')),
                             {() : {'key-properties': ['our_pk'], 'database-name': os.getenv('TAP_POSTGRES_DATABASE'), 'schema-name': 'public', 'is-view': False, 'row-count': 0},
                              ('properties', 'our_pk') : {'inclusion': 'automatic', 'sql-datatype' : 'hstore',  'selected-by-default' : True},
                              ('properties', 'our_hstore') : {'inclusion': 'available', 'sql-datatype' : 'hstore',  'selected-by-default' : True}})


            self.assertEqual({'properties': {'our_hstore':                  {'type': ['null', 'string']},
                                             'our_pk':                    {'type': ['string']}},
                              'type': 'object'},
                             stream_dict.get('schema'))



#TODO:
#hstore

if __name__== "__main__":
    test1 = TestStringTableWithPK()
    test1.setUp()
    test1.test_catalog()
