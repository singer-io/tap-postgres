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

                                 {"name" : '"character-varying name"',  "type": "character varying"},
                                 {"name" : '"varchar-name"',            "type": "varchar(28)"},
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
                              ('properties', 'character-varying name') : {'inclusion': 'available', 'sql-datatype' : 'character varying', 'selected-by-default' : True},
                              ('properties', 'id')                     : {'inclusion': 'automatic', 'sql-datatype' : 'integer', 'selected-by-default' : True},
                              ('properties', 'varchar-name')           : {'inclusion': 'available', 'sql-datatype' : 'character varying', 'selected-by-default' : True},
                              ('properties', 'text-name')              : {'inclusion': 'available', 'sql-datatype' : 'text', 'selected-by-default' : True}})

            self.assertEqual({'properties': {'id':                      {'type': ['integer'],
                                                                         'maximum':  2147483647,
                                                                         'minimum': -2147483648},
                                             'character-varying name': {'type': ['null', 'string']},
                                             'varchar-name':           {'type': ['null', 'string'], 'maxLength': 28},
                                             'text-name':              {'type': ['null', 'string']}},
                                             'type': 'object'},  stream_dict.get('schema'))


class TestIntegerTablePK(unittest.TestCase):
    maxDiff = None

    def setUp(self):
       table_spec = {"columns": [{"name" :  "size_pk   ",            "type" : "number(4,0)", "primary_key" : True, "serial" : True},
                                 {"name" : '"size_number_4_0"',      "type" : "number(4,0)"},
                                 {"name" : '"size_number_*_0"',      "type" : "number(*,0)"},
                                 {"name" : '"size_number_10_-1"',    "type" : "number(10,-1)"},
                                 {"name" : '"size_number_integer"',  "type" : "integer"},
                                 {"name" : '"size_number_int"',      "type" : "int"},
                                 {"name" : '"size_number_smallint"', "type" : "smallint"}],
                     "name" : '"CHICKEN"'}
       ensure_test_table(table_spec)

    def test_catalog(self):
        with get_test_connection() as conn:
            catalog = tap_postgres.do_discovery(conn)
            chicken_streams = [s for s in catalog.streams if s.table == 'CHICKEN']
            self.assertEqual(len(chicken_streams), 1)
            stream_dict = chicken_streams[0].to_dict()

            stream_dict.get('metadata').sort(key=lambda md: md['breadcrumb'])

            self.assertEqual({'schema': {'properties': {'size_number_10_-1':    {'maximum': 9999999999, 'minimum': -9999999999,
                                                                                 'type': ['null', 'integer'],
                                                                                 'multipleOf': 10 },
                                                        'size_number_*_0':      {'maximum': 99999999999999999999999999999999999999, 'minimum': -99999999999999999999999999999999999999,
                                                                                 'type': ['null', 'integer']},
                                                        'size_number_integer':  {'maximum': 99999999999999999999999999999999999999, 'minimum': -99999999999999999999999999999999999999,
                                                                                 'type': ['null', 'integer']},
                                                        'size_number_4_0':      {'maximum': 9999, 'minimum': -9999,
                                                                                 'type': ['null', 'integer']},
                                                        'size_number_int':      {'maximum': 99999999999999999999999999999999999999, 'minimum': -99999999999999999999999999999999999999,
                                                                                 'type': ['null', 'integer']},
                                                        'size_number_smallint': {'maximum': 99999999999999999999999999999999999999, 'minimum': -99999999999999999999999999999999999999,
                                                                                 'type': ['null', 'integer']},
                                                        'SIZE_PK':               {'maximum': 9999, 'minimum': -9999,
                                                                                  'type': ['integer']}},
                                         'type': 'object'},
                              'stream': 'CHICKEN',
                              'table_name': 'CHICKEN',
                              'tap_stream_id': 'ROOT-CHICKEN',
                              'metadata': [{'metadata': {'key-properties': ['SIZE_PK'],
                                                         'database-name': os.getenv('TAP_POSTGRES_SID'),
                                                         'schema-name': 'ROOT',
                                                         'is-view': False,
                                                         'row-count': 0},
                                            'breadcrumb': ()},
                                           {'metadata': {'inclusion': 'automatic'}, 'breadcrumb': ('properties', 'SIZE_PK')},
                                           {'metadata': {'inclusion': 'available'}, 'breadcrumb': ('properties', 'size_number_*_0')},
                                           {'metadata': {'inclusion': 'available'}, 'breadcrumb': ('properties', 'size_number_10_-1')},
                                           {'metadata': {'inclusion': 'available'}, 'breadcrumb': ('properties', 'size_number_4_0')},
                                           {'metadata': {'inclusion': 'available'}, 'breadcrumb': ('properties', 'size_number_int')},
                                           {'metadata': {'inclusion': 'available'}, 'breadcrumb': ('properties', 'size_number_integer')},
                                           {'metadata': {'inclusion': 'available'}, 'breadcrumb': ('properties', 'size_number_smallint')}]},

                             stream_dict)



class TestDecimalPK(unittest.TestCase):
    maxDiff = None

    def setUp(self):
       table_spec = {"columns": [{"name" : '"our_number"',                "type" : "number", "primary_key": True},
                                 {"name" : '"our_number_10_2"',           "type" : "number(10,2)"},
                                 {"name" : '"our_number_38_4"',           "type" : "number(38,4)"}],
                     "name" : "CHICKEN"}
       ensure_test_table(table_spec)

    def test_catalog(self):
        with get_test_connection() as conn:
            catalog = tap_postgres.do_discovery(conn)
            chicken_streams = [s for s in catalog.streams if s.table == 'CHICKEN']
            self.assertEqual(len(chicken_streams), 1)
            stream_dict = chicken_streams[0].to_dict()
            stream_dict.get('metadata').sort(key=lambda md: md['breadcrumb'])
            self.assertEqual({'schema': {'properties': {'our_number': {'maximum': 99999999999999999999999999999999999999,
                                                                       'minimum': -99999999999999999999999999999999999999,
                                                                       'type': [ 'integer']},
                                                        'our_number_10_2': {'exclusiveMaximum': True,
                                                                            'exclusiveMinimum': True,
                                                                            'maximum': 100000000,
                                                                            'minimum': -100000000,
                                                                            'multipleOf': 0.01,
                                                                            'type': ['null', 'number']},
                                                        'our_number_38_4': {'exclusiveMaximum': True,
                                                                             'exclusiveMinimum': True,
                                                                             'maximum': 10000000000000000000000000000000000,
                                                                             'minimum': -10000000000000000000000000000000000,
                                                                             'multipleOf': 0.0001,
                                                                             'type': ['null', 'number']}},
                                         'type': 'object'},
                              'stream': 'CHICKEN',
                              'table_name': 'CHICKEN',
                              'tap_stream_id': 'ROOT-CHICKEN',
                              'metadata': [{'breadcrumb': (),
                                            'metadata': {'key-properties': ['our_number'],
                                                         'database-name': os.getenv('TAP_POSTGRES_SID'),
                                                         'schema-name': 'ROOT',
                                                         'is-view': False,
                                                         'row-count': 0}},
                                           {'breadcrumb': ('properties', 'our_number'), 'metadata': {'inclusion': 'automatic'}},
                                           {'breadcrumb': ('properties', 'our_number_10_2'), 'metadata': {'inclusion': 'available'}},
                                           {'breadcrumb': ('properties', 'our_number_38_4'), 'metadata': {'inclusion': 'available'}}]},
                             stream_dict)


class TestDatesTablePK(unittest.TestCase):
    maxDiff = None

    def setUp(self):
       table_spec = {"columns": [{"name" : '"our_date"',                   "type" : "DATE", "primary_key": True },
                                 {"name" : '"our_ts"',                     "type" : "TIMESTAMP"},
                                 {"name" : '"our_ts_tz"',                  "type" : "TIMESTAMP WITH TIME ZONE"},
                                 {"name" : '"our_ts_tz_local"',            "type" : "TIMESTAMP WITH LOCAL TIME ZONE"}],
                     "name" : "CHICKEN"}
       ensure_test_table(table_spec)

    def test_catalog(self):
        with get_test_connection() as conn:
            catalog = tap_postgres.do_discovery(conn)
            chicken_streams = [s for s in catalog.streams if s.table == 'CHICKEN']
            self.assertEqual(len(chicken_streams), 1)
            stream_dict = chicken_streams[0].to_dict()

            stream_dict.get('metadata').sort(key=lambda md: md['breadcrumb'])

            self.assertEqual({'schema': {'properties': {'our_date':               {'type': ['string'], 'format' : 'date-time'},
                                                        'our_ts':                 {'type': ['null', 'string'], 'format' : 'date-time'},
                                                        'our_ts_tz':              {'type': ['null', 'string'], 'format' : 'date-time'},
                                                        'our_ts_tz_local':        {'type': ['null', 'string'], 'format' : 'date-time'}},
                                         'type': 'object'},
                              'stream': 'CHICKEN',
                              'table_name': 'CHICKEN',
                              'tap_stream_id': 'ROOT-CHICKEN',
                              'metadata':
                              [{'breadcrumb': (),
                                'metadata': {'key-properties': ['our_date'],
                                             'database-name': os.getenv('TAP_POSTGRES_SID'),
                                             'schema-name': 'ROOT',
                                             'is-view': 0,
                                             'row-count': 0}},
                               {'breadcrumb': ('properties', 'our_date'),
                                'metadata': {'inclusion': 'automatic'}},
                               {'breadcrumb': ('properties', 'our_ts'),
                                'metadata': {'inclusion': 'available'}},
                               {'breadcrumb': ('properties', 'our_ts_tz'),
                                'metadata': {'inclusion': 'available'}},
                               {'breadcrumb': ('properties', 'our_ts_tz_local'),
                                'metadata': {'inclusion': 'available'}}]},

                             stream_dict)


class TestFloatTablePK(unittest.TestCase):
    maxDiff = None

    def setUp(self):
       table_spec = {"columns": [{"name" : '"our_float"',                 "type" : "float", "primary_key": True },
                                 {"name" : '"our_double_precision"',      "type" : "double precision"},
                                 {"name" : '"our_real"',                  "type" : "real"},
                                 {"name" : '"our_binary_float"',          "type" : "binary_float"},
                                 {"name" : '"our_binary_double"',         "type" : "binary_double"}],
                     "name" : "CHICKEN"}
       ensure_test_table(table_spec)

    def test_catalog(self):
        with get_test_connection() as conn:
            catalog = tap_postgres.do_discovery(conn)
            chicken_streams = [s for s in catalog.streams if s.table == 'CHICKEN']
            self.assertEqual(len(chicken_streams), 1)
            stream_dict = chicken_streams[0].to_dict()

            stream_dict.get('metadata').sort(key=lambda md: md['breadcrumb'])
            self.assertEqual({'schema': {'properties': {'our_float':               {'type': ['number'],
                                                                                    'multipleOf': 1e-38},
                                                        'our_double_precision':    {'type': ['null', 'number'],
                                                                                    'multipleOf': 1e-38},
                                                        'our_real':                {'type': ['null', 'number'],
                                                                                    'multipleOf': 1e-18},
                                                        'our_binary_float':        {'type': ['null', 'number']},
                                                        'our_binary_double':       {'type': ['null', 'number']}},
                                         'type': 'object'},
                              'stream': 'CHICKEN',
                              'table_name': 'CHICKEN',
                              'tap_stream_id': 'ROOT-CHICKEN',
                              'metadata': [{'breadcrumb': (),
                                            'metadata': {'key-properties': ["our_float"],
                                                         'database-name': os.getenv('TAP_POSTGRES_SID'),
                                                         'schema-name': 'ROOT',
                                                         'is-view': False,
                                                         'row-count': 0}},
                                           {'breadcrumb': ('properties', 'our_binary_double'), 'metadata': {'inclusion': 'available'}},
                                           {'breadcrumb': ('properties', 'our_binary_float'), 'metadata': {'inclusion': 'available'}},
                                           {'breadcrumb': ('properties', 'our_double_precision'), 'metadata': {'inclusion': 'available'}},
                                           {'breadcrumb': ('properties', 'our_float'), 'metadata': {'inclusion': 'automatic'}},
                                           {'breadcrumb': ('properties', 'our_real'), 'metadata': {'inclusion': 'available'}}]},
                             stream_dict)
if __name__== "__main__":
    test1 = TestStringTableWithPK()
    test1.setUp()
    test1.test_catalog()
