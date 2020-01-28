from decimal import Decimal
import unittest
from unittest.mock import patch

from utils import get_test_connection_config
from tap_postgres.sync_strategies import logical_replication


class TestHandlingArrays(unittest.TestCase):
    def setUp(self):
        self.env = patch.dict(
            'os.environ', {
                'TAP_POSTGRES_HOST':'test',
                'TAP_POSTGRES_USER':'test',
                'TAP_POSTGRES_PASSWORD':'test',
                'TAP_POSTGRES_PORT':'5432'
            },
        )

        self.arrays = [
            '{10,01,NULL}',
            '{t,f,NULL}',
            '{127.0.0.1/32,10.0.0.0/32,NULL}',
            '{CASE_INSENSITIVE,case_insensitive,NULL,"CASE,,INSENSITIVE"}',
            '{2000-12-31,2001-01-01,NULL}',
            '{3.14159265359,3.1415926,NULL}',
            '{"\\"foo\\"=>\\"bar\\"","\\"baz\\"=>NULL",NULL}',
            '{1,2,NULL}',
            '{9223372036854775807,NULL}',
            '{198.24.10.0/24,NULL}',
            '{"{\\"foo\\":\\"bar\\"}",NULL}',
            '{"{\\"foo\\": \\"bar\\"}",NULL}',
            '{08:00:2b:01:02:03,NULL}',
            '{$19.99,NULL}',
            '{19.9999999,NULL}',
            '{3.14159,NULL}',
            '{0,1,NULL}',
            '{foo,bar,NULL,"foo,bar","diederik\'s motel "}',
            '{16:38:47,NULL}',
            '{"2019-11-19 11:38:47-05",NULL}',
            '{123e4567-e89b-12d3-a456-426655440000,NULL}'
        ]

        self.sql_datatypes = {
            'bit[]': bool,
            'boolean[]': bool,
            'cidr[]': str,
            'citext[]': str,
            'date[]': str,
            'double precision[]': float,
            'hstore[]': dict,
            'integer[]': int,
            'bigint[]': int,
            'inet[]': str,
            'json[]': str,
            'jsonb[]': str,
            'macaddr[]': str,
            'money[]': Decimal,
            'numeric[]': Decimal,
            'real[]': float,
            'smallint[]': int,
            'text[]': str,
            'time with time zone[]': str,
            'timestamp with time zone[]': str,
            'uuid[]': str,
        }

    def test_create_array_elem(self):
        expected_arrays = [
            ['10', '01' ,None],
            ['t', 'f', None],
            ['127.0.0.1/32', '10.0.0.0/32', None],
            ['CASE_INSENSITIVE', 'case_insensitive', None,"CASE,,INSENSITIVE"],
            ['2000-12-31', '2001-01-01', None],
            ['3.14159265359','3.1415926', None],
            ['"foo"=>"bar"', '"baz"=>NULL', None],
            ['1','2',None],
            ['9223372036854775807', None],
            ['198.24.10.0/24', None],
            ["{\"foo\":\"bar\"}", None],
            ["{\"foo\": \"bar\"}", None],
            ['08:00:2b:01:02:03', None],
            ['$19.99', None],
            ['19.9999999', None],
            ['3.14159', None],
            ['0','1', None],
            ['foo','bar',None,"foo,bar","diederik\'s motel "],
            ['16:38:47',None],
            ["2019-11-19 11:38:47-05",None],
            ['123e4567-e89b-12d3-a456-426655440000', None],
        ]
        for elem, expected_array in zip(self.arrays, expected_arrays):
            array = logical_replication.create_array_elem(elem)
            self.assertEqual(array, expected_array)

    def test_selected_value_to_singer_value_impl(self):
        with self.env:
            conn_info = get_test_connection_config()
            for elem, sql_datatype in zip(self.arrays, self.sql_datatypes.keys()):
                array = logical_replication.selected_value_to_singer_value(elem, sql_datatype, conn_info)

                for element in array:
                    python_datatype = self.sql_datatypes[sql_datatype]
                    if element:
                        self.assertIsInstance(element, python_datatype)

if __name__== "__main__":
    test1 = TestHandlingArrays()
    test1.setUp()
    test1.test_selected_value_to_singer_value_impl()
