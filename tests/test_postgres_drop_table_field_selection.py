import os
import unittest

import psycopg2.extras
from psycopg2.extensions import quote_ident
from tap_tester.scenario import SCENARIOS
import tap_tester.connections as connections
import tap_tester.menagerie   as menagerie
import tap_tester.runner      as runner

import db_utils  # pylint: disable=import-error

test_schema_name = "public"
test_table_name = "postgres_drop_table_test"

def canonicalized_table_name(schema, table, cur):
    return "{}.{}".format(quote_ident(schema, cur), quote_ident(table, cur))

class PostgresDropTable(unittest.TestCase):

    @staticmethod
    def name():
        return "tap_tester_postgres_drop_table_field_selection"

    @staticmethod
    def get_properties():
        return {'host' :   os.getenv('TAP_POSTGRES_HOST'),
                'dbname' : os.getenv('TAP_POSTGRES_DBNAME'),
                'port' : os.getenv('TAP_POSTGRES_PORT'),
                'user' : os.getenv('TAP_POSTGRES_USER'),
                'default_replication_method' : 'LOG_BASED',
                'filter_dbs' : 'discovery0'
        }

    @staticmethod
    def get_credentials():
        return {'password': os.getenv('TAP_POSTGRES_PASSWORD')}

    @staticmethod
    def get_type():
        return "platform.postgres"

    @staticmethod
    def tap_name():
        return "tap-postgres"

    @staticmethod
    def expected_check_streams():
        return { 'discovery0-public-postgres_drop_table_test'}


    def setUp(self):
        db_utils.ensure_environment_variables_set()

        db_utils.ensure_db('discovery0')

        with db_utils.get_test_connection('discovery0') as conn:
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

                #pylint: disable=line-too-long
                create_table_sql = 'CREATE TABLE {} (id SERIAL PRIMARY KEY)'.format(canonicalized_table_name(test_schema_name, test_table_name, cur))

                cur.execute(create_table_sql)

    def test_run(self):
        conn_id = connections.ensure_connection(self)

        # Run discovery
        check_job_name = runner.run_check_mode(self, conn_id)

        # Verify check exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        # There should not be any tables in this database
        with db_utils.get_test_connection('discovery0') as conn:
            cur = conn.cursor()
            cur.execute("DROP TABLE {}".format(canonicalized_table_name(test_schema_name, test_table_name, cur)))

        # Run discovery again
        check_job_name = runner.run_check_mode(self, conn_id)
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)

        # When discovery mode finds 0 tables, the tap returns an error
        self.assertEqual(exit_status['discovery_exit_status'], 1)




SCENARIOS.add(PostgresDropTable)
