import os
import psycopg2

def ensure_db(dbname=os.getenv('TAP_POSTGRES_DBNAME')):
    # Create database dev if not exists
    with get_test_connection() as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = '{}'".format(dbname))
            exists = cur.fetchone()
            if not exists:
                print("Creating database {}".format(dbname))
                cur.execute("CREATE DATABASE {}".format(dbname))

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
