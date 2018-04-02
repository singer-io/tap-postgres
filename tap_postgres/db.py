import psycopg2
import psycopg2.extras

def canonicalize_identifier(identifier):
    return identifier

def fully_qualified_column_name(schema, table, column):
    return '"{}"."{}"."{}"'.format(canonicalize_identifier(schema), canonicalize_identifier(table), canonicalize_identifier(column))

def fully_qualified_table_name(schema, table):
    return '"{}"."{}"'.format(canonicalize_identifier(schema), canonicalize_identifier(table))


def open_connection(conn_config, logical_replication=False):
    conn_string = "host='{}' dbname='{}' user='{}' password='{}' port='{}'".format(conn_config['host'],
                                                                                   conn_config['dbname'],
                                                                                   conn_config['user'],
                                                                                   conn_config['password'],
                                                                                   conn_config['port'])
    if logical_replication:
        conn = psycopg2.connect(conn_string, connection_factory=psycopg2.extras.LogicalReplicationConnection)
    else:
        conn = psycopg2.connect(conn_string)

    return conn
