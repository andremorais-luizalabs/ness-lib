"""
PostgreSQL abstraction layer
"""

import pprint
from time import sleep
import psycopg2
from .log import get_logger

_QUERY_RETRIES = 6
_QUERY_RETRIES_INTERVAL = 10  # Seconds


def connect(host, database_name, user, password):
    """
    Connect to database
    """
    connection = None
    try:
        get_logger().debug('Connecting to PostgreSQL...')
        connection = psycopg2.connect(
            host=host, dbname=database_name, user=user, password=password)
    except Exception as exc:
        get_logger().exception(exc)
        get_logger().error(
            'Unable to connect to the host <%s> with the user <%s>', host,
            user)
        raise Exception(exc)
    get_logger().debug(
        'PostgreSQL connected to the host <%s> with th user <%s>', host, user)
    return connection


def disconnect(connection):
    """
    Disconnect from database
    """
    connection.close()
    get_logger().debug('PostgreSQL Disconnected')


def run_query(connection, query):
    """
    Run query in postgresql with protection
    """
    cur = connection.cursor()

    success = False
    for _ in range(_QUERY_RETRIES):
        try:
            cur.execute(query)
            get_logger().debug('Query execute successfully')
            connection.commit()
            success = True
            break
        except psycopg2.extensions.TransactionRollbackError as exc:
            connection.rollback()
            get_logger().exception(exc)
            get_logger().error(
                'Unable to run query: %s \nAgainst the host <%s>, database <%s>, user <%s>',
                query,
                connection.get_dsn_parameters()['host'],
                connection.get_dsn_parameters()['dbname'],
                connection.get_dsn_parameters()['user'])
            sleep(_QUERY_RETRIES_INTERVAL)
    if success is False:
        raise Exception('Query run failed!')

    return cur


def get_query_description(connection, query):
    """
    Get only 1 row of the query
    """
    get_logger().debug('Original query: %s', query)
    query = query.replace(';', '')
    query_limited = 'select * from (%s) as query limit 1;' % query
    get_logger().debug('Limited query: %s', query_limited)
    cur = run_query(connection, query_limited)
    description = cur.description
    cur.close()
    return description


def get_db_types(connection):
    """
    Get all db types
    """
    db_types_query = '''
    SELECT pg_type.typname, pg_type.oid
    FROM pg_type JOIN pg_namespace
        ON typnamespace = pg_namespace.oid;
    '''
    get_logger().debug('Database types query: \n%s', db_types_query)
    cur = run_query(connection, db_types_query)
    rows = cur.fetchall()
    cur.close()
    return rows


def merge_types_info(query_description, database_types):
    """
    merge_types_info
    """
    desc_idx_name = 0
    desc_idx_type_code = 1
    desc_idx_null_ok = 6
    types_idx_type_name = 0
    types_idx_type_code = 1

    cols = []
    for col in query_description:
        new_col = {'name': col[desc_idx_name], 'type_code': col[desc_idx_type_code], 'null_ok': col[desc_idx_null_ok],
                   'type_name': None}

        for type_tp in database_types:
            if type_tp[types_idx_type_code] == col[desc_idx_type_code]:
                new_col['type_name'] = type_tp[types_idx_type_name]
                break

        cols.append(new_col)

    return cols


def get_query_schema(connection, query):
    """
    Get query schema
    """
    query_description = get_query_description(connection, query)
    get_logger().debug('Query description: \n%s',
                       pprint.pformat(query_description))
    database_types = get_db_types(connection)
    get_logger().debug('Database types: \n%s', pprint.pformat(database_types))
    cols_info = merge_types_info(query_description, database_types)
    get_logger().debug('Columns informations: \n%s', pprint.pformat(cols_info))
    return cols_info


def get_table_schema(connection, table):
    """
    Get query schema
    """
    query = 'select * from %s;' % table
    cols_info = get_query_schema(connection, query)
    return cols_info


def get_table_size(connection, table):
    """
    Get estimated table size
    """
    schema_name = table.split('.')[0]
    table_name = table.split('.')[1]

    table_size_query = '''
    select
        pg_catalog.pg_namespace.nspname as schema_name,
        relname as table_name,
        pg_relation_size(pg_catalog.pg_class.oid) as table_size
    from pg_catalog.pg_class
        join pg_catalog.pg_namespace on relnamespace = pg_catalog.pg_namespace.oid
        and relname = '%s' and pg_catalog.pg_namespace.nspname = '%s';
    ''' % (table_name, schema_name)
    get_logger().debug('Database table size query: \n%s', table_size_query)
    cur = run_query(connection, table_size_query)
    rows = cur.fetchall()
    cur.close()
    return rows[0][2]
