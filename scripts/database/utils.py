import logging
import psycopg2
import cx_Oracle
import sys


# create a postgres connection
def create_postgres_connection(hostname, port, username, password, database):
    conn_string_template = "host = '{}' port='{}' user = '{}' password='{}' dbname ='{}' "
    conn_string = conn_string_template.format(hostname, port, username, password, database)

    try:
        connection = psycopg2.connect(conn_string)
        return connection
    except Exception as error:
        logging.error(error)
        sys.exit(-1)


# create an oracle connection
def create_oracle_connection(hostname, port, username, password, database):
    conn_string_template = "{}/{}@{}:{}/{}"
    conn_string = conn_string_template.format(username, password, hostname, port, database)

    try:
        connection = cx_Oracle.connect(conn_string)
        return connection
    except Exception as error:
        logging.error(error)
        sys.exit(-1)


# get tables to work with configured within metadata database
def get_table_info(metadata_config, metadata_schema, table_name=None):
    # get metadata database information
    hostname = metadata_config.get("hostname")
    port = metadata_config.get("port")
    username = metadata_config.get("username")
    password = metadata_config.get("password")
    database = metadata_config.get("database")

    # create metadata database connection and search for configured tables
    connection = create_postgres_connection(hostname,
                                            port,
                                            username,
                                            password,
                                            database)
    cursor = connection.cursor()

    try:
        query = """SELECT st.table_name,
                                 st.partition_column,
                                 st.num_partitions,
                                 sdc.date_column
                          FROM {}.stage_table st
                            LEFT JOIN {}.stage_date_column sdc
                            ON sdc.id = st.date_column_id""". \
                format(metadata_schema, metadata_schema)
        if table_name:
            query += " WHERE st.table_name = '{}'".format(table_name.lower())

        cursor.execute(query)
        table_info = cursor.fetchall()

        if not table_info:
            raise Exception("missing metadata information (maybe table is not configured?)")

        return table_info
    except Exception as error:
        logging.error(error)
        sys.exit(-1)


# create source connection to collect informations
def create_source_connection(db_type, hostname, port, username, password, database):
    if db_type == 'postgres':
        return create_postgres_connection(hostname, port, username, password, database)
    elif db_type == 'oracle':
        return create_oracle_connection(hostname, port, username, password, database)


# find lower and upper bound values in source database
def get_lower_upper_bound(connection, schema, table_name, partition_column):
    # define what kind of cursor should be used according to database type
    try:
        cursor = connection.cursor()
        query = "SELECT {}({}) FROM {}.{}"

        # get min id from table
        cursor.execute(query.format("min",
                                    partition_column,
                                    schema,
                                    table_name))
        lower = cursor.fetchone()[0]
        # get max id from table
        cursor.execute(query.format("max",
                                    partition_column,
                                    schema,
                                    table_name))
        upper = cursor.fetchone()[0]

        return lower, upper
    except Exception as error:
        logging.error(error)
        sys.exit(-1)
