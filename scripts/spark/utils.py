import logging
import sys
from collections import Counter
from hdfs import InsecureClient
from pyspark.sql import SparkSession


# create jdbc url for spark
def create_database_spark_url(**kwargs):
    database_type = kwargs.get("database_type")
    hostname = kwargs.get("hostname")
    port = kwargs.get("port")
    database = kwargs.get("database")
    username = kwargs.get("username")
    password = kwargs.get("password")

    postgres_url = "jdbc:{}ql://{}:{}/{}?user={}&password={}&port={}"
    oracle_url = "jdbc:{}:thin:{}/{}@//{}:{}/{}"

    url_connection = {"postgres": postgres_url.format(database_type,
                                                      hostname,
                                                      port,
                                                      database,
                                                      username,
                                                      password,
                                                      port),
                      "oracle": oracle_url.format(database_type,
                                                  username,
                                                  password,
                                                  hostname,
                                                  port,
                                                  database)}
    return url_connection[database_type]


# create a new spark session
def create_spark_session(aws_access_key, aws_secret_key, app_name):
    try:

        spark = SparkSession.builder. \
            config("fs.s3a.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem"). \
            config("fs.s3a.awsAccessKeyId", aws_access_key). \
            config("fs.s3a.awsSecretAccessKey", aws_secret_key). \
            config("fs.s3a.fast.upload", "true"). \
            config("fs.s3a.multipart.size", "1G"). \
            config("fs.s3a.fast.upload.buffer", "disk"). \
            config("fs.s3a.connection.maximum", 4). \
            config("fs.s3a.attempts.maximum", 20). \
            config("fs.s3a.connection.timeout", 30). \
            config("fs.s3a.threads.max", 10). \
            config("fs.s3a.buffer.dir", "hdfs:///user/hadoop/temporary/s3a"). \
            appName(app_name). \
            getOrCreate()

        return spark
    except Exception as e:
        logging.error(e)
        sys.exit(-1)


# close spark session
def stop_spark_context(spark):
    try:
        spark.sparkContext.stop()
    except Exception as error:
        logging.error(error)


# delete file from hdfs directory
def delete_hdfs_file(remove_from_local_hdfs, schema, table_name):
    # removing hdfs temporary files
    if remove_from_local_hdfs:
        # get private ip to connect to hdfs
        import socket
        private_ip = socket.gethostbyname(socket.gethostname())

        try:
            hdfs_client = InsecureClient(url="http://{}:8020".format(private_ip), user="spark")
            hdfs_client.delete("/user/hadoop/{}/{}".format(schema, table_name), recursive=True)
        except Exception as error:
            logging.error(error)


# read a etl sql file and return its content
def read_sql_file(project_name, country, script_name):
    try:
        with open('../etl/{}/{}/{}'.format(project_name, country, script_name)) as file:
            sql = " ".join(line.strip() for line in file)

        return sql
    except Exception as error:
        logging.error(error)
        sys.exit(-1)


# check if any config is not set, so it should not try to write on database
def check_write_dataframe_database(database_config):
    # count how many configs are none
    count_values = Counter(database_config.values())

    # if all configs are filled, then return true
    if count_values[None] == 0:
        return True
    # if none configs are filled, then return false
    if count_values[None] == 7:
        return False

    # incomplete number of fields filled to write on database
    raise Exception("Missing one or more database configurations to write on database")
