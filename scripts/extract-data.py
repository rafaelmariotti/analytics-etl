#!/usr/bin/env python

"""
Extract tables from OLTP database and save data as parquet or csv files in HDFS or S3 bucket repository.


Usage:
    extract-data.py [options]
    extract-data.py --version
    extract-data.py (-h | --help)


Options:
    -H SOURCE-DATABASE, --source-hostname=SOURCE-DATABASE   Source database hostname
    -p SOURCE-PORT, --source-port=SOURCE-PORT               Source database port
    -u SOURCE-USER, --source-username=SOURCE-USER           Source database user
    -p SOURCE-PASSWORD, --source-password=SOURCE-PASSWORD   Source database password
    -d SOURCE-DATABASE, --source-database=SOURCE-DATABASE   Source database name/service name
    -s SOURCE-SCHEMA, --source-schema=SOURCE-SCHEMA         Source database schema
    -t SOURCE-TYPE, --source-type=SOURCE-TYPE               Source database type [default: oracle]
    -T TABLE-NAME, --single-table=TABLE-NAME                Single database table to extract. If empty, all tables will be extracted
    --save-s3=SAVE-S3                                       Flag to either save contents to s3 bucket [default: True]
    --use-s3-dist-cp=USE-S3-DIST-CP                         Flag to either save contents to s3 using AWS wrapped hadoop s3distcp [default: True]
    --save-hdfs=SAVE-HDFS                                   Flag to either save contents to hdfs [default: True]
    --base-directory=DIRECTORY                              Base directory location to save files [default: hdfs:///user/hadoop/stage]
    --project-name=PROJECT-NAME                             Project name to save on base directory [default: $SOURCE-SCHEMA]
    -a AWS-ACCESS-KEY, --aws-access-key=AWS-ACCESS-KEY      AWS Access Key to write on S3 bucket
    -k AWS-SECRET-KEY, --aws-secret-key=AWS-SECRET-KEY      AWS Access Key to write on S3 bucket
    -b S3-BUCKET-NAME, --s3-bucket-name=S3-BUCKET-NAME      S3 bucket name to write source tables
    -f, --read-fetch-size=READ-FETCH-SIZE                   Read fetch size from source database [default: 20000]
    --remove-from-local-hdfs=FLAG-REMOVE-FROM-LOCAL-HDFS    Flag to either remove or not all the local hdfs files after process [default: False]
    -l LOG-LEVEL, --log-level=LOG-LEVEL                     Script log level information [default: INFO]
    --file-type=FILE-TYPE                                   File type output (current suports csv and parquet) [default: parquet]
    --csv-separator=CSV-SEPARATOR                           String to separate all fields from CSV file [default: ,]
    --csv-header=HEADER                                     Flag to either write or not the CSV header to file output [default: False]
    --infer-schema=INFER-SCHEMA                             Flag to either infer or not the dataframe schema on Spark [default: False]
    --config-file=CONFIG-FILE                               Configuration file [default: ../config/extract-data.yml]
"""

import sys
import os
import logging
from docopt import docopt
from config.utils import get_config
from logger.utils import set_log
from s3.utils import validate_s3_interface, send_s3_using_s3_dist_cp
from spark.utils import create_database_spark_url, create_spark_session, delete_hdfs_file, stop_spark_context
from spark.read import read_from_database, read_from_file_system
from spark.write import write_dataframe_into_file_system, write_dataframe_into_s3
from database.utils import get_table_info, create_source_connection, get_lower_upper_bound


# main function to run
def run_job(args, config):
    # get spark job name to set on Spark interface
    metadata_config = config.get("metadata")
    app_name_template = config.get('global').get('description')

    # read source database informations
    try:
        source_type = args["--source-type"].lower()
        source_hostname = args["--source-hostname"].lower()
        source_port = args["--source-port"].lower()
        source_username = args["--source-username"].lower()
        source_password = args["--source-password"]
        source_database = args["--source-database"].lower()
        source_schema = args["--source-schema"].lower()
    except AttributeError:
        logging.error('error while parsing attribute (missing value)')
        sys.exit(-1)
    single_table = args["--single-table"]

    # read spark/hadoop informations
    read_fetch_size = args["--read-fetch-size"]
    remove_from_local_hdfs = (args["--remove-from-local-hdfs"].lower() == "true")
    save_hdfs = (args["--save-hdfs"].lower() == "true")

    # file system saving info
    base_directory = args["--base-directory"]
    project_name = args["--project-name"].replace("$SOURCE-SCHEMA", source_schema)
    save_dir = "{}/{}".format(base_directory, project_name)

    # write s3 bucket informations
    use_s3_dist_cp = (args["--use-s3-dist-cp"].lower() == "true")
    aws_access_key = args["--aws-access-key"]
    aws_secret_key = args["--aws-secret-key"]
    s3_bucket = args["--s3-bucket-name"]

    # file related args
    file_type = args["--file-type"]
    separator = args["--csv-separator"]
    header = (args["--csv-header"].lower() == "true")
    infer_schema = (args["--infer-schema"].lower() == "true")

    # validate if s3 interface is valid with given option
    validate_s3_interface(use_s3_dist_cp, s3_bucket)

    # generate jdbc_url for spark
    jdbc_url = create_database_spark_url(database_type=source_type,
                                         hostname=source_hostname,
                                         port=source_port,
                                         database=source_database,
                                         username=source_username,
                                         password=source_password)

    # create spark session/context
    app_name = app_name_template.format(source_type,
                                        source_database,
                                        source_schema,
                                        file_type)
    spark = create_spark_session(aws_access_key,
                                 aws_secret_key,
                                 app_name)

    for table_info in get_table_info(metadata_config, project_name, single_table):
        # parse table name, partition column and date column informations
        table_name = table_info[0].lower()
        partition_column = table_info[1].lower()
        num_partitions = table_info[2]
        date_column = table_info[3]
        if date_column is not None:
            date_column = date_column.lower()

        # create a database connection with source database
        source_connection = create_source_connection(source_type,
                                                     source_hostname,
                                                     source_port,
                                                     source_username,
                                                     source_password,
                                                     source_database)

        # collect lower and upper bound values
        lower, upper = get_lower_upper_bound(source_connection,
                                             source_schema,
                                             table_name,
                                             partition_column)
        # close source database connection
        source_connection.close()

        log_partition_col = "partition_col: {}".format(partition_column)
        log_date_column = "date_col: {}".format(date_column)
        log_num_partitions = "num_partitions: {}".format(num_partitions)
        log_lower_upper_bound = "lower_upper_bound: ({}, {})".format(lower, upper)
        log_fetch_size = "fetch_size: {}".format(read_fetch_size)
        logging.debug("config : {{ {}, {}, {}, {}, {} }}"
                      .format(log_partition_col,
                              log_date_column,
                              log_num_partitions,
                              log_lower_upper_bound,
                              log_fetch_size))

        if save_hdfs:
            # read table from database and create a spark dataframe
            df_table = read_from_database(spark=spark,
                                          jdbc_url=jdbc_url,
                                          schema=source_schema,
                                          table_name=table_name,
                                          partition_column=partition_column,
                                          num_partitions=num_partitions,
                                          lower=lower,
                                          upper=upper,
                                          read_fetch_size=read_fetch_size)
            # write the dataframe into hdfs
            write_dataframe_into_file_system(spark=spark,
                                             dataframe=df_table,
                                             table_name=table_name,
                                             file_type=file_type,
                                             separator=separator,
                                             header=header,
                                             infer_schema=infer_schema,
                                             save_dir=save_dir)

        # send file to s3
        if s3_bucket is not None:
            if not use_s3_dist_cp:
                # read table from hdfs and create a spark dataframe
                df_table = read_from_file_system(spark=spark,
                                                 table_name=table_name,
                                                 file_type=file_type,
                                                 separator=separator,
                                                 header=header,
                                                 infer_schema=infer_schema,
                                                 save_dir=save_dir)
                # write dataframe into s3 through spark
                write_dataframe_into_s3(spark=spark,
                                        dataframe=df_table,
                                        project_name=project_name,
                                        table_name=table_name,
                                        s3_bucket=s3_bucket,
                                        file_type=file_type,
                                        separator=separator,
                                        header=header,
                                        infer_schema=infer_schema)
            if use_s3_dist_cp:
                # write dataframe into s3 through s3-dist-cp binary
                send_s3_using_s3_dist_cp(save_dir=save_dir,
                                         project_name=project_name,
                                         table_name=table_name,
                                         s3_bucket=s3_bucket,
                                         file_type=file_type)

        # delete temporary hdfs files
        delete_hdfs_file(remove_from_local_hdfs, source_schema, table_name)

        logging.info("Extract process for table {} finished".format(table_name))
    # close spark context
    stop_spark_context(spark)

if __name__ == '__main__':
    args = docopt(__doc__, version='1')
    # configure log
    set_log(args['--log-level'])

    # .get('default').get('metadata').get('test')
    config_file_path = args['--config-file']
    env = os.getenv('env_type', 'default')
    config = get_config(config_file_path).get(env)

    run_job(args, config)
