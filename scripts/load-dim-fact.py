#!/usr/bin/env python

"""
Create Facts and Dimensions according to a given script.


Usage:
    load-dim-fact.py [options]
    load-dim-fact.py --version
    load-dim-fact.py (-h | --help)


Options:
    -p PROJECT-NAME, --project=PROJECT-NAME                 Project name to execute
    -c COUNTRY, --country=country                           Country from project
    --dependent-schema-dir=DEPENDENT-SCHEMA-DIR             Directory where dependent stage schemas are located [default: hdfs:///user/hadoop/stage]
    --save-hdfs=SAVE-HDFS                                   Flag to either save contents to hdfs [default: True]
    --remove-from-local-hdfs=FLAG-REMOVE-FROM-LOCAL-HDFS    Flag to either remove or not all the local hdfs files after process [default: False]
    --base-directory=DIRECTORY                              Base directory location to save files [default: hdfs:///user/hadoop/datamart]
    -H TARGET-DATABASE, --target-hostname=TARGET-DATABASE   Target database hostname
    -p TARGET-PORT, --target-port=TARGET-PORT               Target database port
    -u TARGET-USER, --target-username=TARGET-USER           Target database user
    -p TARGET-PASSWORD, --target-password=TARGET-PASSWORD   Target database password
    -d TARGET-DATABASE, --target-database=TARGET-DATABASE   Target database name/service name
    -s TARGET-SCHEMA, --target-schema=TARGET-SCHEMA         Target database schema
    -t TARGET-TYPE, --target-type=TARGET-TYPE               Target database type
    -f, --write-fetch-size=WRITE-FETCH-SIZE                 Write fetch size from target database [default: 20000]
    -l LOG-LEVEL, --log-level=LOG-LEVEL                     Script log level information [default: INFO]
    -a AWS-ACCESS-KEY, --aws-access-key=AWS-ACCESS-KEY      AWS Access Key to write on S3 bucket
    -k AWS-SECRET-KEY, --aws-secret-key=AWS-SECRET-KEY      AWS Access Key to write on S3 bucket
    -b S3-BUCKET-NAME, --s3-bucket-name=S3-BUCKET-NAME      S3 bucket name to write source tables
    --use-s3-dist-cp=USE-S3-DIST-CP                         Flag to either save contents to s3 using AWS wrapped hadoop s3distcp [default: True]
    --input-file-type=INPUT-FILE-TYPE                       Input file type from stage files (current suports only csv and parquet) [default: parquet]
    --output-file-type=OUTPUT-FILE-TYPE                     Output file type (current suports only csv and parquet) [default: parquet]
    --csv-separator=CSV-SEPARATOR                           String to separate all fields from CSV file [default: ,]
    --csv-header=HEADER                                     Flag to either write or not the CSV header to file output [default: False]
    --infer-schema=INFER-SCHEMA                             Flag to either infer or not the dataframe schema on Spark [default: False]

"""

from docopt import docopt
import logging
from logger.utils import set_log
import os
from s3.utils import validate_s3_interface, send_s3_using_s3_dist_cp
from config.utils import get_config
from database.utils import get_table_info
from spark.utils import create_spark_session, delete_hdfs_file, create_database_spark_url, check_write_dataframe_database, stop_spark_context
from spark.read import load_dependent_table, load_dimension_fact, load_spark_sql_udf
from spark.write import write_dataframe_into_file_system, write_dataframe_into_s3, write_dataframe_into_database


def _process_dim_fact(**kwargs):
    spark = kwargs.get("spark")
    project_name = kwargs.get("project_name")
    project_schema = kwargs.get("project_schema")
    country = kwargs.get("country")
    dim_fact_name = kwargs.get("dim_fact_name")
    dim_fact_file = kwargs.get("dim_fact_file")
    dim_fact_partition = kwargs.get("dim_fact_partition")
    output_file_type = kwargs.get("output_file_type")
    separator = kwargs.get("separator")
    header = kwargs.get("header")
    use_s3_dist_cp = kwargs.get("use_s3_dist_cp")
    s3_bucket = kwargs.get("s3_bucket")
    save_hdfs = kwargs.get("save_hdfs")
    remove_from_local_hdfs = kwargs.get("remove_from_local_hdfs")
    save_dir = kwargs.get("save_dir")
    database_config = kwargs.get("database_config")
    unpersist_after_write = kwargs.get("unpersist_after_write")
    infer_schema = kwargs.get("infer_schema")
    fetch_size = kwargs.get("fetch_size")

    # load spark sql script, repartitioning dataset to a single partition
    df_dim_fact = load_dimension_fact(spark=spark,
                                      project_name=project_name,
                                      country=country,
                                      dim_fact_name=dim_fact_name,
                                      dim_fact_file=dim_fact_file,
                                      unpersist_after_write=unpersist_after_write) \
        .coalesce(1)

    # write dataframe to hdfs if needed
    if save_hdfs:
        write_dataframe_into_file_system(spark=spark,
                                         dataframe=df_dim_fact,
                                         table_name=dim_fact_name,
                                         file_type=output_file_type,
                                         separator=separator,
                                         header=header,
                                         infer_schema=infer_schema,
                                         partition=dim_fact_partition,
                                         save_dir=save_dir)

    # write to database if needed
    if check_write_dataframe_database(database_config):
        # generate jdbc_url for spark
        jdbc_url = create_database_spark_url(database_type=database_config.get("database_type").lower(),
                                             hostname=database_config.get("hostname").lower(),
                                             port=database_config.get("port"),
                                             database=database_config.get("database").lower(),
                                             username=database_config.get("username").lower(),
                                             password=database_config.get("password"))
        # write dataframe into database
        write_dataframe_into_database(jdbc_url=jdbc_url,
                                      spark=spark,
                                      dataframe=df_dim_fact,
                                      schema=project_schema,
                                      table_name=dim_fact_name,
                                      fetch_size=fetch_size,
                                      unpersist_after_write=unpersist_after_write)

    # send file to s3
    if s3_bucket is not None:
        if not use_s3_dist_cp:
            # write dataframe into s3 through spark
            write_dataframe_into_s3(spark=spark,
                                    dataframe=df_dim_fact,
                                    project_name=project_name,
                                    table_name=dim_fact_name,
                                    s3_bucket=s3_bucket,
                                    file_type=output_file_type,
                                    separator=separator,
                                    header=header,
                                    infer_schema=infer_schema,
                                    partition=dim_fact_partition)
        if use_s3_dist_cp:
            # write dataframe into s3 through s3-dist-cp binary
            send_s3_using_s3_dist_cp(save_dir=save_dir,
                                     project_name=project_schema,
                                     table_name=dim_fact_name,
                                     s3_bucket=s3_bucket,
                                     file_type=output_file_type)

    # delete dimensions from hdfs if needed
    delete_hdfs_file(remove_from_local_hdfs,
                     project_schema,
                     dim_fact_name)


# main function to run
def run_job(args, config):
    app_name_template = config.get('global').get('description')
    project_name = args["--project"]
    country = args["--country"]

    # read spark/hadoop informations
    save_hdfs = (args["--save-hdfs"].lower() == "true")
    remove_from_local_hdfs = (args["--remove-from-local-hdfs"].lower() == "true")

    # file system saving info
    dependent_schema_dir = args["--dependent-schema-dir"]
    base_directory = args["--base-directory"]
    project_schema = "dw_{}_{}".format(project_name, country)
    save_dir = "{}/{}".format(base_directory, project_schema)

    target_database_config = {"type": args["--target-type"],
                              "hostname": args["--target-hostname"],
                              "port": args["--target-port"],
                              "username": args["--target-username"],
                              "password": args["--target-password"],
                              "database": args["--target-database"],
                              "schema": args["--target-schema"]}

    # write s3 bucket informations
    use_s3_dist_cp = (args["--use-s3-dist-cp"].lower() == "true")
    aws_access_key = args["--aws-access-key"]
    aws_secret_key = args["--aws-secret-key"]
    s3_bucket = args["--s3-bucket-name"]

    # file related args
    input_file_type = args["--input-file-type"]
    output_file_type = args["--output-file-type"]
    separator = args["--csv-separator"]
    header = (args["--csv-header"].lower() == "true")
    infer_schema = (args["--infer-schema"].lower() == "true")

    # validate if s3 interface is valid with given option
    validate_s3_interface(use_s3_dist_cp, s3_bucket)

    metadata_config = config.get('metadata')
    project_config_file = '../config/dimensions-facts/{}/{}.yml'.format(project_name,
                                                                        country)
    project_config = get_config(project_config_file).get('default')

    app_name = app_name_template.format(project_name,
                                        country,
                                        output_file_type)
    spark = create_spark_session(aws_access_key,
                                 aws_secret_key,
                                 app_name)

    dimensions_config = project_config.get("dimensions")
    facts_config = project_config.get("facts")

    dependent_schemas = project_config.get("dependency")

    # load all tables (stages) from all dependent schemas to calculate dimensions and facts
    for schema in dependent_schemas:
        logging.info("loading tables from {} schema".format(schema))
        schema_name = dependent_schemas.get(schema)
        stage_tables_info = get_table_info(metadata_config, schema_name)

        # for each stage table found, load a temporary view for spark
        for stage_table in stage_tables_info:
            table_name = stage_table[0]
            load_dependent_table(spark,
                                 dependent_schema_dir,
                                 schema_name,
                                 table_name,
                                 input_file_type)

    # load user defined functions (udf) to use as spark sql functions
    load_spark_sql_udf(spark)

    # for each dimension
    for dimension in dimensions_config:
        # unpersist dataframe from cache after write
        unpersist_after_write = True
        # get sql file
        dim_file = dimensions_config.get(dimension).get("file")
        # get partition column list
        dim_partition = dimensions_config.get(dimension).get("partition")
        if dim_partition is not None:
            dim_partition = dim_partition.replace(" ", "").split(',')

        _process_dim_fact(spark=spark,
                          project_name=project_name,
                          project_schema=project_schema,
                          country=country,
                          dim_fact_name=dimension,
                          dim_fact_file=dim_file,
                          dim_fact_partition=dim_partition,
                          output_file_type=output_file_type,
                          separator=separator,
                          header=header,
                          use_s3_dist_cp=use_s3_dist_cp,
                          s3_bucket=s3_bucket,
                          save_hdfs=save_hdfs,
                          remove_from_local_hdfs=remove_from_local_hdfs,
                          save_dir=save_dir,
                          database_config=target_database_config,
                          unpersist_after_write=unpersist_after_write,
                          infer_schema=infer_schema)
        logging.info("Dimension process {} finished".format(dimension))

    # for each fact
    for fact in facts_config:
        # unpersist dataframe from cache after write
        unpersist_after_write = True
        # get sql file
        fact_file = facts_config.get(fact).get("file")
        # get partition column list
        fact_partition = facts_config.get(fact).get("partition")
        if fact_partition is not None:
            fact_partition = fact_partition.replace(" ", "").split(',')

        _process_dim_fact(spark=spark,
                          project_name=project_name,
                          project_schema=project_schema,
                          country=country,
                          dim_fact_name=fact,
                          dim_fact_file=fact_file,
                          dim_fact_partition=fact_partition,
                          output_file_type=output_file_type,
                          separator=separator,
                          header=header,
                          use_s3_dist_cp=use_s3_dist_cp,
                          s3_bucket=s3_bucket,
                          save_hdfs=save_hdfs,
                          remove_from_local_hdfs=remove_from_local_hdfs,
                          save_dir=save_dir,
                          database_config=target_database_config,
                          unpersist_after_write=unpersist_after_write,
                          infer_schema=infer_schema)
        logging.info("Fact process {} finished".format(fact))

    # spark is not needed
    stop_spark_context(spark)


if __name__ == '__main__':
    args = docopt(__doc__, version='1')
    # configure log
    set_log(args['--log-level'])

    env = os.getenv('env_type', 'default')
    config = get_config('../config/load-dim-fact.yml').get(env)
    run_job(args, config)
