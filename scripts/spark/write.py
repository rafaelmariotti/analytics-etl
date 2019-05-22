import logging
import sys
from pyspark.sql.types import DoubleType, DecimalType


# replace decimal fields to double type to prevent crash on redshift external tables
def _spark_replace_decimal_fields(dataframe):
    # list all fields and types from dataframe
    df_columns = {}
    for field in dataframe.schema.fields:
        df_columns.update({field.name: field.dataType})

    new_dataframe = dataframe
    decimal_types = (DecimalType(38, 10), DecimalType(38, 0))

    for column, data_type in df_columns.items():
        # decimal type is transformed to double type
        if isinstance(data_type, DecimalType) and data_type not in decimal_types:
            new_dataframe = new_dataframe.withColumn(column, new_dataframe[column].cast(DoubleType()))

    return new_dataframe


# write a dataframe into hdfs
def write_dataframe_into_file_system(**kwargs):
    spark = kwargs.get("spark")
    dataframe = kwargs.get("dataframe")
    table_name = kwargs.get("table_name")
    file_type = kwargs.get("file_type")
    separator = kwargs.get("separator")
    header = kwargs.get("header")
    infer_schema = kwargs.get("infer_schema")
    partition = kwargs.get("partition")
    save_dir = kwargs.get("save_dir")

    try:
        logging.info("writing {}.{} file into {}".format(table_name,
                                                         file_type,
                                                         save_dir))

        # set job description on spark GUI
        spark.sparkContext.setLocalProperty("callSite.short", "dataframe.write.{}.{}".format(table_name,
                                                                                             file_type))
        spark.sparkContext.setLocalProperty("callSite.long", "writing table {}.{} into {}".format(table_name,
                                                                                                  file_type,
                                                                                                  save_dir))

        file_path = "{}/{}.{}".format(save_dir,
                                      table_name,
                                      file_type)

        df_double_fields = _spark_replace_decimal_fields(dataframe)
        df_double_fields.write.save(file_path,
                                    format=file_type,
                                    sep=separator,
                                    header=header,
                                    inferSchema=infer_schema,
                                    partitionBy=partition,
                                    mode="overwrite",
                                    maxRecordsPerFile=1048576)

        df_double_fields.unpersist()
    except Exception as error:
        logging.error(error)
        sys.exit(-1)


# write files from hdfs to s3 bucket using spark
def write_dataframe_into_s3(**kwargs):
    spark = kwargs.get("spark")
    dataframe = kwargs.get("dataframe")
    project_name = kwargs.get("project_name")
    table_name = kwargs.get("table_name")
    s3_bucket = kwargs.get("s3_bucket")
    file_type = kwargs.get("file_type")
    separator = kwargs.get("separator")
    header = kwargs.get("header")
    infer_schema = kwargs.get("infer_schema")
    partition = kwargs.get("partition")

    try:
        logging.info("writing file {}.{} into s3 bucket using spark".format(table_name, file_type))

        # set job description on spark GUI
        spark.sparkContext.setLocalProperty("callSite.short", "dataframe.write.s3.{}.{}".format(project_name,
                                                                                                table_name))
        spark.sparkContext. \
            setLocalProperty("callSite.long", "writing file {}.{} into {}/{}".format(table_name,
                                                                                     file_type,
                                                                                     s3_bucket,
                                                                                     project_name,))

        s3_file_path = "{}/{}/{}.{}".format(s3_bucket,
                                            project_name,
                                            table_name,
                                            file_type)

        dataframe.write.save(s3_file_path,
                             format=file_type,
                             sep=separator,
                             header=header,
                             inferSchema=infer_schema,
                             partitionBy=partition,
                             mode="overwrite",
                             maxRecordsPerFile=1048576)

        dataframe.unpersist()
    except Exception as error:
        logging.error(error)
        sys.exit(-1)


# write a spark dataframe into a database
def write_dataframe_into_database(**kwargs):
    jdbc_url = kwargs.get("jdbc_url")
    spark = kwargs.get("spark")
    dataframe = kwargs.get("dataframe")
    schema = kwargs.get("schema")
    table_name = kwargs.get("table_name")
    fetch_size = kwargs.get("fetch_size")
    unpersist_after_write = kwargs.get("unpersist_after_write")

    try:
        logging.info("writing {}.{} into database {}".format(schema,
                                                             table_name,
                                                             jdbc_url))
        # set job description on spark GUI
        spark.sparkContext.setLocalProperty("callSite.short", "dataframe.write.{}.{}".format(schema,
                                                                                             table_name))
        spark.sparkContext.setLocalProperty("callSite.long", "writing {}.{} into {}".format(schema,
                                                                                            table_name,
                                                                                            jdbc_url))

        table_name = "{}.{}".format(schema, table_name)

        dataframe. \
            write. \
            format("jdbc")\
            .options(url=jdbc_url,
                     dbtable=table_name,
                     batchsize=fetch_size)\
            .mode("overwrite")

        if unpersist_after_write:
            dataframe.unpersist()
    except Exception as error:
        logging.error(error)
        sys.exit(-1)
