import logging
import sys
from .utils import read_sql_file


# read a table from a database and generates a spark dataframe
def read_from_database(**kwargs):
    spark = kwargs.get("spark")
    jdbc_url = kwargs.get("jdbc_url")
    schema = kwargs.get("schema")
    table_name = kwargs.get("table_name")
    partition_column = kwargs.get("partition_column")
    num_partitions = kwargs.get("num_partitions")
    lower = kwargs.get("lower")
    upper = kwargs.get("upper")
    read_fetch_size = kwargs.get("read_fetch_size")

    try:
        complete_table_name = "{}.{}".format(schema, table_name)

        # set job description on spark GUI
        spark.sparkContext.setLocalProperty("callSite.short", "dataframe.read.database.{}.{}".format(schema,
                                                                                                     table_name))
        spark.sparkContext.setLocalProperty("callSite.long", "reading table {}.{} from {}".format(schema,
                                                                                                  table_name,
                                                                                                  jdbc_url))

        oltp_table = spark.read.format("jdbc").options(url=jdbc_url,
                                                       dbtable=complete_table_name,
                                                       partitionColumn=partition_column,
                                                       numPartitions=num_partitions,
                                                       lowerBound=lower,
                                                       upperBound=upper,
                                                       fetchsize=int(read_fetch_size))
        df_oltp_table = oltp_table.load()

        return df_oltp_table
    except Exception as error:
        logging.error(error)
        sys.exit(-1)


# read file from hdfs and generates a spark dataframe
def read_from_file_system(**kwargs):
    spark = kwargs.get("spark")
    table_name = kwargs.get("table_name")
    file_type = kwargs.get("file_type")
    separator = kwargs.get("separator")
    header = kwargs.get("header")
    infer_schema = kwargs.get("infer_schema")
    save_dir = kwargs.get("save_dir")

    try:
        logging.info("reading file {}.{} from local hdfs".format(table_name, file_type))

        # set job description on spark GUI
        spark.sparkContext.setLocalProperty("callSite.short", "dataframe.read.file-system.{}.{}".format(table_name,
                                                                                                        file_type))
        spark.sparkContext.setLocalProperty("callSite.long", "reading file {}.{} from {}".format(table_name,
                                                                                                 file_type,
                                                                                                 save_dir))

        file_path = "{}/{}.{}".format(save_dir,
                                      table_name,
                                      file_type)
        hdfs_file = spark.read.load(file_path,
                                    format=file_type,
                                    sep=separator,
                                    header=header,
                                    inferSchema=infer_schema)

        return hdfs_file
    except Exception as error:
        logging.error(error)
        sys.exit(-1)


# load stage tables from a given schema, allowing all dimensions and facts to be calculated
def load_dependent_table(spark, dependent_schema_dir, schema, table_name, file_type):
        logging.info("loading table {} as {}_{}".format(table_name,
                                                        schema,
                                                        table_name))
        try:

            # set job description on spark GUI
            spark.sparkContext.setLocalProperty("callSite.short", "dataframe.load.file.{}.{}".format(schema,
                                                                                                     table_name))
            spark.sparkContext. \
                setLocalProperty("callSite.long", "loading dependent table {}.{} from {}".format(table_name,
                                                                                                 file_type,
                                                                                                 dependent_schema_dir))

            spark. \
                read. \
                parquet("{}/{}/{}.{}".format(dependent_schema_dir,
                                             schema,
                                             table_name,
                                             file_type)). \
                createOrReplaceTempView("{}_{}".format(schema,
                                                       table_name))

        except Exception as error:
            logging.error(error)
            sys.exit(-1)


# load all user defined functions in udf.py file to use with spark sql
def load_spark_sql_udf(spark):
    spark.sparkContext.addPyFile("spark/udf.py")
    import udf
    udf.load_udf(spark)


# load all queries from facts and dimensions to temporary views
def load_dimension_fact(**kwargs):
    spark = kwargs.get("spark")
    project_name = kwargs.get("project_name")
    country = kwargs.get("country")
    dim_fact_name = kwargs.get("dim_fact_name")
    dim_fact_file = kwargs.get("dim_fact_file")
    unpersist_after_write = kwargs.get("unpersist_after_write")

    sql = read_sql_file(project_name, country, dim_fact_file)

    try:
        logging.info("loading spark sql script {}".format(dim_fact_file))

        # set job description on spark GUI
        spark.sparkContext.setLocalProperty("callSite.short", "dataframe.load.dim-fact.{}.{}".format(project_name,
                                                                                                     dim_fact_name))
        spark.sparkContext. \
            setLocalProperty("callSite.long", "loading dimension/fact {}.{} from {}".format(project_name,
                                                                                            dim_fact_name,
                                                                                            dim_fact_file))

        df_dim_fact = spark.sql(sql)
        df_dim_fact.createOrReplaceTempView(dim_fact_name)
        # repartitioning dataframe to generate only one partition
        df_dim_fact.coalesce(1)

        # cache the dataframe
        if not unpersist_after_write:
            df_dim_fact.cache()

        return df_dim_fact

    except Exception as error:
        logging.error(error)
        sys.exit(-1)
