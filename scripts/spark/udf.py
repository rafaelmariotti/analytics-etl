# generate a sequence of dates according to an interval (years, months or days) and his value
def generate_series(timestamp_min, timestamp_max, interval="months", value="1"):
    from dateutil.relativedelta import relativedelta
    import logging
    import sys

    try:
        timestamp_list = []
        while timestamp_min <= timestamp_max:
            timestamp_list.append(timestamp_min)
            timestamp_min = eval("timestamp_min + relativedelta({}={})".format(interval, str(value)))

        return timestamp_list
    except Exception as error:
        logging.error(error)
        sys.exit(-1)


# load all previous defined udfs using a given spark context
def load_udf(spark):
    from pyspark.sql.types import TimestampType
    from pyspark.sql.types import ArrayType

    spark.udf.register("generate_series", generate_series, ArrayType(TimestampType()))
