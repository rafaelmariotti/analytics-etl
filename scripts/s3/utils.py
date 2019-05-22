from os import devnull
import logging
import subprocess
import sys


# check if given s3 interface bucket is valid according to spark or hdfs send option
def validate_s3_interface(use_s3_dist_cp, s3_bucket):
    if s3_bucket is not None:
        try:
            # must have "s3" interface
            if use_s3_dist_cp and s3_bucket[0:3] != "s3:":
                raise Exception("wrong s3 interface for {}. Please use \"s3\" instead".format(s3_bucket))
            # must have "s3a" interface
            if not use_s3_dist_cp and s3_bucket[0:3] != "s3a":
                raise Exception("wrong s3 interface for {}. Please use \"s3a\" instead".format(s3_bucket))
        except Exception as error:
            logging.error(error)
            sys.exit(-1)


# remove file from s3 using aws cli (since the environment has direct access from IAM role)
def remove_old_file_from_s3(s3_bucket, project_name, table_name, file_type):
    bash_command = "aws"
    aws_product_name = "s3"
    aws_product_command = "rm"
    recursive_flag = "--recursive"
    s3_file_path = "{}/{}/{}.{}".format(s3_bucket, project_name, table_name, file_type)
    command = [bash_command,
               aws_product_name,
               aws_product_command,
               s3_file_path,
               recursive_flag]

    try:
        result = subprocess.call(command,
                                 stdout=open(devnull, "w"),
                                 stderr=subprocess.STDOUT)
        if result != 0:
            raise Exception("could not remove file from s3 due error {} with command {}".format(str(result)),
                            command)
    except Exception as error:
        logging.error(error)


# write files from hdfs to s3 bucket using s3-dist-cp
def send_s3_using_s3_dist_cp(**kwargs):
    # call to remove file from s3 if it already exists
    save_dir = kwargs.get("save_dir")
    project_name = kwargs.get("project_name")
    table_name = kwargs.get("table_name")
    s3_bucket = kwargs.get("s3_bucket")
    file_type = kwargs.get("file_type")

    remove_old_file_from_s3(s3_bucket, project_name, table_name, file_type)

    # build the command
    bash_command = "s3-dist-cp"
    source = "--src={}/{}.{}".format(save_dir, table_name, file_type)
    destiny = "--dest={}/{}/{}.{}".format(s3_bucket, project_name, table_name, file_type)
    multipart = "--multipartUploadChunkSize=1048"
    # group_by = "--groupBy=.*/{}.parquet/(part-)00(\d).*(.parquet)".format(table_name)
    command = [bash_command,
               source,
               destiny,
               multipart]

    logging.info("writing file {}.{} into s3 bucket using s3-dist-cp".format(table_name, file_type))

    try:
        result = subprocess.call(command,
                                 stdout=open(devnull, "w"),
                                 stderr=subprocess.STDOUT)

        if result != 0:
            if result == 1:
                raise Exception("could not send to s3 [err 1]: file does not exists {}".format(command))
            else:
                raise Exception("could not send to s3 due error {} with command {}".format(str(result), command))
    except Exception as error:
        logging.error(error)
