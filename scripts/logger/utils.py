import sys
import logging


# set log level
def set_log(log_level):
    log_level_number = getattr(logging, log_level.upper(), None)
    if not isinstance(log_level_number, int):
        print('ERROR: Invalid log level \'%s\'' % log_level.upper())
        sys.exit(-1)
    logging.basicConfig(level=log_level.upper(),
                        format='%(asctime)s %(levelname)-s [%(filename)s, %(funcName)s:%(lineno)d] %(message)s',
                        datefmt='%y/%m/%d %H:%M:%S',
                        stream=sys.stdout)
    logging.StreamHandler(sys.stdout)
