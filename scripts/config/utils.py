import sys
import yaml
import logging


# create a json with config.yml informations
def get_config(config_file_path):
    with open(config_file_path, 'r') as config_file:
        try:
            return yaml.load(config_file)
        except yaml.YAMLError as error:
            logging.error(error)
            sys.exit(-1)
