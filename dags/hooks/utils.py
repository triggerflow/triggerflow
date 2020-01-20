import os
import yaml

CONFIG_FILE = 'dags_config.yaml'
CONFIG_FILE_PATH = os.path.expanduser(os.path.join('~', CONFIG_FILE))


def load_conf(key: str = None):
    with open(CONFIG_FILE_PATH, 'r') as config_file:
        if key is None:
            cfg = yaml.safe_load(config_file)
        else:
            cfg = yaml.safe_load(config_file)[key]
    return cfg
