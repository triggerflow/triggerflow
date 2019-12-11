import yaml
import os


def load_config_yaml(path):
    path = os.path.expanduser(path)
    with open(path, 'r') as config_file:
        config = yaml.safe_load(config_file)
    return config
