import yaml
import os
from binascii import hexlify

FILE = os.path.expanduser('~/event-processor_client_config.yaml')


def load_config_yaml():
    with open(FILE, 'r') as config_file:
        config = yaml.safe_load(config_file)
    return config


def generate_apikey(length):
    return hexlify(os.urandom(length)).decode()


if __name__ == '__main__':
    print(generate_apikey(64))
