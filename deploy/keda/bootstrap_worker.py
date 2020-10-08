import yaml
import os
import logging

from triggerflow.service.worker import Worker

CONFIG_MAP_PATH = 'config_map.yaml'

if __name__ == '__main__':
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    stream_handler = logging.StreamHandler()
    formatter = logging.Formatter('[%(asctime)s.%(msecs)03dZ][%(levelname)s][triggerflow] %(message)s',
                                  datefmt="%Y-%m-%dT%H:%M:%S")
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    with open(CONFIG_MAP_PATH, 'r') as config_map_file:
        config_map = yaml.safe_load(config_map_file)

    workspace = os.environ['TRIGGERFLOW_BOOTSTRAP_WORKSPACE']

    logging.info('Starting Triggerflow Worker for workspace {}'.format(workspace))
    worker = Worker(workspace=workspace, config=config_map)
    worker.start()
    worker.join()
