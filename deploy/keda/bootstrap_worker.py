import yaml
import os

from triggerflow.service.worker import Worker

CONFIG_MAP_PATH = 'config_map.yaml'

if __name__ == '__main__':
    with open(CONFIG_MAP_PATH, 'r') as config_map_file:
        config_map = yaml.safe_load(config_map_file)

    workspace = os.environ['WORKSPACE']

    worker = Worker(workspace=workspace, config=config_map)
    worker.start()
    worker.join()
