import sys
import yaml
import logging

from triggerflow.service.worker import Worker as TriggerflowWorker

WORKSPACE = 'sm_testt'
CONFIG_MAP_PATH = 'config_map.yaml'

stream_handler = logging.StreamHandler()
formatter = logging.Formatter('[%(asctime)s.%(msecs)03dZ][%(levelname)s][triggerflow] %(message)s',
                              datefmt="%Y-%m-%dT%H:%M:%S")

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)


def bootstrap_worker(workspace: str, config_map: dict):
    """
    Bootstrap a Triggerflow worker manually, without the intervention of the controller.
    Used for testing and development purposes.
    """
    worker = TriggerflowWorker(workspace=workspace, config=config_map)
    worker.run()


if __name__ == '__main__':
    if len(sys.argv) == 3:
        workspace, config_map_path = sys.argv[1:3]
    elif len(sys.argv) == 2:
        workspace, config_map_path = sys.argv[1], CONFIG_MAP_PATH
    else:
        workspace, config_map_path = WORKSPACE, CONFIG_MAP_PATH

    with open(config_map_path, 'r') as config_map_file:
        config_map = yaml.safe_load(config_map_file.read())

    bootstrap_worker(workspace, config_map)
