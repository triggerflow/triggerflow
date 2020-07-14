import os
import yaml

try:
    config_map = {'triggerflow_controller': {'endpoint': os.environ['TRIGGERFLOW_CONTROLLER_ENDPOINT'],
                                             'token': os.environ.get('TRIGGERFLOW_CONTROLLER_TOKEN', 'token')}}

    backend = os.environ['TRIGGERFLOW_STORAGE_BACKEND']

    if backend == 'redis':
        parameters = {'host': os.environ['REDIS_HOST'],
                      'password': os.environ.get('REDIS_PASSWORD', ''),
                      'port': os.environ.get('REDIS_PORT', 6379),
                      'db': os.environ.get('REDIS_DB', 0)}
        config_map['trigger_storage'] = {'backend': backend, 'parameters': parameters}
    else:
        raise Exception('Backend {} not supported'.format(backend))

    with open('config_map.yaml', 'w') as config_map_file:
        yaml.safe_dump(config_map, config_map_file)
except KeyError as e:
    raise KeyError('Missing configuration parameter ENV variable "{}"'.format(str(e)))
