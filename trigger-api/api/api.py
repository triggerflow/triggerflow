import os
import yaml
import logging

import requests as req
from requests.auth import HTTPBasicAuth
from flask import Flask, jsonify, request
from gevent.pywsgi import WSGIServer

import workspaces
import triggers
import eventsources as event_sources
import triggerstorage

CONFIG_MAP_PATH = os.path.realpath(os.path.join(os.getcwd(), 'config_map.yaml'))

api = Flask(__name__)
api.debug = False

config_map = None
trigger_storage = None


@api.before_request
def authenticate_request():

    if request.authorization.type != 'basic' \
            or request.authorization.username is None \
            or request.authorization.password is None:
        logging.warn('Unauthorized request attempt from {}'.format(request.remote_addr))
        return jsonify({'error': 'Unauthorized'}), 401

    x = request

    password = trigger_storage.get_auth(username=request.authorization.username)
    if not password or password != request.authorization.password:
        logging.warn('Unauthorized request attempt from {}'.format(request.remote_addr))
        return jsonify({'error': 'Unauthorized'}), 401

    logging.debug('Authorization for {} succeeded'.format(request.remote_addr))


#
# Workspaces
#

@api.route('/workspace', methods=['POST'])
def create_workspace():
    global trigger_storage
    parameters = request.get_json(force=True, silent=True)
    if not parameters:
        return jsonify({'error': 'Error parsing request parameters', 'err_code': 0}), 400

    if {'workspace_name', 'event_source', 'global_context'} != set(parameters) \
            or not isinstance(parameters['workspace_name'], str) \
            or not isinstance(parameters['event_source'], dict) \
            or not isinstance(parameters['global_context'], dict):
        return jsonify({'error': 'Invalid parameters', 'err_code': 1}), 400

    res, code = workspaces.create_workspace(trigger_storage, parameters['workspace_name'],
                                            parameters['global_context'], parameters['event_source'])
    if code != 200:
        return jsonify(res), code

    auth = HTTPBasicAuth(username='token', password=config_map['triggerflow_controller']['token'])
    url = '/'.join([config_map['triggerflow_controller']['endpoint'], 'workspace', parameters['workspace_name']])
    try:
        controller_res = req.post(url, auth=auth, json={})
    except req.exceptions.RequestException as e:
        logging.warning('Triggerflow Controller Service is unavailable: {}'.format(e))
        return jsonify(res), 202

    if controller_res.ok:
        return jsonify(res), 201
    else:
        return jsonify({'error': 'Triggerflow Controller service is unavailable'}), 202


@api.route('/workspace/<workspace>', methods=['GET'])
def get_workspace(workspace):
    global trigger_storage
    if not trigger_storage.workspace_exists(workspace=workspace):
        return jsonify({'error': 'Workspace {} not found'.format(workspace)}), 404

    res, code = workspaces.get_workspace(trigger_storage, workspace)
    return jsonify(res), code


@api.route('/workspace/<workspace>', methods=['DELETE'])
def delete_workspace(workspace):
    global trigger_storage
    if not trigger_storage.workspace_exists(workspace=workspace):
        return jsonify({'error': 'Workspace {} not found'.format(workspace)}), 404

    res, code = workspaces.delete_workspace(trigger_storage, workspace)

    auth = HTTPBasicAuth(username='token', password=config_map['triggerflow_controller']['token'])
    controller_res = req.post('/'.join([config_map['triggerflow_controller']['endpoint'],
                                        'workspace', workspace]), auth=auth, json={})
    if controller_res.ok:
        return jsonify(res), 200
    else:
        return {'error': 'Workspace deleted, Triggerflow service unavailable'}, 500


#
# Event Sources
#

@api.route('/workspace/<workspace>/eventsource', methods=['POST'])
def add_eventsource(workspace):
    global trigger_storage
    if not trigger_storage.workspace_exists(workspace=workspace):
        return jsonify({'error': 'Workspace {} not found'.format(workspace)}), 404

    parameters = request.get_json(force=True, silent=True)
    if not parameters or 'event_source' not in parameters or not isinstance(parameters['event_source'], dict):
        return jsonify({'error': 'Invalid parameters'}), 400

    if 'overwrite' in request.args['overwrite'] and isinstance(request.args['overwrite'], bool):
        overwrite = request.args['overwrite']
    else:
        overwrite = False

    res, code = event_sources.add_event_source(trigger_storage, workspace, parameters['event_source'], overwrite)

    return jsonify(res), code


@api.route('/workspace/<workspace>/eventsource', methods=['GET'])
def list_event_sources(workspace):
    global trigger_storage
    if not trigger_storage.workspace_exists(workspace=workspace):
        return jsonify({'error': 'Workspace {} not found'.format(workspace)}), 404

    res, code = event_sources.list_event_sources(trigger_storage, workspace)

    return jsonify(res), code


@api.route('/workspace/<string:workspace>/eventsource/<string:eventsource_name>', methods=['GET'])
def get_eventsource(workspace, eventsource_name):
    global trigger_storage
    if not trigger_storage.workspace_exists(workspace=workspace):
        return jsonify({'error': 'Workspace {} not found'.format(workspace)}), 404

    res, code = event_sources.get_event_source(trigger_storage, workspace, eventsource_name)

    return jsonify(res), code


@api.route('/workspace/<workspace>/eventsource/<eventsource_name>', methods=['DELETE'])
def delete_eventsource(workspace, eventsource_name):
    global trigger_storage
    if not trigger_storage.workspace_exists(workspace=workspace):
        return jsonify({'error': 'Workspace {} not found'.format(workspace)}), 404

    res, code = event_sources.delete_event_source(trigger_storage, workspace, eventsource_name)

    return jsonify(res), code


#
# Triggers
#

@api.route('/workspace/<workspace>/trigger', methods=['POST'])
def add_trigger(workspace):
    global trigger_storage
    if not trigger_storage.workspace_exists(workspace=workspace):
        return jsonify({'error': 'Workspace {} not found'.format(workspace)}), 404

    parameters = request.get_json(force=True, silent=True)
    if not parameters \
            or 'triggers' not in parameters \
            or not isinstance(parameters['triggers'], list) \
            or not all([isinstance(trigger, dict) for trigger in parameters['triggers']]):
        return jsonify({'error': 'Invalid parameters'}), 400

    res, code = triggers.add_triggers(trigger_storage, workspace, parameters['triggers'])

    return jsonify(res), code


@api.route('/workspace/<string:workspace>/trigger/<string:trigger_id>', methods=['GET'])
def get_trigger(workspace, trigger_id):
    global trigger_storage
    if not trigger_storage.workspace_exists(workspace=workspace):
        return jsonify({'error': 'Workspace {} not found'.format(workspace)}), 404

    res, code = triggers.get_trigger(trigger_storage, workspace, trigger_id)

    return jsonify(res), code


@api.route('/workspace/<string:workspace>/trigger', methods=['GET'])
def list_triggers(workspace):
    global trigger_storage
    if not trigger_storage.workspace_exists(workspace=workspace):
        return jsonify({'error': 'Workspace {} not found'.format(workspace)}), 404

    res, code = triggers.list_triggers(trigger_storage, workspace)

    return jsonify(res), code


@api.route('/workspace/<string:workspace>/trigger/<string:trigger_id>', methods=['DELETE'])
def delete_trigger(workspace, trigger_id):
    global trigger_storage
    if not trigger_storage.workspace_exists(workspace=workspace):
        return jsonify({'error': 'Workspace {} not found'.format(workspace)}), 404

    res, code = triggers.delete_trigger(trigger_storage, workspace, trigger_id)

    return jsonify(res), code


##########################


def main():
    global config_map, trigger_storage

    logger = logging.getLogger()
    logger.setLevel(logging.NOTSET)

    component = os.getenv('INSTANCE', 'triggerflow-api')

    # Make sure we log to the console
    stream_handler = logging.StreamHandler()
    formatter = logging.Formatter('[%(asctime)s.%(msecs)03dZ][%(levelname)s][triggerflow] %(message)s',
                                  datefmt="%Y-%m-%dT%H:%M:%S")
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    logging.info('Starting Triggerflow API')

    # also log to file if /logs is present
    if os.path.isdir('/logs'):
        fh = logging.FileHandler('/logs/{}_logs.log'.format(component))
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    logging.info('Loading config map')
    with open(CONFIG_MAP_PATH, 'r') as config_file:
        config_map = yaml.safe_load(config_file)

    # Instantiate trigger storage client
    logging.info('Creating trigger storage client')
    backend = config_map['trigger_storage']['backend']
    trigger_storage_class = getattr(triggerstorage, backend.capitalize() + 'TriggerStorage')
    trigger_storage = trigger_storage_class(**config_map['trigger_storage']['parameters'])

    # Check config parameters
    if 'triggerflow_controller' not in config_map \
            or {'endpoint', 'token'} != (set(config_map['triggerflow_controller'])):
        raise KeyError('Missing triggerflow controller parameters in config map')

    if 'http://' not in config_map['triggerflow_controller']['endpoint']:
        config_map['triggerflow_controller']['endpoint'] = 'http://' + config_map['triggerflow_controller']['endpoint']

    port = int(os.getenv('PORT', 8080))
    server = WSGIServer(('', port), api, log=logging.getLogger())
    logging.info('Triggerflow API started on port {}'.format(port))

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print('exiting...')


if __name__ == "__main__":
    main()
