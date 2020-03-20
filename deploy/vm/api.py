import os
import re
import yaml
import signal
import logging
from uuid import uuid4
import requests as req
from requests.auth import HTTPBasicAuth
from flask import Flask, jsonify, request
from gevent.pywsgi import WSGIServer

from triggerflow.service.databases import RedisDatabase

app = Flask(__name__)
app.debug = False

private_credentials = None
db = None


def authenticate_request(db, request):
    if not request.authorization or \
       'username' not in request.authorization \
       or 'password' not in request.authorization:
        return False

    passwd = db.get_auth(username=request.authorization['username'])
    return passwd and passwd == request.authorization['password']


@app.before_request
def before_request_func():
    if not authenticate_request(db, request):
        return jsonify('Unauthorized'), 401


#
# Workspaces
#

@app.route('/workspace/<workspace>', methods=['PUT'])
def put_workspace(workspace):

    if not db.workspace_exists(workspace=workspace):
        params = request.get_json()

        if not re.fullmatch(r"^[a-zA-Z0-9.-]*$", workspace):
            return {"statusCode": 400, "body": {"error": "Illegal workspace name".format(workspace)}}

        if 'event_source' in params:
            name = params['event_source']['name']
            event_sources = {name: params['event_source']}
        else:
            event_sources = {}

        if 'global_context' in params:
            global_context = params['global_context']
        else:
            global_context = {}
        global_context['event_sources'] = event_sources

        db.create_workspace(workspace)

    user = request.authorization['username']
    password = request.authorization['password']
    auth = HTTPBasicAuth(username=user, password=password)

    resp = req.post('/'.join([private_credentials['triggerflow_service']['endpoint'],
                             'workspace', workspace]), auth=auth, json={})
    return (resp.text, resp.status_code, resp.headers.items())


@app.route('/workspace/<workspace>', methods=['GET'])
def get_workspace(workspace):
    if not db.workspace_exists(workspace=workspace):
        return jsonify({"error": "Workspace {} does not exists".format(workspace)}), 404

    return jsonify({"error": "Not Implemented".format(workspace)}), 501


@app.route('/workspace/<workspace>', methods=['DELETE'])
def delete_workspace(workspace):
    if not db.workspace_exists(workspace=workspace):
        return jsonify({"error": "Workspace {} does not exists".format(workspace)}), 404
    else:
        db.delete_workspace(workspace)

        user = request.authorization['username']
        password = request.authorization['password']
        auth = HTTPBasicAuth(username=user, password=password)

        resp = req.delete('/'.join([private_credentials['triggerflow_service']['endpoint'],
                                   'workspace', workspace]), auth=auth, json={})
        return (resp.text, resp.status_code, resp.headers.items())


#
# Event Sources
#

@app.route('/workspace/<workspace>/eventsource', methods=['GET'])
def list_eventsources(workspace):
    if not db.workspace_exists(workspace=workspace):
        return jsonify({"error": "Workspace {} does not exists".format(workspace)}), 404

    event_sources = db.get(workspace=workspace, document_id='event_sources')

    return jsonify({"event_sources": list(event_sources.keys())}), 200


@app.route('/workspace/<workspace>/eventsource/<eventsource>', methods=['PUT'])
def add_eventsource(workspace, eventsource):
    if not db.workspace_exists(workspace=workspace):
        return jsonify({"error": "Workspace {} does not exists".format(workspace)}), 404

    event_sources = db.get(workspace=workspace, document_id='event_sources')
    params = request.get_json()

    if eventsource not in event_sources:
        event_sources[eventsource] = params['eventsource']
        db.put(workspace=workspace, document_id='event_sources', data=event_sources)
        res = jsonify({"created": eventsource}), 201
    elif eventsource in event_sources and params['overwrite']:
        event_sources[eventsource] = params['eventsource'].copy()
        res = jsonify({"updated": eventsource}), 201
        db.put(workspace=workspace, document_id='event_sources', data=event_sources)
    else:
        res = jsonify({"error": "{} already exists".format(eventsource)}), 409

    return res


@app.route('/workspace/<workspace>/eventsource/<eventsource>', methods=['GET'])
def get_eventsource(workspace, eventsource):
    if not db.workspace_exists(workspace=workspace):
        return jsonify({"error": "Workspace {} does not exists".format(workspace)}), 404

    event_source = db.get_key(workspace=workspace, document_id='event_sources', key=eventsource)

    if not event_source:
        return jsonify({"error": "Eventsource {} does not exist".format(eventsource)}), 404
    else:
        return jsonify({eventsource: event_source}), 200


@app.route('/workspace/<workspace>/eventsource/<eventsource>', methods=['DELETE'])
def delete_eventsource(workspace, eventsource):
    if not db.workspace_exists(workspace=workspace):
        return jsonify({"error": "Workspace {} does not exists".format(workspace)}), 404

    event_source = db.get_key(workspace=workspace, document_id='event_sources', key=eventsource)

    if not event_source:
        return jsonify({"error": "Eventsource {} does not exist".format(eventsource)}), 404
    else:
        db.delete_key(workspace=workspace, document_id='event_sources', key=eventsource)
        return jsonify({"deleted": eventsource}), 200


#
# Triggers
#

@app.route('/workspace/<workspace>/trigger', methods=['POST'])
def add_trigger(workspace):
    if not db.workspace_exists(workspace=workspace):
        return jsonify({"error": "Workspace {} does not exists".format(workspace)}), 404

    params = request.get_json()
    triggers = params['triggers']
    committed_triggers = {}
    failed_trigger_commit = {}

    if not triggers:
        return jsonify({"error": "Trigger list is empty"}), 400

    elif len(triggers) == 1:  # Commit a single trigger

        trigger = triggers.pop()

        if trigger['trigger_id']:  # Named trigger, check if it already exists
            if db.key_exists(workspace=workspace, document_id='triggers', key=trigger['trigger_id']):
                return jsonify({"error":  "Trigger {} already exists".format(trigger['trigger_id'])}), 409

        elif not trigger['trigger_id'] and trigger['transient']:  # Unnamed trigger, check if it is transient
            trigger['trigger_id'] = str(uuid4())

        else:  # Unnamed non-transient trigger: illegal
            return jsonify({"error": "Non-transient unnamed trigger".format(trigger['trigger_id'])}), 400

        db.set_key(workspace=workspace, document_id='triggers', key=trigger['trigger_id'], value=trigger)
        committed_triggers = trigger['trigger_id']

    else:  # Commit multiple triggers

        db_triggers = db.get(workspace=workspace, document_id='triggers')

        for i, trigger in enumerate(triggers):
            if trigger['trigger_id']:  # Named trigger, check if it already exists
                if trigger['trigger_id'] in db_triggers:
                    failed_trigger_commit[i] = 'Trigger {} already exists'.format(trigger['trigger_id'])
            elif not trigger['trigger_id'] and trigger['transient']:  # Unnamed trigger, check if it is transient
                trigger['trigger_id'] = str(uuid4())
            else:  # Unnamed non-transient trigger: illegal
                failed_trigger_commit[i] = 'Non-transient unnamed trigger'
            db_triggers[trigger['trigger_id']] = trigger
            committed_triggers[i] = trigger['trigger_id']

        db.put(workspace=workspace, document_id='triggers', data=db_triggers)

    return jsonify({"triggers": committed_triggers}), 201


@app.route('/workspace/<workspace>/trigger/<trigger>', methods=['GET'])
def get_trigger(workspace, trigger):
    if not db.workspace_exists(workspace=workspace):
        return jsonify({"error": "Workspace {} does not exists".format(workspace)}), 404

    return jsonify({"error": "Not Implemented".format(workspace)}), 501


@app.route('/workspace/<workspace>/trigger/<trigger>', methods=['DELETE'])
def delete_trigger(workspace, trigger):
    if not db.workspace_exists(workspace=workspace):
        return jsonify({"error": "Workspace {} does not exists".format(workspace)}), 404

    return jsonify({"error": "Not Implemented".format(workspace)}), 501


def main():
    global private_credentials, db

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

    logging.info('Loading private credentials')
    with open('config.yaml', 'r') as config_file:
        private_credentials = yaml.safe_load(config_file)

    logging.info('Creating database client')
    db = RedisDatabase(**private_credentials['redis'])

    port = int(os.getenv('PORT', 8080))
    server = WSGIServer(('', port), app, log=logging.getLogger())
    logging.info('Triggerflow API started on port {}'.format(port))

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print('exiting...')


if __name__ == "__main__":
    main()
