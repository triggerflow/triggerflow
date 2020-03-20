import logging
import os
import signal
import yaml
import queue
from flask import Flask, jsonify, request
from gevent.pywsgi import WSGIServer
from multiprocessing import Process, Queue
from triggerflow.service.databases import RedisDatabase
import triggerflow.service.eventsources as eventsources
from .worker import Worker
import threading

app = Flask(__name__)
app.debug = False

workers = {}
monitors = {}
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


@app.route('/workspace/<workspace>', methods=['POST'])
def create_worker(workspace):
    """
    This method gets the request parameters and starts a new thread worker
    that will act as the event-processor for the the specific trigger workspace.
    It returns 400 error if the provided parameters are not correct.
    """
    if not db.workspace_exists(workspace):
        return jsonify('Workspace does not exists in the database'.format(workspace)), 400

    if workspace in monitors:
        return jsonify('Workspace {} is already created'.format(workspace)), 400

    logging.info('New request to create workspace {}'.format(workspace))

    start_worker_monitor(workspace)

    return jsonify('Created workspace {}'.format(workspace)), 201


def start_worker_monitor(workspace):
    """
    Auxiliary method to monitor a worker triggers
    """
    global monitors
    logging.info('Starting {} workspace monitor'.format(workspace))

    def monitor():

        if len(db.get(workspace, 'triggers')) > 1:
            start_worker(workspace)

        while True:
            if db.new_trigger(workspace):
                start_worker(workspace)
            else:
                break

    monitors[workspace] = threading.Thread(target=monitor, daemon=True)
    monitors[workspace].start()


def start_worker(workspace):
    """
    Auxiliary method to start a worker
    """
    global workers

    if workspace not in workers or not workers[workspace].is_alive():
        logging.info('Starting {} workspace'.format(workspace))
        workers[workspace] = Worker(workspace, private_credentials)
        workers[workspace].start()


@app.route('/workspace/<workspace>', methods=['DELETE'])
def delete_worker(workspace):
    logging.info('New request to delete workspace {}'.format(workspace))
    global workers, monitors

    if workspace not in monitors and workspace not in workers:
        return jsonify('Workspace {} is not active'.format(workspace)), 400
    else:
        if workspace in workers:
            if workers[workspace].is_alive():
                workers[workspace].stop_worker()
            del workers[workspace]
        del monitors[workspace]
        return jsonify('Workspace {} deleted'.format(workspace)), 200


def main():
    global private_credentials, db, workers

    # Create process group
    os.setpgrp()

    logger = logging.getLogger()
    logger.setLevel(logging.NOTSET)

    component = os.getenv('INSTANCE', 'triggerflow-controller')

    # Make sure we log to the console
    stream_handler = logging.StreamHandler()
    formatter = logging.Formatter('[%(asctime)s.%(msecs)03dZ][%(levelname)s][triggerflow] %(message)s',
                                  datefmt="%Y-%m-%dT%H:%M:%S")
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    logging.info('Starting Triggerflow Controller')

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

    port = int(os.getenv('PORT', 5000))
    server = WSGIServer(('', port), app, log=logging.getLogger())
    logging.info('Triggerflow service started on port {}'.format(port))

    workspaces = db.list_workspaces()
    for wsp in workspaces:
        start_worker_monitor(wsp)

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print('exiting...')
    finally:
        # Kill all child processes
        os.killpg(0, signal.SIGKILL)


if __name__ == '__main__':
    main()
