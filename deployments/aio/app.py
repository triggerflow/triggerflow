import logging
import os
import signal
import yaml

from flask import Flask, jsonify, request
from gevent.pywsgi import WSGIServer

from triggerflow.service.databases import RedisClient
from triggerflow.service.utils import authenticate_request

from .worker import Worker


app = Flask(__name__)
app.debug = False

workers = {}
private_credentials = None
db = None


@app.route('/workspace/<workspace>', methods=['POST'])
def start_worker(workspace):
    """
    This method gets the request parameters and starts a new thread worker
    that will act as the event-processor for the the specific trigger workspace.
    It returns 400 error if the provided parameters are not correct.
    """
    if not authenticate_request(db, request):
        return jsonify('Unauthorized'), 401

    global workers
    if workspace in workers.keys():
        return jsonify('Worksapce {} is already running'.format(workspace)), 400

    if not db.workspace_exists(workspace):
        return jsonify('Workspace does not exists in the database'.format(workspace)), 400

    logging.info('New request to run workspace {}'.format(workspace))
    worker = Worker(workspace, private_credentials)
    worker.start()
    workers[workspace] = worker

    return jsonify('Started workspace {}'.format(workspace)), 201


@app.route('/workspace/<workspace>', methods=['DELETE'])
def delete_worker(workspace):
    if not authenticate_request(db, request):
        return jsonify('Unauthorized'), 401

    global workers
    if workspace not in workers:
        return jsonify('Workspace {} is not active'.format(workspace)), 400
    else:
        workers[workspace].stop_worker()
        return jsonify('Workspace {} stopped'.format(workspace)), 200


def main():
    global private_credentials, db

    # Create process group
    os.setpgrp()

    logger = logging.getLogger()
    logger.setLevel(logging.NOTSET)

    component = os.getenv('INSTANCE', 'event_processor-0')

    # Make sure we log to the console
    stream_handler = logging.StreamHandler()
    formatter = logging.Formatter('[%(asctime)s.%(msecs)03dZ][%(levelname)s][triggerflow] %(message)s',
                                  datefmt="%Y-%m-%dT%H:%M:%S")
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    logging.info('Starting Triggerflow Service')

    # also log to file if /logs is present
    if os.path.isdir('/logs'):
        fh = logging.FileHandler('/logs/{}_logs.log'.format(component))
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    logging.info('Loading private credentials')
    with open('config.yaml', 'r') as config_file:
        private_credentials = yaml.safe_load(config_file)

    logging.info('Creating database client')
    db = RedisClient(**private_credentials['redis'])

    port = int(os.getenv('PORT', 5000))
    server = WSGIServer(('', port), app, log=logging.getLogger())
    logging.info('Triggerflow service started on port {}'.format(port))
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print('exiting...')
    finally:
        # Kill all child processes
        os.killpg(0, signal.SIGKILL)


if __name__ == '__main__':
    main()
