import logging
import os
import signal
import yaml

from flask import Flask, jsonify, request
from gevent.pywsgi import WSGIServer

from eventprocessor.libs.cloudant_client import CloudantClient
from eventprocessor.health import generateHealthReport
from eventprocessor.worker import Worker
from eventprocessor.utils import authenticate_request

app = Flask(__name__)
app.debug = False

workers = {}
private_credentials = None
db = None


@app.route('/namespace/<namespace>', methods=['POST'])
def start_worker(namespace):
    """
    This method gets the request parameters and starts a new thread worker
    that will act as the event-processor for the the specific trigger namespace.
    It returns 400 error if the provided parameters are not correct.
    """
    if not authenticate_request(db, request):
        return jsonify('Unauthorized'), 401

    global workers
    if namespace in workers.keys():
        return jsonify('Worker for namespace {} is already running'.format(namespace)), 400
    logging.info('New request to run worker for namespace {}'.format(namespace))
    worker = Worker(namespace, private_credentials)
    worker.start()
    workers[namespace] = worker

    return jsonify('Started worker for namespace {}'.format(namespace)), 201


@app.route('/namespace/<namespace>', methods=['DELETE'])
def delete_worker(namespace):
    if not authenticate_request(db, request):
        return jsonify('Unauthorized'), 401

    global workers
    if namespace not in workers:
        return jsonify('Worker for namespcace {} is not active'.format(namespace)), 400
    else:
        workers[namespace].stop_worker()
        return jsonify('Worker for namespcace {} stopped'.format(namespace)), 200



@app.route('/')
def test_route():
    return jsonify('Hi!')


# TODO call TheDoctor.isAlive() and report on that
@app.route('/health')
def health_route():
    return jsonify(generateHealthReport(workers))


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

    logging.info('Starting Event Processor Service')

    # also log to file if /logs is present
    if os.path.isdir('/logs'):
        fh = logging.FileHandler('/logs/{}_logs.log'.format(component))
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    logging.info('Loading private credentials')
    with open('config.yaml', 'r') as config_file:
        private_credentials = yaml.safe_load(config_file)

    logging.info('Creating database client')
    db = CloudantClient(**private_credentials['cloudant'])

    port = int(os.getenv('PORT', 5000))
    server = WSGIServer(('', port), app, log=logging.getLogger())
    logging.info('Event Processor service started')
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print('exiting...')
    finally:
        # Kill all child processes
        os.killpg(0, signal.SIGKILL)


if __name__ == '__main__':
    main()
