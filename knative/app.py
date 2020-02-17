import logging
import os
import yaml
from flask import Flask, jsonify, request
from eventprocessor.libs.cloudant_client import CloudantClient
from eventprocessor.health import generateHealthReport
from eventprocessor.worker import Worker
from eventprocessor.utils import authenticate_request

logger = logging.getLogger('triggerflow')

app = Flask(__name__)

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
    logger.info('New request to run worker for namespace {}'.format(namespace))
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
        msg = 'Worker for namespcace {} is not active'.format(namespace)
        logger.info(msg)
        return jsonify(), 400
    else:
        workers[namespace].stop_worker()
        msg = 'Worker for namespcace {} stopped'.format(namespace)
        logger.info(msg)
        return jsonify(), 200


@app.route('/')
def test_route():
    return jsonify('Hi!')


# TODO call TheDoctor.isAlive() and report on that
@app.route('/health')
def health_route():
    return jsonify(generateHealthReport(workers))


def main():
    global private_credentials, db

    logger.info('Loading private credentials')
    with open('config.yaml', 'r') as config_file:
        private_credentials = yaml.safe_load(config_file)

    logger.info('Creating database client')
    db = CloudantClient(**private_credentials['cloudant'])

    port = int(os.getenv('PORT', 8080))
    app.run(debug=True, host='0.0.0.0', port=port)

    logger.info('Event Processor service started')


if __name__ == '__main__':
    main()
