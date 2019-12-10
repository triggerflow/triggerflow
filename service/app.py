"""Flask application.

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
"""

import logging
import os
import json
from datetime import datetime

import dateutil.parser
from flask import Flask, jsonify, request
from gevent.pywsgi import WSGIServer

from service.libs.cloudant_client import CloudantClient
from .health import generateHealthReport
from .worker import Worker

app = Flask(__name__)
app.debug = False

workers = []
private_credentials = None
db = None


@app.route('/start_worker', methods=['POST'])
def run_workflow():
    """
    This method gets the request parameters and starts a new thread worker
    that will act as the event-processor for the the specific trigger namespace.
    It returns 404 error if the provided parameters are not correct.
    """
    try:
        data = request.get_json(force=True, silent=True)
        namespace = data['namespace']
        user_credentials = data['user_credentials']
    except Exception as e:
        logging.error(e)
        return jsonify('Invalid input parameters'), 400

    # Authenticate request
    if 'token' not in data:
        return jsonify('Unauthorized'), 401
    token = data['token']
    sessions = db.get(database_name=namespace, document_id='sessions')
    if token in sessions:
        emitted_token_time = dateutil.parser.parse(sessions[token])
        seconds = (datetime.utcnow() - emitted_token_time).seconds
        if seconds > 3600:
            return jsonify('Token expired'), 401
    else:
        return jsonify('Unauthorized'), 401

    global workers
    if namespace in list(map(lambda wk: wk.namespace, workers)):
        return jsonify('Worker for namespace {} is already running'.format(namespace)), 400
    logging.info('New request to run worker for namespace {}'.format(namespace))
    worker = Worker(namespace, private_credentials, user_credentials)
    worker.daemon = True
    worker.start()
    workers.append(workers)

    return jsonify('Started worker for namespace {}'.format(namespace)), 201


@app.route('/get_workers', methods=['GET'])
def get_workers():
    workers_ns = list(map(lambda wk: wk.namespace, workers))
    return jsonify(workers=workers_ns), 200


@app.route('/')
def test_route():
    return jsonify('Hi!')


# TODO call TheDoctor.isAlive() and report on that
@app.route('/health')
def health_route():
    return jsonify(generateHealthReport(workers))


def main():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    component = os.getenv('INSTANCE', 'event_trigger-0')

    # Make sure we log to the console
    streamHandler = logging.StreamHandler()
    formatter = logging.Formatter('[%(asctime)s.%(msecs)03dZ][%(levelname)s][event-router] %(message)s',
                                  datefmt="%Y-%m-%dT%H:%M:%S")
    streamHandler.setFormatter(formatter)
    logger.addHandler(streamHandler)

    logging.info('Starting event_trigger service for IBM Composer')

    # also log to file if /logs is present
    if os.path.isdir('/logs'):
        fh = logging.FileHandler('/logs/{}_logs.log'.format(component))
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    logging.info('Loading private credentials')
    global private_credentials
    with open(os.path.expanduser('~/private_credentials.json'), 'r') as config_file:
        private_credentials = json.loads(config_file.read())['private_credentials']

    db = CloudantClient(private_credentials['cloudant']['username'], private_credentials['cloudant']['apikey'])

    port = int(os.getenv('PORT', 5000))
    server = WSGIServer(('', port), app, log=logging.getLogger())
    logging.info('HTTP server started')
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print('exiting...')


if __name__ == '__main__':
    main()
