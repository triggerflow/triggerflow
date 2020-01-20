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
import signal
import yaml
from datetime import datetime

import dateutil.parser
from flask import Flask, jsonify, request
from gevent.pywsgi import WSGIServer

from standalone_service.libs.cloudant_client import CloudantClient
from standalone_service.health import generateHealthReport
from standalone_service.worker import Worker

app = Flask(__name__)
app.debug = False

workers = []
private_credentials = None
db = None


@app.route('/run', methods=['POST'])
def run_workflow():
    """
    This method gets the request parameters and starts a new thread worker
    that will act as the event-processor for the the specific trigger namespace.
    It returns 404 error if the provided parameters are not correct.
    """
    try:
        data = request.get_json(force=True, silent=True)
        namespace = data['namespace']
        # user_credentials = data['authentication']
    except Exception as e:
        logging.error(e)
        return jsonify('Invalid input parameters'), 400

    # Authenticate request
    # if 'token' not in user_credentials:
    #     logging.warn("Client {} has tried to connect without proper authentication.".format(request.remote_addr))
    #     return jsonify('Unauthorized'), 401
    # token = user_credentials['token']
    # try:
    #     sessions = db.get(database_name=namespace, document_id='sessions')
    # except KeyError:
    #     logging.warn("Client {} has tried to start worker {} but it doesn't exist.".format(request.remote_addr, namespace))
    #     return jsonify('Unauthorized'), 401
    # if token in sessions:
    #     emitted_token_time = dateutil.parser.parse(sessions[token])
    #     seconds = (datetime.utcnow() - emitted_token_time).seconds
    #     if seconds > 3600:
    #         logging.warn("Client {} has tried to connect with expired token.".format(request.remote_addr))
    #         return jsonify('Token expired'), 401
    # else:
    #     logging.warn("Client {} has tried to connect without proper authentication.".format(request.remote_addr))
    #     return jsonify('Unauthorized'), 401

    global workers
    if namespace in list(map(lambda wk: wk.namespace, workers)):
        return jsonify('Worker for namespace {} is already running'.format(namespace)), 400
    logging.info('New request to run worker for namespace {}'.format(namespace))
    worker = Worker(namespace, private_credentials)
    # worker.daemon = True
    worker.start()
    workers.append(workers)

    return jsonify('Started worker for namespace {}'.format(namespace)), 201


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

    component = os.getenv('INSTANCE', 'event_trigger-0')

    # Make sure we log to the console
    stream_handler = logging.StreamHandler()
    formatter = logging.Formatter('[%(asctime)s.%(msecs)03dZ][%(levelname)s][event-router] %(message)s',
                                  datefmt="%Y-%m-%dT%H:%M:%S")
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    logging.info('Starting event_trigger standalone_service for IBM Composer')

    # also log to file if /logs is present
    if os.path.isdir('/logs'):
        fh = logging.FileHandler('/logs/{}_logs.log'.format(component))
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    logging.info('Loading private credentials')
    with open('config.yaml', 'r') as config_file:
        private_credentials = yaml.safe_load(config_file)

    db = CloudantClient(private_credentials['cloudant']['username'], private_credentials['cloudant']['apikey'])

    port = int(os.getenv('PORT', 5000))
    server = WSGIServer(('', port), app, log=logging.getLogger())
    logging.info('HTTP server started')
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print('exiting...')
    finally:
        os.killpg(0, signal.SIGKILL)


if __name__ == '__main__':
    main()
