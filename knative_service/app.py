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
import yaml
import requests as req
from flask import Flask, jsonify, request
from gevent.pywsgi import WSGIServer

from libs.cloudant_client import CloudantClient
from health import generateHealthReport
from worker import Worker
from brokers.kafka_broker import KafkaBroker


app = Flask(__name__)
app.debug = False

workers = []
private_credentials = None
db = None
TOTAL_REQUESTS = 0


@app.route('/', methods=['POST'])
def run():
    """
    This method gets the request parameters and starts a new thread worker
    that will act as the event-processor for the the specific trigger namespace.
    It returns 404 error if the provided parameters are not correct.
    """
    try:
        data = request.get_json(force=True, silent=True)
        namespace = data['namespace']
    except Exception as e:
        logging.error(e)
        return jsonify('Invalid input parameters'), 400

    global workers
    if namespace in list(map(lambda wk: wk.namespace, workers)):
        return jsonify('Worker for namespace {} is already running'.format(namespace)), 400
    logging.info('New request to run worker for namespace {}'.format(namespace))
    #worker = Worker(namespace, private_credentials, user_credentials)
    #worker.daemon = True
    #worker.start()
    #workers.append(workers)

    return jsonify('Started worker for namespace {}'.format(namespace)), 201


@app.route('/test')
def test_route():
    global TOTAL_REQUESTS
    TOTAL_REQUESTS = TOTAL_REQUESTS+1
    logging.info('Checking Internet connection: {} Request'.format(request.method))
    message = request.get_json(force=True, silent=True)

    print(message, flush=True)

    url = os.environ.get('URL', 'https://httpbin.org/get')
    resp = req.get(url)
    print(resp.status_code, flush=True)
    #print('Sleeping 30 seconds', flush=True)
    #time.sleep(30)
    #print('Before sleep', flush=True)

    if resp.status_code == 200:
        return_statement = {'Internet Connection': "True", "Total Requests": TOTAL_REQUESTS}
    else:
        return_statement = {'Internet Connection': "False", "Total Requests": TOTAL_REQUESTS}

    config = {
      "broker_list": ["192.168.2.51:9092"],
      "username": "token",
      "password": "ZHmkYJVF9H9pMqPVspuez8RADRtkASXhIrOZCU6nmNY-",
      "topic": "FirstTopic"}

    kb = KafkaBroker(**config)

    print('listing topics')
    print(kb.consumer.list_topics().topics)
    print('Finishing')

    return jsonify(return_statement)


# TODO call TheDoctor.isAlive() and report on that
@app.route('/health')
def health_route():
    return jsonify(generateHealthReport(workers))


def main():
    global private_credentials, db

    logger = logging.getLogger()
    logger.setLevel(logging.NOTSET)

    component = os.getenv('INSTANCE', 'event_processor-0')

    # Make sure we log to the console
    stream_handler = logging.StreamHandler()
    formatter = logging.Formatter('[%(asctime)s.%(msecs)03dZ][%(levelname)s][event-router] %(message)s',
                                  datefmt="%Y-%m-%dT%H:%M:%S")
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    logging.info('Starting Event Processor knative service')

    # also log to file if /logs is present
    if os.path.isdir('/logs'):
        fh = logging.FileHandler('/logs/{}_logs.log'.format(component))
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    logging.info('Loading private credentials')
    with open('config.yaml', 'r') as config_file:
        private_credentials = yaml.safe_load(config_file)

    port = int(os.getenv('PORT', 5000))
    server = WSGIServer(('', port), app, log=logging.getLogger())
    logging.info('HTTP server started on port {}'.format(port))
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print('exiting...')


if __name__ == '__main__':
    main()
