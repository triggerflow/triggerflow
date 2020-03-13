import logging
import os
import yaml
from flask import Flask, jsonify, request
from kubernetes import client, config, watch
import requests as req

from redis_db import RedisDatabase

logger = logging.getLogger('triggerflow-controller')

app = Flask(__name__)

TOTAL_REQUESTS = 0

print('Loading private credentials')
with open('config.yaml', 'r') as config_file:
    private_credentials = yaml.safe_load(config_file)

print('Creating DB connection')
db = RedisDatabase(**private_credentials['redis'])

print('loading kubernetes config')
config.load_incluster_config()
k_v1_api = client.CoreV1Api()

service_res = """
apiVersion: apps/v1
kind: Deployment
metadata:
  name: triggerflow-worker
spec:
  selector:
    matchLabels:
      app: triggerflow-worker
  replicas: 1
  template:
    metadata:
      labels:
        app: triggerflow-worker
    spec:
      containers:
      - name: triggerflow-worker
        image: jsampe/triggerflow-worker
        env:
          - name: NAMESPACE
            value: 'pywren'
        resources:
          limits:
            memory: 1Gi
            cpu: 1000m
"""


def authenticate_request(db, request):
    if not request.authorization or \
       'username' not in request.authorization \
       or 'password' not in request.authorization:
        return False

    passwd = db.get_auth(username=request.authorization['username'])
    return passwd and passwd == request.authorization['password']


@app.route('/workspace/<workspace>', methods=['POST'])
def start_worker(workspace):
    """
    This method gets the request parameters and starts a new thread worker
    that will act as the event-processor for the the specific trigger workspace.
    It returns 400 error if the provided parameters are not correct.
    """
    if not authenticate_request(db, request):
        return jsonify('Unauthorized'), 401

    print('New request to start a worker for workspace {}'.format(workspace))

    svc_res = yaml.safe_load(service_res)

    service_name = 'triggerflow-worker-{}'.format(workspace)
    svc_res['metadata']['name'] = service_name
    svc_res['spec']['template']['spec']['containers'][0]['env'][0]['value'] = workspace

    try:
        # create the service resource
        k_v1_api.create_namespaced_deployment(
                namespace='default',
                body=svc_res
            )

        w = watch.Watch()
        for event in w.stream(k_v1_api.list_namespaced_deployment,
                              namespace='default',
                              field_selector="metadata.name={0}".format(service_name)):
            conditions = None
            if event['object'].get('status') is not None:
                conditions = event['object']['status']['conditions']
                if event['object']['status'].get('url') is not None:
                    service_url = event['object']['status']['url']
            if conditions and conditions[0]['status'] == 'True' and \
               conditions[1]['status'] == 'True' and conditions[2]['status'] == 'True':
                w.stop()
        worker_created = True
    except Exception as e:
        print('Warning: {}'.format(str(e)))
        worker_created = False

    if worker_created:
        return jsonify('Started workspace {}'.format(workspace)), 201
    else:
        return jsonify('Workspace {} is already running'.format(workspace)), 400


@app.route('/workspace/<workspace>', methods=['DELETE'])
def delete_worker(workspace):
    if not authenticate_request(db, request):
        return jsonify('Unauthorized'), 401

    print('Stopping workspace: {}'.format(workspace))
    try:
        # delete the service resource if exists
        service_name = 'triggerflow-worker-{}'.format(workspace)
        k_v1_api.delete_namespaced_deployment(
                name=service_name,
                namespace='default',
                body=client.V1DeleteOptions()
            )
        print('Workspace {} stopped'.format(workspace))
        worker_deleted = True
    except Exception:
        # Most probable exception is the service does not exists
        print('Workspace {} is not active'.format(workspace))
        worker_deleted = False

    if worker_deleted:
        return jsonify('Workspace {} stopped'.format(workspace)), 200
    else:
        return jsonify('Workspace {} is not active'.format(workspace)), 400


@app.route('/')
def test_route():
    return jsonify('Hi!')


@app.route('/test', methods=['GET', 'POST'])
def net_test():
    global TOTAL_REQUESTS
    TOTAL_REQUESTS = TOTAL_REQUESTS+1
    logger.info('Checking Internet connection: {} Request'.format(request.method))
    message = request.get_json(force=True, silent=True)

    print(message, flush=True)

    url = os.environ.get('URL', 'https://httpbin.org/get')
    resp = req.get(url)
    print(resp.status_code, flush=True)

    if resp.status_code == 200:
        return_statement = {'Internet Connection': "True", "Total Requests": TOTAL_REQUESTS}
    else:
        return_statement = {'Internet Connection': "False", "Total Requests": TOTAL_REQUESTS}

    # return_statement = {"Total Requests": TOTAL_REQUESTS}
    return jsonify(return_statement), 200


def main():
    port = int(os.getenv('PORT', 8080))
    app.run(debug=True, host='0.0.0.0', port=port)

    logger.info('Trigger Service controller started')


if __name__ == '__main__':
    main()
