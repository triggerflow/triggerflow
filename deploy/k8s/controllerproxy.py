import logging
import yaml
from flask import Flask, jsonify, request
from kubernetes import client, config
from redis_db import RedisDatabase

logger = logging.getLogger('triggerflow-controller')

app = Flask(__name__)

private_credentials = None
db = None
k_v1_api = None

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


def authenticate_request(db, auth):
    if not auth or \
       'username' not in auth \
       or 'password' not in auth:
        return False

    passwd = db.get_auth(username=auth['username'])
    return passwd and passwd == auth['password']


@app.before_request
def before_request_func():
    if not authenticate_request(db, request.auth):
        return jsonify('Unauthorized'), 401


@app.route('/workspace/<workspace>', methods=['POST'])
def create_workspace(workspace):
    """
    This method gets the request parameters and starts a new thread worker
    that will act as the event-processor for the the specific trigger workspace.
    It returns 400 error if the provided parameters are not correct.
    """
    if not db.workspace_exists(workspace):
        return jsonify('Workspace {} does not exists in the database'.format(workspace)), 400

    print('New request to create workspace {}'.format(workspace))

    if create_k8s_deployment(workspace):
        return jsonify('Created workspace {}'.format(workspace)), 201
    else:
        return jsonify('Workspace {} is already created'.format(workspace)), 400


def create_k8s_deployment(workspace):
    """
    Auxiliary method to start a worker
    """
    svc_res = yaml.safe_load(service_res)

    service_name = 'triggerflow-worker-{}'.format(workspace)
    svc_res['metadata']['name'] = service_name
    svc_res['spec']['template']['spec']['containers'][0]['env'][0]['value'] = workspace

    try:
        k_v1_api.create_namespaced_deployment(
                namespace='default',
                body=svc_res
            )
    except Exception as e:
        print('Warning: {}'.format(str(e)))
        return False

    print('Created workspace {}'.format(workspace))
    return True


@app.route('/workspace/<workspace>', methods=['DELETE'])
def delete_workspace(workspace):
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


def main():
    print('Starting Triggerflow controller')

    global private_credentials, db, k_v1_api, k_co_api

    print('Loading private credentials')
    with open('config.yaml', 'r') as config_file:
        private_credentials = yaml.safe_load(config_file)

    print('Creating DB connection')
    db = RedisDatabase(**private_credentials['redis'])

    print('loading kubernetes config')
    config.load_incluster_config()
    k_v1_api = client.AppsV1Api()

    workspaces = db.list_workspaces()
    if workspaces:
        for wsp in workspaces:
            print('Starting {} workspace...'.format(wsp))
            create_k8s_deployment(wsp)

    print('Triggerflow controller started')


main()
