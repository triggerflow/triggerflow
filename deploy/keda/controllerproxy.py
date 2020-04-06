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
k_co_api = None

scaledobject_res = """
apiVersion: keda.k8s.io/v1alpha1
kind: ScaledObject
metadata:
  name: triggerflow-keda-worker-so
spec:
  scaleType: job
  jobTargetRef:
    parallelism: 1
    #completions: 1
    #activeDeadlineSeconds: 600
    ttlSecondsAfterFinished: 10
    backoffLimit: 0
    template:
      metadata:
        labels:
          app: triggerflow-keda-worker
      spec:
        containers:
        - name: triggerflow-keda-worker
          image: jsampe/triggerflow-keda-worker
          env:
            - name: WORKSPACE
              value: 'workspace_name'
          resources:
            limits:
              memory: 512Mi
              cpu: 250m

        restartPolicy: Never
  pollingInterval: 10  # Optional. Default: 30 seconds
  cooldownPeriod:  10 # Optional. Default: 300 seconds
  minReplicaCount: 0   # Optional. Default: 0
  maxReplicaCount: 1   # Optional. Default: 100
  triggers:
  - type: kafka
    metadata:
      bootstrapServers: IP:PORT
      consumerGroup: my-group
      topic: my-topic
      lagThreshold: '5'
"""

deployment_res = """
apiVersion: apps/v1
kind: Deployment
metadata:
  name: triggerflow-keda-worker
spec:
  selector:
    matchLabels:
      app: triggerflow-keda-worker
  replicas: 0
  template:
    metadata:
      labels:
        app: triggerflow-keda-worker
    spec:
      containers:
      - name: triggerflow-keda-worker
        image: jsampe/triggerflow-keda-worker
        env:
          - name: WORKSPACE
            value: 'workspace'
        resources:
          limits:
            memory: 256Mi
            cpu: 250m
"""

scaledobject_res = """
apiVersion: keda.k8s.io/v1alpha1
kind: ScaledObject
metadata:
  name: triggerflow-keda-worker-so
  namespace: default
spec:
  scaleTargetRef:
    deploymentName: triggerflow-keda-worker
  pollingInterval: 5  # Optional. Default: 30 seconds
  cooldownPeriod:  10  # Optional. Default: 300 seconds
  minReplicaCount: 0   # Optional. Default: 0
  maxReplicaCount: 1   # Optional. Default: 100
  triggers:
  - type: kafka
    metadata:
      bootstrapServers: IP:PORT
      consumerGroup: my-group
      topic: my-topic
      lagThreshold: '5'
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

    if create_keda_scaledobject(workspace):
        return jsonify('Created workspace {}'.format(workspace)), 201
    else:
        return jsonify('Workspace {} is already created'.format(workspace)), 400


def create_keda_scaledobject(workspace):
    """
    Auxiliary method to start a worker
    """
    dpl_res = yaml.safe_load(deployment_res)

    service_name = 'triggerflow-keda-worker-{}'.format(workspace)

    dpl_res['metadata']['name'] = service_name
    dpl_res['spec']['selector']['matchLabels']['app'] = service_name
    dpl_res['spec']['template']['metadata']['labels']['app'] = service_name
    dpl_res['spec']['template']['spec']['containers'][0]['name'] = service_name
    dpl_res['spec']['template']['spec']['containers'][0]['env'][0]['value'] = workspace

    try:
        k_v1_api.create_namespaced_deployment(
                namespace='default',
                body=dpl_res
            )
    except Exception as e:
        print('Warning: {}'.format(str(e)))

    so_res = yaml.safe_load(scaledobject_res)
    so_res['metadata']['name'] = '{}-so'.format(service_name)
    so_res['spec']['scaleTargetRef']['deploymentName'] = service_name
    # so_res['spec']['jobTargetRef']['template']['metadata']['labels']['app'] = service_name
    # so_res['spec']['jobTargetRef']['template']['spec']['containers'][0]['name'] = service_name
    # so_res['spec']['jobTargetRef']['template']['spec']['containers'][0]['env'][0]['value'] = workspace

    event_sources = db.get(workspace, 'event_sources')
    for es_name in event_sources:
        es = event_sources[es_name]
        if es['class'] == 'KafkaEventSource':
            so_res['spec']['triggers'][0]['metadata']['bootstrapServers'] = ','.join(es['broker_list'])
            so_res['spec']['triggers'][0]['metadata']['consumerGroup'] = es['name']
            so_res['spec']['triggers'][0]['metadata']['topic'] = es['topic']

    try:
        k_co_api.create_namespaced_custom_object(
                group="keda.k8s.io",
                version="v1alpha1",
                namespace='default',
                plural="scaledobjects",
                body=so_res
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
        service_name = 'triggerflow-keda-worker-{}'.format(workspace)
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

    try:
        service_name = 'triggerflow-keda-worker-{}-so'.format(workspace)
        k_co_api.delete_namespaced_custom_object(
                group="keda.k8s.io",
                version="v1alpha1",
                name=service_name,
                namespace='default',
                plural="scaledobjects",
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
    k_co_api = client.CustomObjectsApi()

    workspaces = db.list_workspaces()
    if workspaces:
        for wsp in workspaces:
            print('Starting {} workspace...'.format(wsp))
            create_keda_scaledobject(wsp)

    print('Triggerflow controller started')


main()
