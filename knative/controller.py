import logging
import os
import yaml
from flask import Flask, jsonify, request
from kubernetes import client, config, watch
from cloudant_client import CloudantClient

logger = logging.getLogger('triggerflow')

app = Flask(__name__)

workers = {}
private_credentials = None
db = None

config.load_incluster_config()
k_co_api = client.CustomObjectsApi()
k_v1_api = client.CoreV1Api()

service_res = """
apiVersion: serving.knative.dev/v1alpha1
kind: Service
metadata:
  name: triggerflow-worker
  #namespace: default
spec:
  template:
    metadata:
      annotations:
        autoscaling.knative.dev/minScale: "0"
        autoscaling.knative.dev/maxScale: "5"
        autoscaling.knative.dev/metric: cpu
        autoscaling.knative.dev/target: "85"
        autoscaling.knative.dev/class: hpa.autoscaling.knative.dev
    spec:
      #containerConcurrency: 1
      #timeoutSeconds: 300
      containers:
        - image: jsampe/triggerflow-controller
          env:
          - name: NAMESPACE
            value: 'default'
          resources:
            limits:
              memory: 2Gi
              cpu: 2
"""


def authenticate_request(db, request):
    if not request.authorization or 'username' not in request.authorization or 'password' not in request.authorization:
        return False

    passwd = db.get_key(database_name='auth$', document_id='users', key=request.authorization['username'])
    return passwd == request.authorization['password']


@app.route('/namespace/<namespace>', methods=['POST'])
def start_worker(namespace):
    """
    This method gets the request parameters and starts a new thread worker
    that will act as the event-processor for the the specific trigger namespace.
    It returns 400 error if the provided parameters are not correct.
    """

    #if not authenticate_request(db, request):
    #    return jsonify('Unauthorized'), 401

    logger.info('New request to start a worker for namespace {}'.format(namespace))

    logger.debug("Creating PyWren runtime service resource in k8s")
    svc_res = yaml.safe_load(service_res)

    service_name = 'triggerflow-worker-{}'.format(namespace)
    svc_res['metadata']['name'] = service_name
    svc_res['spec']['template']['spec']['containers'][0]['env'][0]['value'] = namespace

    try:
        # create the service resource
        k_co_api.create_namespaced_custom_object(
                group="serving.knative.dev",
                version="v1alpha1",
                namespace='default',
                plural="services",
                body=svc_res
            )

        w = watch.Watch()
        for event in w.stream(k_co_api.list_namespaced_custom_object,
                              namespace='default', group="serving.knative.dev",
                              version="v1alpha1", plural="services",
                              field_selector="metadata.name={0}".format(service_name)):
            conditions = None
            if event['object'].get('status') is not None:
                conditions = event['object']['status']['conditions']
                if event['object']['status'].get('url') is not None:
                    service_url = event['object']['status']['url']
            if conditions and conditions[0]['status'] == 'True' and \
               conditions[1]['status'] == 'True' and conditions[2]['status'] == 'True':
                w.stop()

        logger.info('Runtime Service resource created - URL: {}'.format(service_url))

        return jsonify('Started worker for namespace {}'.format(namespace)), 201
    except Exception:
        return jsonify('Worker for namespace {} is already running'.format(namespace)), 400


@app.route('/namespace/<namespace>', methods=['DELETE'])
def delete_worker(namespace):
    #if not authenticate_request(db, request):
    #    return jsonify('Unauthorized'), 401

    service_name = 'triggerflow-worker-{}'.format(namespace)
    try:
        # delete the service resource if exists
        k_co_api.delete_namespaced_custom_object(
                group="serving.knative.dev",
                version="v1alpha1",
                name=service_name,
                namespace='default',
                plural="services",
                body=client.V1DeleteOptions()
            )
        return jsonify('Worker for namespcace {} stopped'.format(namespace)), 200
    except Exception:
        return jsonify('Worker for namespcace {} is not active'.format(namespace)), 400


@app.route('/')
def test_route():
    return jsonify('Hi!')


def main():
    global private_credentials, db

    logger.info('Loading private credentials')
    with open('config.yaml', 'r') as config_file:
        private_credentials = yaml.safe_load(config_file)

    logger.info('Creating database client')
    db = CloudantClient(**private_credentials['cloudant'])

    port = int(os.getenv('PORT', 8080))
    app.run(debug=True, host='0.0.0.0', port=port)

    logger.info('Trigger Service controller started')


if __name__ == '__main__':
    main()
