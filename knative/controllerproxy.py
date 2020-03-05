import logging
import os
import yaml
from flask import Flask, jsonify, request
from kubernetes import client, config, watch
from cloudant_client import CloudantClient
import requests as req

logger = logging.getLogger('triggerflow-controller')

app = Flask(__name__)

TOTAL_REQUESTS = 0

print('Loading private credentials')
with open('config.yaml', 'r') as config_file:
    private_credentials = yaml.safe_load(config_file)

print('Creating DB connection')
db = CloudantClient(**private_credentials['cloudant'])

print('loading kubernetes config')
config.load_incluster_config()
k_co_api = client.CustomObjectsApi()
k_v1_api = client.CoreV1Api()

service_res = """
apiVersion: serving.knative.dev/v1
kind: Service
metadata:
  name: triggerflow-worker
  #namespace: default
spec:
  template:
    metadata:
      annotations:
        autoscaling.knative.dev/minScale: "1"
        autoscaling.knative.dev/maxScale: "1"
    spec:
      #containerConcurrency: 1
      #timeoutSeconds: 300
      containers:
        - image: jsampe/triggerflow-knative-worker
          env:
          - name: NAMESPACE
            value: 'default'
          resources:
            limits:
              memory: 1Gi
              cpu: 1
"""

event_source_res = """
apiVersion: sources.knative.dev/v1alpha1
kind: KafkaSource
metadata:
  name: kafka-source
spec:
  consumerGroup: triggerflow
  bootstrapServers: IP:PORT
  topics: topic-name
  sink:
    #apiVersion: serving.knative.dev/v1
    #kind: Service
    #name: event-display
    apiVersion: eventing.knative.dev/v1alpha1
    kind: Broker
    name: default
    #apiVersion: messaging.knative.dev/v1alpha1
    #kind: Channel
    #name: triggerflow-channel
  resources:
    requests:
      memory: 512Mi
      cpu: 1000m
"""

trigger_res = """
apiVersion: eventing.knative.dev/v1alpha1
kind: Trigger
metadata:
  name: triggerflow-worker-trigger
spec:
  subscriber:
    ref:
      apiVersion: serving.knative.dev/v1
      kind: Service
      #name: event-display
      name: triggerflow-worker-pywren
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
    global db

    #if not authenticate_request(db, request):
    #    return jsonify('Unauthorized'), 401

    print('New request to start a worker for namespace {}'.format(namespace))

    svc_res = yaml.safe_load(service_res)

    service_name = 'triggerflow-worker-{}'.format(namespace)
    svc_res['metadata']['name'] = service_name
    svc_res['spec']['template']['spec']['containers'][0]['env'][0]['value'] = namespace

    try:
        # create the service resource
        k_co_api.create_namespaced_custom_object(
                group="serving.knative.dev",
                version="v1",
                namespace='default',
                plural="services",
                body=svc_res
            )

        w = watch.Watch()
        for event in w.stream(k_co_api.list_namespaced_custom_object,
                              namespace='default', group="serving.knative.dev",
                              version="v1", plural="services",
                              field_selector="metadata.name={0}".format(service_name)):
            conditions = None
            if event['object'].get('status'):
                conditions = event['object']['status']['conditions']
                if event['object']['status'].get('url') is not None:
                    service_url = event['object']['status']['url']
            if conditions and conditions[0]['status'] == 'True' and \
               conditions[1]['status'] == 'True' and conditions[2]['status'] == 'True':
                w.stop()

        print('Trigger Service worker created - URL: {}'.format(service_url))
        worker_created = True
    except Exception as e:
        print('Warning: {}'.format(str(e)))
        worker_created = False

    try:
        # Create event sources
        print('Starting event sources...')
        event_sources = db.get(database_name=namespace, document_id='.event_sources')
        for evt_src in event_sources.values():
            if evt_src['class'] == 'KafkaCloudEventSource':
                print('Starting {}'.format(evt_src['name']))
                es_res = yaml.safe_load(event_source_res)
                es_res['metadata']['name'] = evt_src['name']
                es_res['spec']['bootstrapServers'] = ','.join(evt_src['spec']['broker_list'])
                es_res['spec']['topics'] = evt_src['spec']['topic']
                #es_res['spec']['sink']['name'] = service_name

                # create the service resource
                k_co_api.create_namespaced_custom_object(
                        group="sources.knative.dev",
                        version="v1alpha1",
                        namespace='default',
                        plural="kafkasources",
                        body=es_res
                    )

                w = watch.Watch()
                for event in w.stream(k_co_api.list_namespaced_custom_object,
                                      namespace='default', group="sources.knative.dev",
                                      version="v1alpha1", plural="kafkasources",
                                      field_selector="metadata.name={0}".format(evt_src['name'])):
                    conditions = None
                    if event['object'].get('status'):
                        conditions = event['object']['status']['conditions']
                        if event['object']['status'].get('url') is not None:
                            service_url = event['object']['status']['url']
                    if conditions and conditions[0]['status'] == 'True' and \
                       conditions[1]['status'] == 'True' and conditions[2]['status'] == 'True':
                        w.stop()
            else:
                return jsonify('Not supported event source: {}'.format(evt_src['class'])), 400
        print('Event source started')
    except Exception as e:
        print('Warning: {}'.format(str(e)))

    if worker_created:
        return jsonify('Started worker for namespace {}'.format(namespace)), 201
    else:
        return jsonify('Worker for namespace {} is already running'.format(namespace)), 400


@app.route('/namespace/<namespace>', methods=['DELETE'])
def delete_worker(namespace):
    #if not authenticate_request(db, request):
    #    return jsonify('Unauthorized'), 401

    print('Stoppnig workers for namespace: {}'.format(namespace))
    try:
        # delete the service resource if exists
        service_name = 'triggerflow-worker-{}'.format(namespace)
        k_co_api.delete_namespaced_custom_object(
                group="serving.knative.dev",
                version="v1",
                name=service_name,
                namespace='default',
                plural="services",
                body=client.V1DeleteOptions()
            )
        print('Workers for namespcace {} stopped'.format(namespace))
        worker_deleted = True
    except Exception:
        # Most probable exception is the service does not exists
        print('Worker for namespcace {} is not active'.format(namespace))
        worker_deleted = False

    # Stop event sources
    event_sources = db.get(database_name=namespace, document_id='.event_sources')
    print('Stopping event sources for namespace {}'.format(namespace))
    for evt_src in event_sources.values():
        if evt_src['class'] == 'KafkaCloudEventSource':
            print('Stopping {}'.format(evt_src['name']))
            try:
                k_co_api.delete_namespaced_custom_object(
                        group="sources.knative.dev",
                        version="v1alpha1",
                        name=evt_src['name'],
                        namespace='default',
                        plural="kafkasources",
                        body=client.V1DeleteOptions()
                    )
            except Exception:
                pass
    print('Event sources for namespace {} stopped'.format(namespace))

    if worker_deleted:
        return jsonify('Worker for namespcace {} stopped'.format(namespace)), 200
    else:
        return jsonify('Worker for namespcace {} is not active'.format(namespace)), 400


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
