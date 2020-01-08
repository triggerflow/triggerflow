from api.sources.kafka import KafkaCloudEventSource, KafkaSASLAuthMode
from api.utils import load_config_yaml
from api.client import CloudEventProcessorClient, DefaultActions
from dags.dag import DAG

from uuid import uuid4
import json


def make(dag_def: str):
    dag_vars = {}
    exec(dag_def, globals(), dag_vars)
    l = list(filter(lambda obj: type(obj) is DAG, dag_vars.values()))
    if len(l) != 1:
        raise Exception("Found more than one DAG definition in the same file, or not definition at all")

    dag_obj = l.pop()
    dag_dict = dag_obj.to_dict()
    return json.dumps(dag_dict, indent=4)


def deploy(dag_json):
    dagrun_id = str(uuid4()) + '-' + dag_json['dag_id']

    kafka_credentials = load_config_yaml('~/kafka_credentials.yaml')
    ep_config = load_config_yaml('~/event-processor_credentials.yaml')

    # TODO Make event source generic
    event_source = KafkaCloudEventSource(broker_list=kafka_credentials['eventstreams']['kafka_brokers_sasl'],
                                         topic=dagrun_id,
                                         auth_mode=KafkaSASLAuthMode.SASL_PLAINTEXT,
                                         username=kafka_credentials['eventstreams']['user'],
                                         password=kafka_credentials['eventstreams']['password'])

    ep = CloudEventProcessorClient(namespace=dagrun_id,
                                   event_source=event_source,
                                   global_context={
                                       'ibm_cf_credentials': ep_config['authentication']['ibm_cf_credentials'],
                                       'kafka_credentials': kafka_credentials['eventstreams']},
                                   api_endpoint=ep_config['event_processor']['api_endpoint'],
                                   authentication=ep_config['authentication'])

    # TODO Update dag build to add function url to the json definition
    for init_task in dag_json['initial_tasks']:
        task = dag_json[init_task]
        ep.add_trigger(event_source.event('init__'),
                       action=DefaultActions.IBM_CF_INVOKE_KAFKA,
                       context={'subject': init_task,
                                'args': task['operator']['function_args'],
                                'url': 'https://us-east.functions.cloud.ibm.com/api/v1/namespaces/\
                                       cloudlab_urv_us_east/actions/{}/{}'.format(
                                           task['operator']['function_package'], task['operator']['function_name'])})

    for task in dag_json['tasks']:
        for downstream_relative in task['downstream_relatives']:
            ep.add_trigger([event_source.event(upstream_relative) for upstream_relative in task['upstream_relatives']],
                           action=DefaultActions.IBM_CF_INVOKE_KAFKA,
                           context={'subject': downstream_relative,
                                    'args': task['operator']['function_args'],
                                    'url': 'https://us-east.functions.cloud.ibm.com/api/v1/namespaces/\
                                            cloudlab_urv_us_east/actions/{}/{}'.format(
                                                task['operator']['function_package'],
                                                task['operator']['function_name'])})

    for final_task in dag_json['final_tasks']:
        task = dag_json[final_task]
        ep.add_trigger(event_source.event(final_task),
                       action=DefaultActions.IBM_CF_INVOKE_KAFKA,
                       context={'subject': 'end__',
                                'args': task['operator']['function_args'],
                                'url': 'https://us-east.functions.cloud.ibm.com/api/v1/namespaces/\
                                       cloudlab_urv_us_east/actions/{}/{}'.format(
                                           task['operator']['function_package'], task['operator']['function_name'])})

    return dagrun_id


def run(dag_id):
    pass
