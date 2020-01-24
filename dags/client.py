import json
import os
from uuid import uuid4
from importlib import import_module

from eventprocessor_client.sources.kafka import KafkaCloudEventSource, KafkaAuthMode
from eventprocessor_client.utils import load_config_yaml
from eventprocessor_client.client import CloudEventProcessorClient, CloudEvent, DefaultActions, DefaultConditions
from dags.dag import DAG



def make(dag_path: str):
    if not (dag_path.endswith('.py') or os.path.isfile(dag_path)):
        raise Exception('Path must be a python script containing a DAG definition')
    package, _ = dag_path.replace('/', '.').rsplit('.', 1)
    mod = import_module(package)
    attributes = dir(mod)

    dags = [attribute for attribute in attributes if type(getattr(mod, attribute)) is DAG]

    if len(dags) < 1:
        raise Exception("No DAGs found")
    elif len(dags) > 1:
        raise Exception("Multiple DAGs in the same file is not supported yet")

    dag = getattr(mod, dags.pop())
    dag_dict = dag.to_dict()
    return json.dumps(dag_dict, indent=4)

def deploy(dag_json):
    dagrun_id = '_'.join([dag_json['dag_id'], str(uuid4())])
    ep_config = load_config_yaml('~/client_config.yaml')
    kafka_credentials = ep_config['event_sources']['kafka']


    # TODO Make event source generic
    event_source = KafkaCloudEventSource(name=dagrun_id,
                                         broker_list=kafka_credentials['kafka_brokers_sasl'],
                                         topic=dagrun_id,
                                         auth_mode=KafkaAuthMode.SASL_PLAINTEXT,
                                         username=kafka_credentials['user'],
                                         password=kafka_credentials['password'])

    ep = CloudEventProcessorClient(api_endpoint=ep_config['event_processor']['api_endpoint'],
                                   user=ep_config['event_processor']['user'],
                                   password=ep_config['event_processor']['password'],
                                   namespace=dagrun_id,
                                   eventsource_name=dagrun_id)

    ep.create_namespace(dagrun_id, global_context=ep_config['global_context'], event_source=event_source)

    tasks = dag_json['tasks']
    for task_name, task in tasks.items():
        if not task['downstream_relatives']:
            task['downstream_relatives'].append('__end')

        for _ in task['downstream_relatives']:

            if not task['upstream_relatives']:
                condition = DefaultConditions.TRUE
                task['upstream_relatives'].append('init__')
            else:
                condition = DefaultConditions.IBM_CF_JOIN

            context = {'subject': task_name}
            context.update(task['operator'])
            ep.add_trigger([CloudEvent(upstream_relative) for upstream_relative in task['upstream_relatives']],
                           action=DefaultActions.IBM_CF_INVOKE_KAFKA,
                           condition=condition,
                           context=context)

    # Join final tasks
    ep.add_trigger([CloudEvent(end_task) for end_task in dag_json['final_tasks']],
                   action=DefaultActions.TERMINATE,
                   condition=DefaultConditions.IBM_CF_JOIN,
                   context={'subject': '__end'})

    return dagrun_id


def run(dag_id):
    pass
