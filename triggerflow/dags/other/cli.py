import json
import os
from importlib import import_module

from triggerflow.client import TriggerflowClient, CloudEvent, DefaultActions, DefaultConditions
from triggerflow.cache import get_triggerflow_config
import triggerflow.eventsources as event_sources_mod
from triggerflow.dags.dag import DAG


# TODO Update DAGs CLI

def make(dag_path: str):
    if not (dag_path.endswith('.py') or os.path.isfile(dag_path)):
        raise Exception('Path must be a python script containing a DAG definition')
    package, _ = dag_path.replace('/', '.').rsplit('.', 1)
    mod = import_module(package)
    attributes = dir(mod)

    dags = [attribute for attribute in attributes if type(getattr(mod, attribute)) == DAG]

    if len(dags) < 1:
        raise Exception("No DAGs found")
    elif len(dags) > 1:
        raise Exception("Multiple DAGs in the same file is not supported yet")

    dag = getattr(mod, dags.pop())
    dag_dict = dag.get_json_eventsource()
    return json.dumps(dag_dict, indent=4)


def deploy(dag_json):
    pass

def run(dagrun_id):
    tf_config = get_triggerflow_config('~/client_config.yaml')

    tf = TriggerflowClient(**tf_config['triggerflow'], workspace=dagrun_id)

    event_sources = tf.list_eventsources()
    event_source_name = event_sources['event_sources'].pop()

    event_source = tf.get_eventsource(event_source_name)
    event_source_class = getattr(event_sources_mod, event_source[event_source_name]['class'])
    event_source_if = event_source_class(**event_source[event_source_name])
    event_source_if.publish_cloudevent(
        {'source': 'tf_client', 'subject': 'init__', 'type': 'termination.event.success'})
    return 200, {'result': 'ok'}
