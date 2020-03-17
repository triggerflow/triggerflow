import json
import os
from uuid import uuid4
from importlib import import_module

from triggerflow.client.utils import load_config_yaml
from triggerflow.client.client import TriggerflowClient, CloudEvent, DefaultActions, DefaultConditions
import triggerflow.client.sources as event_sources_mod
from dags.dag import DAG


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
    dag_dict = dag.to_dict()
    return json.dumps(dag_dict, indent=4)


def deploy(dag_json):
    dagrun_id = '_'.join([dag_json['dag_id'], str(uuid4())])
    ep_config = load_config_yaml('~/client_config.yaml')
    dags_config = load_config_yaml('~/dags_config.yaml')

    evt_src_config = dags_config['event_sources'][dag_json['eventsource']]
    evt_src_class = dags_config['event_sources'][dag_json['eventsource']]['class']
    del evt_src_config['class']

    mod = import_module('triggerflow.client.sources')
    evt_src = getattr(mod, '{}EventSource'.format(evt_src_class))
    event_source = evt_src(name=dagrun_id,
                           topic=dagrun_id,
                           **evt_src_config)

    tf = TriggerflowClient(**ep_config['triggerflow'],
                           workspace=dagrun_id,
                           caching=True)

    gc = dags_config['operators'].copy()
    gc[evt_src_class.lower()] = evt_src_config
    tf.create_workspace(dagrun_id, global_context=gc, event_source=event_source)

    tasks = dag_json['tasks']
    for task_name, task in tasks.items():
        context = {'subject': task_name, 'dependencies': {}}

        if not task['downstream_relatives']:
            task['downstream_relatives'].append('__end')

        if task['upstream_relatives']:
            condition = DefaultConditions.FUNCTION_JOIN
            for upstream_relative in task['upstream_relatives']:
                context['dependencies'][upstream_relative] = {'join': -1, 'counter': 0}
        else:
            condition = DefaultConditions.TRUE
            task['upstream_relatives'].append('init__')

        context.update(task['operator'])
        tf.add_trigger([CloudEvent(upstream_relative) for upstream_relative in task['upstream_relatives']],
                       action=DefaultActions[task['operator']['trigger_action']],
                       condition=condition,
                       context=context)

    # Join final tasks
    tf.add_trigger([CloudEvent(end_task) for end_task in dag_json['final_tasks']],
                   action=DefaultActions.TERMINATE,
                   condition=DefaultConditions.FUNCTION_JOIN,
                   context={'subject': '__end', 'dependencies': {end_task: {'join': -1, 'counter': 0}
                                                                 for end_task in dag_json['final_tasks']}})

    tf.commit_cached_triggers()
    return dagrun_id


def run(dagrun_id):
    tf_config = load_config_yaml('~/client_config.yaml')

    tf = TriggerflowClient(**tf_config['triggerflow'], workspace=dagrun_id)

    event_sources = tf.list_eventsources()
    event_source_name = event_sources['event_sources'].pop()

    event_source = tf.get_eventsource(event_source_name)
    event_source_class = getattr(event_sources_mod, event_source[event_source_name]['class'])
    event_source_if = event_source_class(**event_source[event_source_name])
    event_source_if.publish_cloudevent({'source': 'tf_client', 'subject': 'init__', 'type': 'termination.event.success'})
    return 200, {'result': 'ok'}
