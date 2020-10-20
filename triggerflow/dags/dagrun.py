import pickle
import json
from uuid import uuid4
from datetime import datetime
from platform import node
from enum import Enum

from ..cache import TriggerflowCache
from ..client import (
    TriggerflowClient,
    TriggerflowCachedClient,
    DefaultActions,
    DefaultConditions
)
from triggerflow import CloudEvent


class DAGRun:
    class State(Enum):
        EMPTY = 'EMPTY'
        INITIALIZED = 'INITIALIZED'
        DEPLOYED = 'DEPLOYED'
        RUNNING = 'RUNNING'
        FINISHED = 'FINISHED'
        ERROR = 'ERROR'

    def __init__(self):
        self.uuid = None
        self.dagrun_id = None
        self.start_time = None
        self.dag_id = None
        self.dag = None
        self.state = DAGRun.State.EMPTY

    @classmethod
    def from_dag_def(cls, dag_def: 'DAG'):
        dagrun = cls()
        dagrun.uuid = str(uuid4())
        dagrun.dagrun_id = '-'.join([dag_def.dag_id, dagrun.uuid[24:]])
        dagrun.start_time = datetime.utcnow().isoformat()
        dagrun.dag_id = dag_def.dag_id
        dagrun.dag = dag_def
        dagrun.state = DAGRun.State.INITIALIZED
        dagrun.__save_cache()
        return dagrun

    @classmethod
    def load_run(cls, dagrun_id: str):
        dagrun = cls()
        try:
            with TriggerflowCache(path='dag-runs', file_name=dagrun_id + '.json', method='r') as dagrun_file:
                metadata = json.loads(dagrun_file.read())
            with TriggerflowCache(path='dag-runs', file_name=dagrun_id + '.pickle', method='rb') as dag_file:
                dagrun.dag = pickle.load(dag_file)
        except FileNotFoundError:
            raise Exception('Dag run not found in cache')

        dagrun.uuid = metadata['uuid']
        dagrun.dagrun_id = metadata['dagrun_id']
        dagrun.start_time = metadata['start_time']
        dagrun.dag_id = metadata['dag_id']
        dagrun.state = DAGRun.State[metadata['state']]
        return dagrun

    def run(self, silent=False):
        if self.state == DAGRun.State.RUNNING:
            raise Exception('DAG already running')

        self.__create_triggers()
        self.__trigger(silent)
        self.__save_cache()

        return self

    def result(self, task=None):
        tf = TriggerflowClient()
        tf.target_workspace(self.dagrun_id)
        if task is None:
            return tf.get_trigger('__end__')['context']['result']
        else:
            if task in self.dag.tasks_dict:
                task_op = self.dag.tasks_dict[task]
                results = []
                for downstream in task_op.downstream_relatives:
                    trg = tf.get_trigger(downstream.task_id)
                    results.append(trg['result'])

    def __save_cache(self):
        with TriggerflowCache(path='dag-runs', file_name=self.dagrun_id + '.json', method='w') as dagrun_file:
            metadata = {
                'uuid': self.uuid,
                'dagrun_id': self.dagrun_id,
                'start_time': self.start_time,
                'dag_id': self.dag_id,
                'state': self.state.name
            }
            dagrun_file.write(json.dumps(metadata, indent=4))
        with TriggerflowCache(path='dag-runs', file_name=self.dagrun_id + '.pickle', method='wb') as dag_file:
            pickle.dump(self.dag, dag_file)

    def __trigger(self, silent=False):
        event_source = list(self.dag.event_sources.values()).pop()
        uuid = uuid4()
        init_cloudevent = (
            CloudEvent()
            .SetSubject('__init__')
            .SetEventType('event.triggerflow.init')
            .SetEventID(uuid.hex)
            .SetSource(f'urn:{node()}:{str(uuid)}')
        )
        event_source.set_stream(self.dagrun_id)
        event_source.publish_cloudevent(init_cloudevent)
        self.state = DAGRun.State.RUNNING
        if not silent:
            print('DAG Run ID: {}'.format(self.dagrun_id))
        return self

    def __create_triggers(self):
        tf = TriggerflowCachedClient()

        # Create unique workspace for this specific dag run and its event sources
        event_sources = list(self.dag.event_sources.values())
        # Set current DAGRun ID as topic/queue name for the event sources
        [event_source.set_stream(self.dagrun_id) for event_source in event_sources]
        tf.create_workspace(workspace_name=self.dagrun_id, event_source=event_sources.pop(), global_context={})
        for event_source in event_sources:
            tf.add_event_source(event_source)

        for task in self.dag.tasks:
            context = {'subject': task.task_id,
                       'dependencies': {},
                       'operator': task.get_trigger_meta(),
                       'result': []}

            # If this task does not have upstream relatives, then it will be executed when the sentinel event __init__
            # is produced, else, it will be executed every time one of its upstream relatives produces its term. event
            if not task.upstream_relatives:
                condition = DefaultConditions.TRUE  # Initial task do not have dependencies
                activation_event = CloudEvent().SetSubject('__init__').SetEventType('event.triggerflow.init')
                act_events = [activation_event]
            else:
                condition = DefaultConditions.DAG_TASK_JOIN
                act_events = []
                for upstream_relative in task.upstream_relatives:
                    context['dependencies'][upstream_relative.task_id] = {'join': -1, 'counter': 0}
                    activation_event = (
                        CloudEvent()
                        .SetSubject(upstream_relative.task_id)
                        .SetEventType('event.triggerflow.termination.success')
                    )
                    act_events.append(activation_event)

            # Add a trigger that handles this task execution: It will be fired every time one of its upstream
            # relatives sends its termination event, but it is executed only when all dependencies are fulfilled
            tf.add_trigger(event=act_events,
                           action=DefaultActions[task.trigger_action_name],
                           condition=condition,
                           context=context,
                           trigger_id=task.task_id,
                           transient=False)

        # Join final tasks (those that do not have downstream relatives)
        context = {'subject': '__end__',
                   'dependencies': {final_task.task_id: {'join': -1, 'counter': 0} for final_task in
                                    self.dag.final_tasks},
                   'result': []}
        activation_events = [(CloudEvent()
                              .SetSubject(final_task.task_id)
                              .SetEventType('event.triggerflow.termination.success'))
                             for final_task in self.dag.final_tasks]
        tf.add_trigger(event=activation_events,
                       action=DefaultActions.TERMINATE,
                       condition=DefaultConditions.DAG_TASK_JOIN,
                       context=context,
                       trigger_id='__end__',
                       transient=False)

        # Add error handling trigger: All tasks that produce a failure event type will fire this trigger
        # activation_event = v1.Event().SetSubject('*').SetEventType('event.triggerflow.termination.failure')
        # tf.add_trigger(event=activation_event,
        #                action=DefaultActions.DAG_TASK_FAILURE_HANDLER,
        #                condition=DefaultConditions.TRUE,
        #                context={},
        #                context_parser='DAGS',
        #                trigger_id='__error_handler__',
        #                transient=False)
        #
        # # Add retry handler trigger: We will use this trigger to manually fire it to retry any failed task
        # activation_event = v1.Event().SetSubject('__retry__').SetEventType('event.triggerflow.termination.failure')
        # tf.add_trigger(event=activation_event,
        #                action=DefaultActions.DAG_TASK_RETRY_HANDLER,
        #                condition=DefaultConditions.TRUE,
        #                context={},
        #                context_parser='DAGS',
        #                trigger_id='__retry_handler__',
        #                transient=False)

        tf.commit_cached_triggers()
        self.state = DAGRun.State.DEPLOYED

