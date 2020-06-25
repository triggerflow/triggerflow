import re
import json

from typing import Optional, List, Dict

from . import operators
from .models.baseoperator import BaseOperator
from .dagrun import DAGRun
from ..cache import TriggerflowCache
from .other.notebook import display_graph


class DAG:
    def __init__(self, dag_id):
        """
        Initialize an empty DAG object
        :param dag_id: DAG identifier (can only contain alphanumeric characters, hyphens and underscores)
        """
        # Check DAG id
        pattern = r"[a-zA-Z0-9_\-]+"
        if not re.fullmatch(pattern, dag_id):
            raise Exception("Dag id \"{}\" does not match pattern {}".format(dag_id, pattern))

        self.dag_id = dag_id

        self.event_sources = {}
        self.__tasks = set()

    @property
    def initial_tasks(self):
        return [task for task in self.__tasks if not task.upstream_relatives]

    @property
    def final_tasks(self):
        return [task for task in self.__tasks if not task.downstream_relatives]

    @property
    def tasks(self) -> List[BaseOperator]:
        return list(self.__tasks)

    @property
    def tasks_dict(self) -> Dict[str, BaseOperator]:
        return {task.task_id: task for task in self.__tasks}

    def add_task(self, task):
        if task.task_id in [task.task_id for task in self.tasks]:
            raise Exception('A task with ID {} is already registered in the dag'.format(task.task_id))
        self.__tasks.add(task)

    def run(self):
        if self.dag_id is None:
            raise Exception('DAG id must be set')
        return DAGRun.from_dag_def(self).run()

    def show(self):
        return display_graph(self)

    def json_marshal(self):
        return {
            'dag_id': self.dag_id,
            'event_sources': [event_source.get_json_eventsource() for event_source in self.event_sources.values()],
            'initial_tasks': [task.task_id for task in self.initial_tasks],
            'final_tasks': [task.task_id for task in self.final_tasks],
            'tasks': {task.task_id: task.json_marshal() for task in self.__tasks}
        }

    def json_unmarshal(self, json_dag):
        # Instantiate all operator objects
        tasks = {}
        for task_name, task_json in json_dag['tasks'].items():
            operator_class = getattr(operators, task_json['operator']['class'])
            tasks[task_name] = operator_class(task_id=task_name, dag=self, **task_json['operator']['parameters'])

        # Set up dependencies
        for task_name, task_json in json_dag['tasks'].items():
            task_obj = tasks[task_name]
            upstream_relatives = [tasks[up_rel] for up_rel in task_json['upstream_relatives']]
            if upstream_relatives:
                task_obj.set_upstream(upstream_relatives)
            downstream_relatives = [tasks[down_rel] for down_rel in task_json['downstream_relatives']]
            if downstream_relatives:
                task_obj.set_downstream(downstream_relatives)

    def save(self):
        with TriggerflowCache(path='dags', file_name=self.dag_id + '.json', method='w') as dag_file:
            dag_file.write(json.dumps(self.json_marshal(), indent=4))

    def load(self):
        with TriggerflowCache(path='dags', file_name=self.dag_id + '.json', method='r') as dag_file:
            dag_json = json.loads(dag_file.read())
        self.json_unmarshal(dag_json)

    def export_to_json(self, dest_path: Optional[str] = '.', dest_file: Optional[str] = None):
        if dest_file is None:
            dest_file = self.dag_id
        if not dest_file.endswith('.json'):
            dest_file += '.json'

        dag_json = self.json_marshal()
        path = '/'.join([dest_path, dest_file])

        with open(path, 'w') as dag_file:
            dag_file.write(json.dumps(dag_json, indent=4))
        return self

    @classmethod
    def import_from_json(cls, src_file: str):
        with open(src_file, 'r') as dag_file:
            dag_json = json.loads(dag_file.read())
        dag_obj = cls(dag_json['dag_id'])
        dag_obj.json_unmarshal(dag_json)
        return dag_obj
