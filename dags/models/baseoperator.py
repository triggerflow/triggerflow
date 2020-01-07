import re
from dags.dag import DAG


class BaseOperator:
    def __init__(self, task_id: str = None, dag: DAG = None):
        __pattern = '[a-zA-Z0-9_\-]+'  # pylint: disable=anomalous-backslash-in-string
        if not re.fullmatch(__pattern, task_id):
            raise Exception("Task name \"{}\" does not match regex {}".format(task_id, __pattern))
        self.task_id = task_id
        self._upstream_relatives = set()
        self._downstream_relatives = set()

        dag.tasks.add(self)

    @property
    def result(self):
        # TODO return result of operator
        pass

    def _set_relatives(self, tasks, upstream=False):
        tasks = [tasks] if type(tasks) is not list else tasks
        for task in tasks:
            if upstream:
                self._upstream_relatives.add(task)
            else:
                self._downstream_relatives.add(task)

    def set_upstream(self, other):
        self._set_relatives(other, upstream=True)
        other = [other] if type(other) is not list else other
        for op in other:
            op._set_relatives(self, upstream=False)

    def set_downstream(self, other):
        self._set_relatives(other, upstream=False)
        other = [other] if type(other) is not list else other
        for op in other:
            op._set_relatives(self, upstream=True)

    def __lshift__(self, other):
        self.set_upstream(other)
        return self

    def __rshift__(self, other):
        self.set_downstream(other)
        return other

    def rrshift(self, other):
        self.__lshift__(other)
        return self

    def __rlshift__(self, other):
        self.__rshift__(other)
        return self

    def to_json(self):
        spec = dict()
        spec['task_id'] = self.task_id
        spec['upstream_relatives'] = [task.task_id for task in self._upstream_relatives]
        spec['downstream_relatives'] = [task.task_id for task in self._downstream_relatives]
        return spec
