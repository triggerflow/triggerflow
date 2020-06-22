import re


class BaseOperator:
    trigger_action_name = None

    def __init__(self, task_id: str, dag):
        __pattern = r"[a-zA-Z0-9_\-]+"
        if not re.fullmatch(__pattern, task_id):
            raise Exception("Task name \"{}\" does not match regex {}".format(task_id, __pattern))
        self.task_id = task_id
        self.dag = dag

        self.__upstream_relatives = set()
        self.__downstream_relatives = set()

        self.dag.add_task(self)

    @property
    def upstream_relatives(self):
        return list(self.__upstream_relatives)

    @property
    def downstream_relatives(self):
        return list(self.__downstream_relatives)

    def set_relatives(self, tasks, upstream=False):
        tasks = [tasks] if type(tasks) is not list else tasks
        for task in tasks:
            if upstream:
                self.__upstream_relatives.add(task)
            else:
                self.__downstream_relatives.add(task)

    def set_upstream(self, other):
        self.set_relatives(other, upstream=True)
        other = [other] if type(other) is not list else other
        for op in other:
            op.set_relatives(self, upstream=False)

    def set_downstream(self, other):
        self.set_relatives(other, upstream=False)
        other = [other] if type(other) is not list else other
        for op in other:
            op.set_relatives(self, upstream=True)

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

    def json_marshal(self):
        spec = dict()
        spec['task_id'] = self.task_id
        spec['upstream_relatives'] = [task.task_id for task in self.__upstream_relatives]
        spec['downstream_relatives'] = [task.task_id for task in self.__downstream_relatives]
        return spec

    def get_trigger_meta(self):
        raise NotImplementedError()
