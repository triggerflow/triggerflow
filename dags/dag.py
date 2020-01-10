import re


class DAG:
    def __init__(self, dag_id: str):
        __pattern = "[a-zA-Z0-9_\-]+"  # pylint: disable=anomalous-backslash-in-string
        if not re.fullmatch(__pattern, dag_id):
            raise Exception("Dag id \"{}\" does not match pattern {}".format(dag_id, __pattern))
        self.dag_id = dag_id
        self.tasks = set()

    @property
    def initial_tasks(self):
        tasks = list(filter(
            lambda operator: not bool(operator._upstream_relatives),
            self.tasks))

        return tasks

    @property
    def final_tasks(self):
        tasks = list(filter(
            lambda operator: not bool(operator._downstream_relatives),
            self.tasks))

        return tasks

    def to_dict(self):
        spec = dict()
        spec['dag_id'] = self.dag_id
        spec['initial_tasks'] = [task.task_id for task in self.initial_tasks]
        spec['final_tasks'] = [task.task_id for task in self.final_tasks]
        spec['tasks'] = {}
        for task in self.tasks:
            spec['tasks'][task.task_id] = task.to_dict()
        return spec
