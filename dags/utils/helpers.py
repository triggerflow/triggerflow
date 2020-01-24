
# https://github.com/apache/airflow/blob/master/airflow/utils/helpers.py


def chain(*tasks):
    """
    Given a number of tasks, builds a dependency chain.

    chain(task_1, task_2, task_3, task_4)

    is equivalent to

    task_1.set_downstream(task_2)
    task_2.set_downstream(task_3)
    task_3.set_downstream(task_4)
    """
    for up_task, down_task in zip(tasks[:-1], tasks[1:]):
        up_task.set_downstream(down_task)


def cross_downstream(from_tasks, to_tasks):
    r"""
    Set downstream dependencies for all tasks in from_tasks to all tasks in to_tasks.
    E.g.: cross_downstream(from_tasks=[t1, t2, t3], to_tasks=[t4, t5, t6])
    Is equivalent to:

    t1 --> t4
       \ /
    t2 -X> t5
       / \
    t3 --> t6

    t1.set_downstream(t4)
    t1.set_downstream(t5)
    t1.set_downstream(t6)
    t2.set_downstream(t4)
    t2.set_downstream(t5)
    t2.set_downstream(t6)
    t3.set_downstream(t4)
    t3.set_downstream(t5)
    t3.set_downstream(t6)

    :param from_tasks: List of tasks to start from.
    :type from_tasks: List[airflow.models.BaseOperator]
    :param to_tasks: List of tasks to set as downstream dependencies.
    :type to_tasks: List[airflow.models.BaseOperator]
    """
    for task in from_tasks:
        task.set_downstream(to_tasks)
