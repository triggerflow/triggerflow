import shutil
import os
import getpass
from dags.operators import CallAsyncOperator
from dags import DAG

my_dag = DAG(dag_id='example_dag')

task1 = CallAsyncOperator(
    task_id='run_this_first',
    function_name='hello',
    function_package='dag_test',
    function_memory=256,
    code="""
    import time
    def main(args):
        time.sleep(10)
        print("hello {}!".format(args['name']))
        return args
    """,
    args={'name': getpass.getuser()},
    dag=my_dag,
)

shutil.make_archive('func', 'zip', os.path.join(os.getcwd(), 'dags', 'examples', 'my_function'))

with open(os.path.join(os.getcwd(), 'func.zip'), 'rb') as zipf:
    codebin = zipf.read()

task2 = CallAsyncOperator(
    task_id='get_max',
    function_name='max',
    function_package='dag_test',
    function_memory=256,
    zipfile=codebin,
    args={'values': [1, 2, 3, 4, 5]},
    dag=my_dag
)

task3 = CallAsyncOperator(
    task_id='add_value',
    function_name='add',
    function_package='dag_test',
    function_memory=256,
    code="""
    def main(args):
        x = args['x']
        y = args['y']
        return {'result' : x + y}
    """,
    # args={'x' : 7, 'y' : task2.result['max']},
    args={'x': 7, 'y': 5},
    dag=my_dag
)

task1 >> task2 >> task3
