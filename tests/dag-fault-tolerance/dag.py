from triggerflow.dags import DAG
from triggerflow.dags.operators import (
    IBMCloudFunctionsCallAsyncOperator,
    IBMCloudFunctionsMapOperator
)

dag = DAG(dag_id='fault-tolerance')

first_task = IBMCloudFunctionsCallAsyncOperator(
    task_id='first_task',
    function_name='echo',
    function_package='triggerflow-tests',
    invoke_kwargs={'sleep': 1},
    dag=dag,
)

branch1_map = IBMCloudFunctionsMapOperator(
    task_id='branch1_map_task',
    function_name='echo',
    function_package='triggerflow-tests',
    invoke_kwargs={'sleep': 2},
    iter_data=('echo', [x for x in range(10)]),
    dag=dag
)

branch2_map = IBMCloudFunctionsMapOperator(
    task_id='branch2_map_task',
    function_name='echo',
    function_package='triggerflow-tests',
    iter_data=('sleep', [2**x for x in range(5)]),
    dag=dag
)

first_task >> [branch1_map, branch2_map]
