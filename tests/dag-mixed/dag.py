from triggerflow.dags import DAG
from triggerflow.dags.operators import (
    IBMCloudFunctionsCallAsyncOperator,
    IBMCloudFunctionsMapOperator
)

dag = DAG(dag_id='mixed')

first_task = IBMCloudFunctionsCallAsyncOperator(
    task_id='first_task',
    function_name='echo',
    function_package='triggerflow-tests',
    invoke_kwargs={'sleep': 3},
    dag=dag,
)

branch1_callasync = IBMCloudFunctionsCallAsyncOperator(
    task_id='branch1_callasync_task',
    function_name='echo',
    function_package='triggerflow-tests',
    invoke_kwargs={'sleep': 15},
    dag=dag,
)

branch1_map = IBMCloudFunctionsMapOperator(
    task_id='branch1_map_task',
    function_name='echo',
    function_package='triggerflow-tests',
    invoke_kwargs={'sleep': 1},
    iter_data=('echo', [x for x in range(10)]),
    dag=dag
)

branch2_callasync = IBMCloudFunctionsCallAsyncOperator(
    task_id='branch2_callasync_task',
    function_name='echo',
    function_package='triggerflow-tests',
    invoke_kwargs={'sleep': 1},
    dag=dag,
)

branch2_map = IBMCloudFunctionsMapOperator(
    task_id='branch2_map_task',
    function_name='echo',
    function_package='triggerflow-tests',
    invoke_kwargs={'sleep': 1},
    iter_data=('echo', [x for x in range(10)]),
    dag=dag
)

final_task = IBMCloudFunctionsMapOperator(
    task_id='final_task',
    function_name='echo',
    function_package='triggerflow-tests',
    invoke_kwargs={'sleep': 1},
    iter_data=('echo', [x for x in range(5)]),
    dag=dag
)

first_task >> [branch1_callasync, branch2_map]
branch1_callasync >> branch1_map
branch2_map >> branch2_callasync
final_task << [branch1_map, branch2_callasync]
