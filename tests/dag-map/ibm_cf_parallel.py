from triggerflow.dags import DAG
from triggerflow.dags.operators import IBMCloudFunctionsMapOperator

dag = DAG(dag_id='map')

concurrency = 320

task = IBMCloudFunctionsMapOperator(
    task_id='map',
    function_name='sleep',
    function_package='triggerflow-tests',
    iter_data=('sleep', [5 for _ in range(concurrency)]),
    dag=dag,
)
