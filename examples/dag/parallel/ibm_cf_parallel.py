from eventprocessor.client.dag.operators import IBMCloudFunctionsMapOperator
from eventprocessor.client.dag import DAG

dag = DAG(dag_id='parallel',
          event_source='redis')

concurrency = 320

task = IBMCloudFunctionsMapOperator(
    task_id='my_map',
    function_name='sleep5',
    function_package='eventprocessor-experiments',
    iter_data=[{x: x} for x in range(concurrency)],
    dag=dag,
)
