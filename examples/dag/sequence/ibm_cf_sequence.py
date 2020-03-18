from dags.operators import IBMCloudFunctionsCallAsyncOperator
from dags.utils.helpers import chain
from dags import DAG

dag = DAG(dag_id='sequence',
          event_source='redis')

sequence_length = 80

tasks = []

for i in range(sequence_length):
    task = IBMCloudFunctionsCallAsyncOperator(
        task_id=str(i),
        function_name='sleep5',
        function_package='triggerflow-experiments',
        args={i: i},
        dag=dag,
    )
    tasks.append(task)

chain(*tasks)
