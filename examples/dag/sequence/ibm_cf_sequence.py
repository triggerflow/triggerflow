from triggerflow.client.dag.operators import IBMCloudFunctionsCallAsyncOperator
from triggerflow.client.dag.utils.helpers import chain
from triggerflow.client.dag import DAG

dag = DAG(dag_id='sequence',
          event_source='redis')

sequence_length = 80

tasks = []

for i in range(sequence_length):
    task = IBMCloudFunctionsCallAsyncOperator(
        task_id=str(i),
        function_name='sleep',
        args={'sleep': 3, 'data': i, 'id': i},
        dag=dag,
    )
    tasks.append(task)

chain(*tasks)
