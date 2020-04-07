from eventprocessor.client.dag.operators import AWSLambdaCallAsyncOperator
from eventprocessor.client.dag.utils.helpers import chain
from eventprocessor.client.dag import DAG

dag = DAG(dag_id='sequence',
          event_source='lambda_destination')

sequence_length = 1

tasks = []

for i in range(sequence_length):
    task = AWSLambdaCallAsyncOperator(
        task_id='task_{}'.format(i),
        function_name='sleep3',
        dag=dag,
    )
    tasks.append(task)

chain(*tasks)
