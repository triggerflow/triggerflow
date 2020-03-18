from dags.operators import AWSLambdaCallAsyncOperator
from dags.utils.helpers import chain
from dags import DAG

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
