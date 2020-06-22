from triggerflow.dags import DAG
from triggerflow.dags.operators import IBMCloudFunctionsCallAsyncOperator
from triggerflow.dags.other.helpers import chain

dag = DAG(dag_id='sequence')

sequence_length = 40

tasks = []

for i in range(sequence_length):
    task = IBMCloudFunctionsCallAsyncOperator(
        task_id=str(i),
        function_name='sleep',
        function_package='triggerflow-tests',
        invoke_kwargs={'sleep': 5},
        dag=dag,
    )
    tasks.append(task)

chain(*tasks)
