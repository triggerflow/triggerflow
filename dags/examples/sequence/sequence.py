from dags.operators import IBMCloudFunctionsCallAsyncOperator
from dags.utils.helpers import chain
from dags import DAG

dag = DAG(dag_id='sequence',
          event_source='kafka')

sequence_length = 20


tasks = []

for i in range(sequence_length):
    task = IBMCloudFunctionsCallAsyncOperator(
        task_id='task_{}'.format(i),
        function_name='sleep3',
        function_package='triggers-experiments',
        function_memory=128,
        code="""
        import time
        def main(args):
            print(time.time)
            time.sleep(3)
            print(time.time)
            return {'result': 'I slept for 3 seconds'}
        """,
        dag=dag,
        overwrite=True
    )
    tasks.append(task)

chain(*tasks)
