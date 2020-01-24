from dags.operators import CallAsyncOperator
from dags.utils.helpers import chain
from dags import DAG

dag = DAG(dag_id='sequence')

sequence_length = 80


tasks = []

for i in range(sequence_length):
    task = CallAsyncOperator(
        task_id='task_{}'.format(i),
        function_name='sleep3',
        function_package='triggers-experiments',
        function_memory=128,
        code="""
        import time
        def main(args):
            time.sleep(3)
            return {'result': 'I slept for 3 seconds'}
        """,
        dag=dag,
    )
    tasks.append(task)

chain(*tasks)
