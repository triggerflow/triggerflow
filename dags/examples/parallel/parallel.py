from dags.operators import CallAsyncOperator, MapOperator
from dags import DAG

dag = DAG(dag_id='parallel')

concurrency = 80

# for i in range(concurrency):
#     task = CallAsyncOperator(
#         task_id='task_{}'.format(i),
#         function_name='sleep3',
#         function_package='triggers-experiments',
#         function_memory=128,
#         code="""
#         import time
#         def main(args):
#             time.sleep(3)
#             return {'result': 'I slept for 3 seconds'}
#         """,
#         overwrite=False,
#         dag=dag,
#     )

task = MapOperator(
    task_id='my_map',
    function_name='sleep3',
    function_package='triggers-experiments',
    function_memory=128,
    iter_data=[{x: x} for x in range(concurrency)],
    dag=dag,
)
