from eventprocessor.client.dag.operators import AWSLambdaMapOperator
from eventprocessor.client.dag import DAG

dag = DAG(dag_id='parallel',
          event_source='lambda_destination')

concurrency = 320

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

task = AWSLambdaMapOperator(
    task_id='my_map',
    function_name='sleep3',
    iter_data=[{x: x} for x in range(concurrency)],
    dag=dag,
)
