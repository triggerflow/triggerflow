from composer.operators import CallAsyncOperator, MapOperator
from composer import DAG

map_dag = DAG(dag_id='one_map')

my_map = MapOperator(
    task_id='my_map',
    function_name='add',
    function_package='dag_test',
    function_memory=256,
    code="""
    def main(args):
        x = args['x']
        return {'result' : x + 1}
    """,
    iter_data=[{'x' : x} for x in range(5)],
    dag=map_dag,
)

