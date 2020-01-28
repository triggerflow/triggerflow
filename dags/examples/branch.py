from dags.operators import IBMCloudFunctionsCallAsyncOperator, IBMCloudFunctionsMapOperator
from dags import DAG

map_dag = DAG(dag_id='branch')

random = IBMCloudFunctionsCallAsyncOperator(
    task_id='random',
    function_name='do_nothing',
    function_package='dag_test',
    function_memory=256,
    code="""
    def main(args):
        return {'result' : 'I did nothing'}
    """,
    args={},
    dag=map_dag
)

map1 = IBMCloudFunctionsMapOperator(
    task_id='map1',
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

map2 = IBMCloudFunctionsMapOperator(
    task_id='map2',
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

join = IBMCloudFunctionsCallAsyncOperator(
    task_id='map_join',
    function_name='do_nothing',
    function_package='dag_test',
    function_memory=256,
    code="""
    def main(args):
        return {'result' : 'I did nothing'}
    """,
    args={},
    dag=map_dag
)

join << [map1, map2]

