from triggerflow.dags import DAG
from triggerflow.dags.operators import (
    IBMCloudFunctionsCallAsyncOperator,
    IBMCloudFunctionsMapOperator
)

# DAG Parameters
SIMULATIONS = 15
LOOPS = 50

# Instantiate the DAG object
dag = DAG(dag_id='PiEstimationMontecarlo')

# TASKS
sim = IBMCloudFunctionsMapOperator(
    task_id='MontecarloSimulation',
    function_name='pi_montecarlo',
    function_package='triggerflow_examples',
    invoke_kwargs={'loops': LOOPS},
    iter_data=('n', [n for n in range(SIMULATIONS)]),
    dag=dag
)

avg = IBMCloudFunctionsCallAsyncOperator(
    task_id='Average',
    function_name='array_average',
    function_package='triggerflow_examples',
    invoke_kwargs={'array': [3.14 for _ in range(SIMULATIONS)]},    # NOTE: Passing values between tasks is not currently supported :(
    dag=dag
)

# DEPENDENCIES
sim >> avg
