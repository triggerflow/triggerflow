import json
import os
import numpy as np
import ibm_boto3
import shutil
import gc
import math

from dags.operators import CallAsyncOperator, MapOperator
from dags import DAG

N = 8192  # Matrix size
chunks = 256  # Number of chunks
step = (N * N) // chunks  # Number of elements to calculate per chunk
bucket = 'aitor-data'  # Bucket name to store temporary data
generate_matrices_flag = False  # Flag to generate new matrices
matrix_size = 8 * (N * N)  # sizeof(np.float64) * N*N
chunk_matrix_dimension = N // int(math.sqrt(chunks))  # Dimension of the chunk's submatrix

if ((N * N) % chunks) != 0:
    raise Exception('Unbalanced partitioning')


def generate_matrices():
    cos_credentials_path = os.path.expanduser('~/cos_credentials.json')
    with open(cos_credentials_path, 'r') as cos_credentials:
        cos_config = json.loads(cos_credentials.read())['cos_credentials']

    cos = ibm_boto3.client('s3',
                           aws_access_key_id=cos_config['access_key'],
                           aws_secret_access_key=cos_config['secret_access_key'],
                           endpoint_url=cos_config['endpoint'])

    matrix = np.random.rand(N, N).flatten()
    byte_data = matrix.tobytes()
    cos.put_object(Bucket=bucket, Key='A', Body=byte_data)

    matrix = None
    byte_data = None
    gc.collect()

    matrix = np.random.rand(N, N).transpose().flatten()
    byte_data = matrix.tobytes()
    cos.put_object(Bucket=bucket, Key='B', Body=byte_data)

    print('Done')


if generate_matrices_flag:
    generate_matrices()

pmm = DAG(dag_id='parallel_matrix_multiplication')

shutil.make_archive('multiply', 'zip', os.path.join(os.getcwd(),
                                                    'dags', 'examples', 'parallel_matrix_multiplication',
                                                    'ibm-cf_matrix_multiply'))

with open(os.path.join(os.getcwd(), 'multiply.zip'), 'rb') as zipf:
    codebin = zipf.read()

print(step)

parallel_multiplications = MapOperator(
    task_id='parallel_multiplications',
    function_name='matrix_multiply',
    function_package='eventprocessor_functions',
    function_memory=1024,
    function_timeout=120000,
    zipfile=codebin,
    iter_data=[{'chunk': x,
                'step': step,
                'N': N,
                'matrix_size': matrix_size,
                'element': ((x * chunk_matrix_dimension) % N,
                            ((x * chunk_matrix_dimension) // N) * chunk_matrix_dimension),
                'chunk_matrix_dimension': chunk_matrix_dimension} for x in range(chunks)],
    dag=pmm,
)

shutil.make_archive('join', 'zip', os.path.join(os.getcwd(),
                                                'dags', 'examples', 'parallel_matrix_multiplication',
                                                'ibm-cf_matrix_join'))

with open(os.path.join(os.getcwd(), 'join.zip'), 'rb') as zipf:
    codebin = zipf.read()

join_results = CallAsyncOperator(
    task_id='join_results',
    function_name='matrix_join',
    function_package='eventprocessor_functions',
    function_memory=2048,
    function_timeout=120000,
    zipfile=codebin,
    args={'chunks': chunks,
          'N': N},
    dag=pmm
)

# Define task dependencies
parallel_multiplications >> join_results
