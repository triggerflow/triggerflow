import json
import os
import numpy as np
import ibm_boto3
import shutil

from dags.operators import CallAsyncOperator, MapOperator
from dags import DAG

N = 15000  # Matrix size
chunks = 1000  # Number of chunks
step = (N * N) // chunks  # Rows and columns per chunk
bucket = 'aitor-data'  # Bucket name to store temporary data
generate_matrices_flag = True  # Flag to generate new matrices
nbytes = 8 * (N * N)  # sizeof(float64) * N*N

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

    a = np.random.rand(N, N)
    byte_data = a.tobytes()
    cos.put_object(Bucket=bucket, Key='A', Body=byte_data)

    b = np.random.rand(N, N)
    b = np.transpose(b)
    byte_data = b.tobytes()
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
    zipfile=codebin,
    iter_data=[{'chunk': x,
                'step': step,
                'N': N,
                'nbytes': nbytes} for x in range(chunks)],
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
    zipfile=codebin,
    args={'chunks': chunks,
          'N': N},
    dag=pmm
)

# Define task dependencies
parallel_multiplications >> join_results
