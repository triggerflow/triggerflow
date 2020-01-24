import numpy as np
import ibm_boto3
import pickle
import math
from types import SimpleNamespace


def main(args):

    def get_array(matrix, n):
        if matrix not in cache:
            cache[matrix] = {}

        if n not in cache[matrix]:
            bytes0 = n * (params.matrix_size // params.N)
            bytes1 = ((bytes0 + (params.matrix_size // params.N) *
                       params.chunk_matrix_dimension) % params.matrix_size) - 1

            obj = cos.get_object(Bucket=bucket, Key=matrix, Range='bytes={}-{}'.format(bytes0, bytes1))
            arrays = np.frombuffer(obj['Body'].read(), dtype=np.float64)
            arrays = arrays.reshape(len(arrays) // params.N, params.N)
            for i, array in enumerate(arrays):
                cache[matrix][i+n] = array

        return cache[matrix][n]

    print(args)

    cache = {}
    params = SimpleNamespace(**args)
    bucket = params.cos_credentials['bucket']
    results = {}

    cos = ibm_boto3.client('s3',
                           aws_access_key_id=params.cos_credentials['access_key'],
                           aws_secret_access_key=params.cos_credentials['secret_access_key'],
                           endpoint_url=params.cos_credentials['endpoint'])

    er, ec = params.element
    for row in range(params.chunk_matrix_dimension):
        row_array = get_array('A', er + row)
        for column in range(params.chunk_matrix_dimension):
            column_array = get_array('B', ec + column)

            # print("Iteration: {} * {}", row, column)
            res_array = np.multiply(row_array, column_array)
            res = np.sum(res_array)
            results[(row, column)] = res

    byte_data = pickle.dumps(results)
    cos.put_object(Bucket=bucket, Key='result_chunk{}'.format(params.chunk), Body=byte_data)

    return {'result': 'done'}
