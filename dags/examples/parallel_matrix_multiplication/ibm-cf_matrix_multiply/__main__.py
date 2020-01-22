import numpy as np
import ibm_boto3
import pickle
from types import SimpleNamespace

lookahead = 50


def main(args):

    def get_array(matrix, n):
        if matrix not in cache:
            cache[matrix] = {}

        if n not in cache[matrix]:
            bytes0 = n * (params.nbytes // params.N)
            bytes1 = ((bytes0 + (params.nbytes // params.N) * lookahead) % params.nbytes) - 1

            obj = cos.get_object(Bucket=bucket, Key='A', Range='bytes={}-{}'.format(bytes0, bytes1))
            arrays = np.frombuffer(obj['Body'].read(), dtype=np.float64)
            arrays = arrays.reshape(len(arrays) % params.N, params.N)
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

    for i in range(params.step):
        row = (i + (params.step * params.chunk)) // params.N
        row_array = get_array('A', row)
        column = (i + (params.step * params.chunk)) % params.N
        column_array = get_array('B', column)

        # print("Iteration {}: {} * {}", i, row, column)
        res_array = np.multiply(row_array, column_array)
        res = np.sum(res_array)
        results[(row, column)] = res

    byte_data = pickle.dumps(results)
    cos.put_object(Bucket=bucket, Key='result_chunk{}'.format(params.chunk), Body=byte_data)

    return {'result': 'done'}
