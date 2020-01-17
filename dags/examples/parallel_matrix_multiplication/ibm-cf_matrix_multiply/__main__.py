import numpy as np
import ibm_boto3
import pickle
from types import SimpleNamespace


def main(args):
    print(args)

    params = SimpleNamespace(**args)

    bucket = params.cos_credentials['bucket']

    cos = ibm_boto3.client('s3',
                           aws_access_key_id=params.cos_credentials['access_key'],
                           aws_secret_access_key=params.cos_credentials['secret_access_key'],
                           endpoint_url=params.cos_credentials['endpoint'])

    results = {}
    rows_cache = {}
    columns_cache = {}

    for i in range(params.step):
        row = (i + (params.step * params.chunk)) // params.N

        if row not in rows_cache:
            bytes0 = row * (params.nbytes // params.N)
            bytes1 = (bytes0 + (params.nbytes // params.N)) - 1

            obj = cos.get_object(Bucket=bucket, Key='A', Range='bytes={}-{}'.format(bytes0, bytes1))
            row_array = np.frombuffer(obj['Body'].read(), dtype=np.float64)
            rows_cache[row] = row_array
        else:
            row_array = rows_cache[row]

        column = (i + (params.step * params.chunk)) % params.N
        if column not in columns_cache:
            bytes0 = column * (params.nbytes // params.N)
            bytes1 = (bytes0 + (params.nbytes // params.N)) - 1

            obj = cos.get_object(Bucket=bucket, Key='B', Range='bytes={}-{}'.format(bytes0, bytes1))
            column_array = np.frombuffer(obj['Body'].read(), dtype=np.float64)
            columns_cache[column] = column_array
        else:
            column_array = columns_cache[column]

        res_array = np.multiply(row_array, column_array)
        res = np.sum(res_array)
        results[(row, column)] = res

    byte_data = pickle.dumps(results)
    cos.put_object(Bucket=bucket, Key='result_chunk{}'.format(params.chunk), Body=byte_data)

    return {'result': 'done'}
