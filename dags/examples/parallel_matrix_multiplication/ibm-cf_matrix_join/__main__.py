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

    result = np.zeros([params.N, params.N], dtype=np.float32)

    for i in range(params.chunks):
        obj = cos.get_object(Bucket=bucket, Key='result_chunk{}'.format(i))
        data = pickle.loads(obj['Body'].read())
        for elem, value in data.items():
            result[elem[0], elem[1]] = value

    bytes_result = result.tobytes()
    cos.put_object(Bucket=bucket, Key='final_result', Body=bytes_result)

    print(result)

    return {'result': 'done'}
