from dags.models.basehook import BaseHook
from dags.hooks.utils import load_conf

import boto3

MANDATORY_CREDENTIALS = {'access_key_id', 'secret_access_key'}


class AWSLambdaHook(BaseHook):
    def __init__(self):
        self.config = load_conf('operators')['aws_lambda']

        if not set(self.config).issubset(MANDATORY_CREDENTIALS):
            raise Exception('Missing mandatory credentials in config file under "aws_lambda"')

        self.client = boto3.client('lambda',
                                   aws_access_key_id=self.config['access_key_id'],
                                   aws_secret_access_key=self.config['secret_access_key'])

    def get_conn(self):
        return self.client
