import boto3

from ..models.basehook import BaseHook
from ...config import get_dag_operator_config
from ...eventsources import SQSEventSource


class AWSLambdaHookMeta(type):
    __instance = None

    def __call__(cls):
        if cls.__instance is None:
            cls.__instance = super().__call__()
        return cls.__instance


class AWSLambdaHook(BaseHook):
    def __init__(self):
        self.credentials = get_dag_operator_config('aws_lambda')

        if {'access_key_id', 'secret_access_key', 'region_name'} != set(self.credentials):
            raise Exception('Missing parameters for AWS Lambda Operator in config file')

        self.__client = boto3.client('lambda',
                                     region_name=self.credentials['region_name'],
                                     aws_access_key_id=self.credentials['access_key_id'],
                                     aws_secret_access_key=self.credentials['secret_access_key'])

        self.__event_source = SQSEventSource(**self.credentials)

    def get_conn(self):
        return self.__client

    def get_event_source(self):
        return self.__event_source
