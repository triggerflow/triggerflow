from enum import Enum


class AwsAsfConditions(Enum):
    AWS_ASF_CONDITION = {'name': 'aws_asf_condition'}


class AwsAsfActions(Enum):
    AWS_ASF_PASS = {'name': 'aws_asf_pass'}
    AWS_ASF_TASK = {'name': 'aws_asf_task'}
    AWS_ASF_CHOICE = {'name': 'aws_asf_choice'}
    AWS_ASF_PARALLEL = {'name': 'aws_asf_parallel'}
