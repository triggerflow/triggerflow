from enum import Enum
from eventprocessor_client.conditions_actions import ConditionActionModel


class AwsAsfConditions(ConditionActionModel, Enum):
    AWS_ASF_JOIN_STATEMACHINE = {'name': 'aws_asf_join_statemachine'}
    AWS_ASF_CONDITION = {'name': 'aws_asf_condition'}


class AwsAsfActions(ConditionActionModel, Enum):
    AWS_ASF_PASS = {'name': 'aws_asf_pass'}
    AWS_ASF_TASK = {'name': 'aws_asf_task'}
    AWS_ASF_CHOICE = {'name': 'aws_asf_choice'}
    AWS_ASF_PARALLEL = {'name': 'aws_asf_parallel'}
    AWS_ASF_MAP = {'name': 'aws_asf_map'}
    AWS_ASF_END_STATEMACHINE = {'name': 'aws_asf_end_statemachine'}
