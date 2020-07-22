import base64
import inspect
import logging

import cloudpickle
import sys
import pickle
from base64 import b64encode
from enum import Enum
from typing import List

log = logging.getLogger(__name__)


class ConditionActionModel:
    pass


class DefaultConditions(ConditionActionModel, Enum):
    TRUE = {'name': 'TRUE'}
    JOIN = {'name': 'JOIN'}
    FUNCTION_JOIN = {'name': 'FUNCTION_JOIN'}
    DAG_TASK_JOIN = {'name': 'DAG_TASK_JOIN'}
    COUNTER_THRESHOLD = {'name': 'COUNTER_THRESHOLD'}


class DefaultActions(ConditionActionModel, Enum):
    PASS = {'name': 'PASS'}
    TERMINATE = {'name': 'TERMINATE'}
    DAG_DUMMY_TASK = {'name': 'DAG_DUMMY_TASK'}
    DAG_TASK_FAILURE_HANDLER = {'name': 'DAG_TASK_FAILURE_HANDLER'}
    DAG_TASK_RETRY_HANDLER = {'name': 'DAG_TASK_RETRY_HANDLER'}
    THROTTLE_IBMCF_FUNCTION = {'name': 'THROTTLE_IBMCF_FUNCTION'}
    IBM_CF_INVOKE = {'name': 'IBM_CF_INVOKE'}
    AWS_LAMBDA_INVOKE = {'name': 'AWS_LAMBDA_INVOKE'}


class DockerImage(ConditionActionModel):
    def __init__(self, image: str, class_name: str):
        self.value = {'name': 'DOCKER_IMAGE',
                      'image': image,
                      'class_name': class_name}


class PythonCallable(ConditionActionModel):
    def __init__(self, function: callable, modules_to_capture: List[str] = None):

        try:
            assert inspect.isfunction(function)
            assert set(inspect.signature(function).parameters.keys()).issubset({'context', 'event'})
        except AssertionError:
            raise Exception('Function must be a callable and fulfill signature (context, event)')

        if modules_to_capture is None:
            modules_to_capture = [function.__module__]

        old_modules = {}
        try:  # Try is needed to restore the state if something goes wrong
            for module_name in modules_to_capture:
                if module_name in sys.modules:
                    old_modules[module_name] = sys.modules.pop(module_name)
            func_pickle = cloudpickle.dumps(function, pickle.DEFAULT_PROTOCOL)
        finally:
            sys.modules.update(old_modules)

        encoded_callable = base64.b64encode(func_pickle).decode('utf-8')

        self.value = {'name': 'PYTHON_CALLABLE',
                      'callable': encoded_callable}


def python_object(obj: object):
    dump = cloudpickle.dumps(obj)
    encoded = b64encode(dump).decode('utf-8')
    return {'__object__': encoded}
