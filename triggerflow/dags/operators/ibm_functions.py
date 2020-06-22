from typing import Union, Tuple, List

from ..models.baseoperator import BaseOperator
from ..hooks import IBMCloudFunctionsHook

deployed_functions = {}


class IBMCloudFunctionsOperator(BaseOperator):
    trigger_action_name = 'IBM_CF_INVOKE'

    def __init__(self, function_name: str, function_package: str = None, **kwargs):
        super().__init__(**kwargs)

        self.function_name = function_name
        self.function_package = function_package

        self.kind = None
        self.invoke_kwargs = {}
        self.iter_data = ()

        # Get IBM Cloud Functions Hook
        self.hook = IBMCloudFunctionsHook()
        self.connection = self.hook.get_conn()

        if self.function_package in [None, 'default', '_']:
            self.url = '/'.join([self.connection.endpoint, 'api', 'v1', 'namespaces',
                                 self.connection.namespace, 'actions', self.function_name])
        else:
            self.url = '/'.join([self.connection.endpoint, 'api', 'v1', 'namespaces',
                                 self.connection.namespace, 'actions', self.function_package, self.function_name])

        # Add event source to DAG event sources if it does not already exist
        if 'IBMCloudFunctions' not in self.dag.event_sources:
            self.dag.event_sources['IBMCloudFunctions'] = self.hook.get_event_source()

        # TODO Check if function exists

    def json_marshal(self):
        base_operator = super().json_marshal()
        spec = {
            'trigger_action': self.trigger_action_name,
            'class': self.__class__.__name__,
            'parameters': {
                'function_name': self.function_name,
                'function_package': self.function_package,
                'invoke_kwargs': self.invoke_kwargs,
            }
        }

        if self.iter_data:
            spec['parameters']['iter_data'] = {self.iter_data[0]: self.iter_data[1]}

        base_operator['operator'] = spec
        return base_operator

    def get_trigger_meta(self):
        if self.iter_data:
            iter_data_dict = {self.iter_data[0]: self.iter_data[1]}
        else:
            iter_data_dict = {}

        return {
            'url': self.url,
            'api_key': self.connection.api_key,
            'invoke_kwargs': self.invoke_kwargs,
            'iter_data': iter_data_dict,
            'sink': self.hook.get_event_source().get_json_eventsource()
        }


class IBMCloudFunctionsCallAsyncOperator(IBMCloudFunctionsOperator):
    def __init__(self, invoke_kwargs: dict = None, **kwargs):
        super().__init__(**kwargs)

        if invoke_kwargs is None:
            invoke_kwargs = {}
        if type(invoke_kwargs) is not dict:
            raise Exception('Parameter \'args\' must be dict')

        self.invoke_kwargs = invoke_kwargs


class IBMCloudFunctionsMapOperator(IBMCloudFunctionsOperator):
    def __init__(self, iter_data: Union[Tuple[str, List], dict], invoke_kwargs: dict = None, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if invoke_kwargs is None:
            invoke_kwargs = {}

        if isinstance(iter_data, dict):
            if len(iter_data) != 1:
                raise ValueError("\"iter_data\" as dict must contain a single entry as {string: iterable}")
            kwarg, it_data = list(iter_data.items()).pop()
            if not isinstance(kwarg, str):
                raise ValueError("\"iter_data\" as dict must contain a single entry as {string: iterable}")
            iter_data = (kwarg, it_data)
        elif not isinstance(iter_data, tuple):
            raise TypeError('Parameter \'iter_data\' must be a tuple as (kwarg, iterable)')

        try:
            kwarg, it_data = iter_data
            iter(it_data)  # Will raise TypeError if iter_data is not iterable
        except ValueError:
            raise TypeError('Parameter \'iter_data\' must be a tuple as (kwarg, iterable)')

        self.iter_data = iter_data
        self.invoke_kwargs = invoke_kwargs
