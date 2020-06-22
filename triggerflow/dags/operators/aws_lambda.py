from ..models.baseoperator import BaseOperator
from ..hooks import AWSLambdaHook


class AWSLambdaOperator(BaseOperator):
    trigger_action_name = 'AWS_LAMBDA_INVOKE'

    def __init__(self, function_arn: str, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.function_arn = function_arn
        self.invoke_kwargs = {}
        self.iter_data = ()

        self.hook = AWSLambdaHook()

        # Add event source to DAG event sources if it does not already exist
        if 'AWSLambda' not in self.dag.event_sources:
            self.dag.event_sources['AWSLambda'] = self.hook.get_event_source()

        # Check if function exists -- 'get_function' will raise exception if it does not
        self.hook.get_conn().get_function(FunctionArn=self.function_arn)

    def json_marshal(self):
        base_operator = super().json_marshal()
        spec = {
            'trigger_action': self.trigger_action_name,
            'class': self.__class__.__name__,
            'parameters': {
                'function_arn': self.function_arn,
                'invoke_kwargs': self.invoke_kwargs,
            }
        }

        if self.iter_data:
            spec['parameters']['iter_data'] = self.iter_data

        base_operator['operator'] = spec
        return base_operator

    def get_trigger_meta(self):
        meta = {}.update(self.hook.credentials)
        return meta


class AWSLambdaCallAsyncOperator(AWSLambdaOperator):
    def __init__(self, invoke_kwargs: dict = None, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if invoke_kwargs is None:
            invoke_kwargs = {}
        if type(invoke_kwargs) != dict:
            raise Exception('Parameters \'invoke_kwargs\' must be dict')

        self.invoke_kwargs = invoke_kwargs


class AWSLambdaMapOperator(AWSLambdaOperator):
    def __init__(self, iter_data: tuple, invoke_kwargs: dict = None, **kwargs):
        super().__init__(**kwargs)

        if invoke_kwargs is None:
            invoke_kwargs = {}
        try:
            kwarg, it_data = iter_data
            iter(it_data)  # Will raise TypeError if iter_data is not iterable
        except ValueError:
            raise Exception('Parameter \'iter_data\' must be a tuple as (kwarg, iterable)')

        self.iter_data = iter_data
        self.invoke_kwargs = invoke_kwargs
