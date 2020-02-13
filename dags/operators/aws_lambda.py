from dags.models.baseoperator import BaseOperator
from dags.hooks import AWSLambdaHook


class AWSLambdaOperator(BaseOperator):
    def __init__(self,
                 function_name: str,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.function_name = function_name
        self.invoke_args = {}
        self.kind = None

        self.client = AWSLambdaHook().get_conn()

        self.client.get_function(FunctionName=self.function_name)

    def to_dict(self):
        baseop = super().to_json()
        spec = dict()
        spec['kind'] = self.kind
        spec['function_name'] = self.function_name
        spec['invoke_args'] = self.invoke_args
        baseop['operator'] = spec
        return baseop


class AWSLambdaCallAsyncOperator(AWSLambdaOperator):
    def __init__(self,
                 invoke_args: dict = None,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.kind = 'callasync'

        if invoke_args is None:
            invoke_args = {}
        if type(invoke_args) != dict:
            raise Exception('args must be dict')
        self.invoke_args = invoke_args


class AWSLambdaMapOperator(AWSLambdaOperator):
    def __init__(self,
                 iter_data: list,
                 **kwargs):
        super().__init__(**kwargs)

        self.kind = 'map'

        # Check if iter_data is iterable
        iter(iter_data)
        self.invoke_args = iter_data
