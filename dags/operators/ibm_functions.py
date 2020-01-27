import textwrap
from dags.models.baseoperator import BaseOperator
from dags.hooks import IBMCloudFunctionsHook


class IBMCloudFunctionsOperator(BaseOperator):
    def __init__(
            self,
            function_name: str,
            function_package: str,
            function_memory: int = 256,
            function_timeout: int = 60000,
            function_image: str = 'aitorarjona/eventprocessor-kafka-runtime-v36:0.3',
            code: str = None,
            zipfile: bytes = None,
            overwrite: bool = False,
            **kwargs):
        super().__init__(**kwargs)

        self.function_name = function_name
        self.function_package = function_package
        self.function_memory = function_memory
        self.function_timeout = function_timeout
        self.function_image = function_image
        self.function_args = {}
        self.kind = None

        if bool(code) and bool(zipfile):
            raise Exception("Both 'code' and 'zipfile' arguments not permitted.")

        create_function = overwrite and (bool(code) or bool(zipfile))
        is_binary = not bool(code) and bool(zipfile)

        code = textwrap.dedent(code) if bool(code) and not bool(zipfile) else zipfile

        self.connection = IBMCloudFunctionsHook().get_conn()

        self.url = '{}/api/v1/namespaces/{}/actions/{}/{}'.format(self.connection.endpoint,
                                                                  self.connection.namespace,
                                                                  self.function_package,
                                                                  self.function_name)

        if create_function:
            res = self.connection.create_package(
                package=self.function_package
            )
            res = self.connection.create_action(
                package=self.function_package,
                action_name=self.function_name,
                image_name=function_image,
                code=code,
                memory=function_memory,
                timeout=function_timeout,
                is_binary=is_binary
            )
        else:
            res = self.connection.get_action(
                package=self.function_package,
                action_name=self.function_name)
            # TODO Check if action exists, and if does, update memory if necessary

    def to_dict(self):
        baseop = super().to_json()
        spec = dict()
        spec['kind'] = self.kind
        spec['function_name'] = self.function_name
        spec['function_package'] = self.function_package
        spec['function_args'] = self.function_args
        spec['function_url'] = self.url
        baseop['operator'] = spec
        return baseop


class IBMCloudFunctionsCallAsyncOperator(IBMCloudFunctionsOperator):
    def __init__(
            self,
            args: dict = None,
            **kwargs):
        super().__init__(**kwargs)

        self.kind = 'callasync'

        if args is None:
            args = {}
        if type(args) is not dict:
            raise Exception('args must be dict')
        self.function_args = args


class IBMCloudFunctionsMapOperator(IBMCloudFunctionsOperator):
    def __init__(
            self,
            iter_data: list,
            **kwargs):
        super().__init__(**kwargs)

        self.kind = 'map'

        # Check if iter_data is iterable
        iter(iter_data)
        self.function_args = iter_data
