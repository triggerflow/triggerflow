from ..models.basehook import BaseHook
from triggerflow.libs.ibm_cloudfunctions_client import CloudFunctionsClient
from ... import eventsources
from ...config import get_dag_operator_config


class IBMCloudFunctionsHookMeta(type):
    __instance = None

    def __call__(cls):
        if cls.__instance is None:
            cls.__instance = super().__call__()
        return cls.__instance


class IBMCloudFunctionsHook(BaseHook, metaclass=IBMCloudFunctionsHookMeta):
    def __init__(self):
        self.credentials = get_dag_operator_config('ibm_cf')

        if not {'endpoint', 'namespace', 'api_key', 'event_source'}.issubset(self.credentials):
            raise Exception('Missing parameters for IBM Cloud Functions Operator in config file')

        self.endpoint = self.credentials['endpoint']
        self.namespace = self.credentials['namespace']

        self.__client = CloudFunctionsClient(
            endpoint=self.endpoint,
            namespace=self.namespace,
            api_key=self.credentials['api_key']
        )

        # Create event source instance
        event_source_name = self.credentials['event_source']
        event_source = getattr(eventsources, event_source_name.capitalize() + 'EventSource')
        self.__event_source = event_source(name=event_source_name, **self.credentials[event_source_name])

    def get_conn(self):
        return self.__client

    def get_event_source(self):
        return self.__event_source
