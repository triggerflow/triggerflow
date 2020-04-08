from ..models.basehook import BaseHook
from ..libs.ibm_cloudfunctions_client import CloudFunctionsClient
from ..hooks.utils import load_conf

MANDATORY_CREDENTIALS = ('endpoint', 'namespace', 'api_key')


class IBMCloudFunctionsHook(BaseHook):
    def __init__(self):
        self.config = load_conf('operators')['ibm_cf']

        if not set(self.config).issubset(MANDATORY_CREDENTIALS):
            raise Exception('Missing mandatory credentials in config file under "ibm_cloud_functions"')

        self.endpoint = self.config['endpoint']
        self.namespace = self.config['namespace']

        self.client = CloudFunctionsClient(
            endpoint=self.endpoint,
            namespace=self.namespace,
            api_key=self.config['api_key']
        )

    def get_conn(self):
        return self.client
