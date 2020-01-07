from dags.models.basehook import BaseHook
from dags.libs.ibm_cloudfunctions_client import CloudFunctionsClient
from dags.hooks.utils import load_conf

MANDATORY_CREDENTIALS = ('endpoint', 'namespace', 'api_key')


class IBMCloudFunctionsHook(BaseHook):
    def __init__(self):
        self.config = load_conf('ibm_cloud_functions')

        if not set(self.config).issubset(MANDATORY_CREDENTIALS):
            raise Exception('Missing mandatory credentials in config file under "ibm_cloud_functions"')

        self.client = CloudFunctionsClient(
            endpoint=self.config['endpoint'],
            namespace=self.config['namespace'],
            api_key=self.config['api_key']
        )

    def get_conn(self):
        return self.client
