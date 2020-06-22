from ..callable_model import Callable
from ...libs.ibm_cloudfunctions_client import CloudFunctionsClient
from ...config import get_config


class IBMCloudFunctions(Callable):
    def __init__(self):

        # Get cloud functions config
        config = get_config()
        if 'eventsourcing' not in config \
                or 'ibm_cf' not in config['eventsourcing'] \
                or {'endpoint', 'namespace', 'api_key'} <= set(config['eventsourcing']['ibm_cf']):
            raise Exception('Missing configuration parameters in config file')
        self.cf_client = CloudFunctionsClient()
        super().__init__()

    def call(self, function_package, function_name, invoke_kwargs):
        pass

    def map(self):
        pass
