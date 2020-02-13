from eventprocessor_client.sources.model import CloudEventSource


class SQSCloudEventSource(CloudEventSource):
    def __init__(self,
                 region: str,
                 account: str,
                 queue_name: str,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.queue_url = 'https://sqs.{}.amazonaws.com/{}/{}'.format(region, account, queue_name)

    @property
    def dict(self):
        d = super().json
        d['spec'] = vars(self)
        d['class'] = self.__class__.__name__
        return d
