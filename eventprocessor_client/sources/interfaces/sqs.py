from ..model import CloudEventSource


class SQSCloudEventSource(CloudEventSource):
    def __init__(self,
                 region: str,
                 account: str,
                 topic: str,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.queue_url = 'https://sqs.{}.amazonaws.com/{}/{}'.format(region, account, topic)

    def publish_cloudevent(self, cloudevent: dict):
        pass

    @property
    def json(self):
        d = super().json
        d['spec'] = vars(self)
        d['class'] = self.__class__.__name__
        return d
