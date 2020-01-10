from .model import CloudEventSource


class RabbitMQCloudEventSource(CloudEventSource):
    def __init__(self, amqp_url: str, topic: str):
        super().__init__()
        self.topic = topic
        self.amqp_url = amqp_url

    @property
    def dict(self):
        d = super().json
        d['spec'] = vars(self)
        d['class'] = self.__class__.__name__
        return d
