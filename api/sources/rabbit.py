from .model import CloudEventSource


class RabbitMQCloudEventSource(CloudEventSource):
    name = 'RabbitMQ'

    def __init__(self, amqp_url: str, topic: str):
        super().__init__()
        self.topic = topic
        self.amqp_url = amqp_url

    @property
    def dict(self):
        d = vars(self)
        return {'event_source_type': self.name, self.name: d}
