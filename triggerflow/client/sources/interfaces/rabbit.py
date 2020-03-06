from ..model import EventSource


class RabbitEventSource(EventSource):
    def __init__(self, amqp_url: str, topic: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.topic = topic
        self.amqp_url = amqp_url

    def publish_cloudevent(self, cloudevent: dict):
        pass

    @property
    def json(self):
        d = super().json
        d['spec'] = vars(self)
        d['class'] = self.__class__.__name__
        return d
