from ..model import EventSource
from typing import Optional


class RabbitEventSource(EventSource):
    def __init__(self,
                 amqp_url: str,
                 queue: Optional[str] = None,
                 *args, **kwargs):

        super().__init__(*args, **kwargs)

        self.queue = queue
        self.amqp_url = amqp_url

    def _set_name(self, prefix):
        self.name = '{}-rabbit-eventsource'.format(prefix)

    @property
    def json(self):
        d = vars(self)
        d['class'] = self.__class__.__name__
        return d
