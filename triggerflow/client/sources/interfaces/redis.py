from ..model import EventSource
from typing import Optional


class RedisEventSource(EventSource):
    def __init__(self,
                 host: str,
                 port: int,
                 password: Optional[str] = None,
                 *args, **kwargs):

        super().__init__(*args, **kwargs)

        self.host = host
        self.port = port
        self.password = password

    def _set_name(self, prefix):
        self.name = '{}-redis-eventsource'.format(prefix)

    @property
    def json(self):
        d = vars(self)
        d['class'] = self.__class__.__name__
        return d
