from ..model import EventSource
from typing import Optional
from redis import StrictRedis


class RedisEventSource(EventSource):
    def __init__(self,
                 host: str,
                 port: int,
                 password: Optional[str] = None,
                 stream: Optional[str] = None,
                 *args, **kwargs):

        super().__init__(*args, **kwargs)

        self.host = host
        self.port = port
        self.password = password
        if stream is None:
            self.stream = self.name

    def _set_name(self, prefix):
        self.name = '{}-redis-eventsource'.format(prefix)

    @property
    def json(self):
        d = vars(self)
        d['class'] = self.__class__.__name__
        return d

    def publish_cloudevent(self, cloudevent: dict):
        r = StrictRedis(host=self.host, port=self.port, password=self.password)
        r.xadd(self.name, cloudevent)
