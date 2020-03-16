import redis
from ..model import EventSource
from typing import Optional


class RedisEventSource(EventSource):
    def __init__(self,
                 host: str,
                 port: int,
                 db: Optional[int] = 0,
                 password: Optional[str] = None,
                 stream: Optional[str] = None,
                 *args, **kwargs):

        super().__init__(*args, **kwargs)

        self.host = host
        self.port = port
        self.db = db
        self.password = password
        self.stream = stream

    def _set_name(self, prefix):
        self.name = self.name or '{}-redis-eventsource'.format(prefix)
        self.stream = self.stream or self.name

    def publish_cloudevent(self, cloudevent):
        r = redis.StrictRedis(host=self.host, port=self.port, password=self.password,
                              charset="utf-8", decode_responses=True)
        r.xadd(self.name, cloudevent)

    @property
    def json(self):
        d = vars(self)
        d['class'] = self.__class__.__name__
        return d
