from ..model import EventSource
from typing import Optional


class RedisEventSource(EventSource):
    def __init__(self, host: str, port: int, password: Optional[str] = None,  *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.host = host
        self.port = port
        self.password = password

    def publish_cloudevent(self, cloudevent: dict):
        pass

    @property
    def json(self):
        d = super().json
        d['spec'] = vars(self)
        d['class'] = self.__class__.__name__
        return d
