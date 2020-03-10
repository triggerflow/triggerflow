from typing import Optional


class EventSource:

    def __init__(self, name: Optional[str] = None, *args, **kwargs):
        self.name = name

    def publish_cloudevent(self, cloudevent: dict):
        raise NotImplementedError()

    @property
    def json(self):
        raise NotImplementedError()
