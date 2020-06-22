from typing import Optional

from ..libs.cloudevents.sdk.event.v1 import Event as CloudEvent


class EventSource:
    def __init__(self, name: Optional[str] = None, *args, **kwargs):
        self.name = name

    def publish_cloudevent(self, cloudevent: CloudEvent):
        raise NotImplementedError()

    def set_stream(self, stream_id: str):
        raise NotImplementedError()

    def get_json_eventsource(self):
        raise NotImplementedError()
