import json
from typing import Optional

from ..libs.cloudevents.sdk.event.v1 import Event as CloudEvent


class EventSource:
    def __init__(self, name: Optional[str] = None, *args, **kwargs):
        self.name = name

    def publish_cloudevent(self, cloudevent: CloudEvent):
        raise NotImplementedError()

    def set_stream(self, stream_id: str):
        raise NotImplementedError()

    def get_stream(self):
        raise NotImplementedError()

    def get_json_eventsource(self):
        raise NotImplementedError()

    @staticmethod
    def _cloudevent_to_json_dict(cloudevent):
        if isinstance(cloudevent, dict):
            dict_event = cloudevent
        else:
            dict_event = json.loads(cloudevent.MarshalJSON(json.dumps).read().decode('utf-8'))
        return dict_event

    @staticmethod
    def _cloudevent_to_json_encoded(cloudevent):
        if isinstance(cloudevent, bytes):
            bytes_cloudevent = cloudevent
        elif isinstance(cloudevent, dict):
            bytes_cloudevent = json.dumps(cloudevent).encode('utf-8')
        else:
            bytes_cloudevent = cloudevent.MarshalJSON(json.dumps).read()
        return bytes_cloudevent

    @staticmethod
    def _cloudevent_to_json_str(cloudevent):
        if isinstance(cloudevent, str):
            bytes_cloudevent = cloudevent
        elif isinstance(cloudevent, dict):
            bytes_cloudevent = json.dumps(cloudevent)
        else:
            bytes_cloudevent = cloudevent.MarshalJSON(json.dumps).read().decode('utf-8')
        return bytes_cloudevent

