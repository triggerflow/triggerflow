from typing import Optional

from triggerflow.eventsources.model import EventSource


class SQSEventSource(EventSource):
    def __init__(self, access_key_id: str, secret_access_key: str, queue: Optional[str] = None, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key
        self.queue = queue

    def set_stream(self, stream_id: str):
        self.queue = stream_id

    def publish_cloudevent(self, cloudevent: dict):
        raise NotImplementedError()

    def get_json_eventsource(self):
        parameters = vars(self).copy()
        del parameters['name']
        return {'name': self.name, 'class': self.__class__.__name__, 'parameters': parameters}
