class EventSource:

    def publish_cloudevent(self, cloudevent: dict):
        raise NotImplementedError()

    @property
    def json(self):
        raise NotImplementedError()
