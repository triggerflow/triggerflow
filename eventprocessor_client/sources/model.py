class CloudEventSource:
    def __init__(self, name: str):
        self.name = name

    def publish_cloudevent(self, cloudevent: dict):
        raise NotImplementedError()

    @property
    def json(self):
        return {'name': self.name}
