class CloudEventSource:
    def __init__(self, name: str):
        self.name = name

    @property
    def json(self):
        return {'name': self.name}
