class CloudEventSource:
    def __init__(self, name: str):
        self.name = name

    @property
    def json(self):
        return {'name': self.name}


class Event:
    def __init__(self, subject: str, type: str = 'termination.event.success'):
        self.subject = subject
        self.type = type
