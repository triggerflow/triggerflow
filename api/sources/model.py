class CloudEventSource:
    def __init__(self):
        pass

    @property
    def dict(self):
        raise NotImplementedError

    @staticmethod
    def event(subject: str, type: str = 'termination.event.success'):
        return {'subject': subject, 'type': type}
