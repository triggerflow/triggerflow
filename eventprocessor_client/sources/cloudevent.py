class Event:
    def __init__(self, subject: str, type: str = 'termination.event.success'):
        self.subject = subject
        self.type = type
