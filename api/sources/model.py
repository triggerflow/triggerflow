class CloudEventSource:
    def __init__(self):
        self.__events = []

    def event(self, subject: str, type: str):
        self.__events.append({'subject': subject, 'type': type})
