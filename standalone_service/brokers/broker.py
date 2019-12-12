class Broker:
    def __init__(self):
        pass

    def poll(self):
        raise NotImplementedError()

    def body(self, record):
        raise NotImplementedError()

    def commit(self, records):
        raise NotImplementedError()
