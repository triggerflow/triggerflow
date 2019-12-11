class Broker:
    def __init__(self):
        pass

    def initialize_consumer(self):
        raise NotImplementedError()

    def poll(self):
        raise NotImplementedError()

    def body(self, record):
        raise NotImplementedError()

    def commit(self, record):
        raise NotImplementedError()
