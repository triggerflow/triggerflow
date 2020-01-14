from multiprocessing import Process


class Hook(Process):
    def __init__(self):
        super().__init__()

    def run(self):
        raise NotImplementedError()

    def poll(self):
        raise NotImplementedError()

    def body(self, record):
        raise NotImplementedError()

    def commit(self, records):
        raise NotImplementedError()
