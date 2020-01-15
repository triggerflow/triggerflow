from multiprocessing import Process


class Hook(Process):
    def __init__(self, name: str, *args, **kwargs):
        super().__init__()
        self.name = name

    def run(self):
        raise NotImplementedError()

    def poll(self):
        raise NotImplementedError()

    def body(self, record):
        raise NotImplementedError()

    def commit(self, records):
        raise NotImplementedError()
