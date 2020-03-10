from multiprocessing import Process


class EventSourceHook(Process):
    def __init__(self, name: str, *args, **kwargs):
        super().__init__()
        self.name = name

    def run(self):
        raise NotImplementedError()

    def commit(self, records):
        raise NotImplementedError()

    def stop(self):
        raise NotImplementedError()
