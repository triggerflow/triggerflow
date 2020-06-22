
class BaseHook:
    def get_conn(self):
        raise NotImplementedError()

    def get_event_source(self):
        raise NotImplementedError()
