from multiprocessing import Process, Queue


class AsyncEventStore(Process):
    def __init__(self, event_store_queue: Queue, namespace: str, database_client):
        super().__init__()
        self.event_store_queue = event_store_queue
        self.namespace = namespace
        self.database_client = database_client

    def run(self):
        while True:
            event_key, events = self.event_store_queue.get()
            self.database_client.set_key(database_name=self.namespace, document_id='.events',
                                         key=event_key, value=events)
