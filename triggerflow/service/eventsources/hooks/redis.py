import redis
import json
import logging
from multiprocessing import Queue
from typing import Optional
from ..model import EventSourceHook


class RedisEventSource(EventSourceHook):
    def __init__(self,
                 event_queue: Queue,
                 host: str,
                 port: int,
                 db: Optional[int] = 0,
                 password: Optional[str] = None,
                 stream: Optional[str] = None,
                 *args, **kwargs):

        super().__init__(*args, **kwargs)

        self.event_queue = event_queue
        self.host = host
        self.port = port
        self.db = db
        self.password = password
        self.stream = stream
        self.__should_run = True

        self.redis = redis.StrictRedis(host=self.host, port=self.port, password=self.password,
                                       charset="utf-8", decode_responses=True)

    def run(self):
        last_id = '$'
        while self.__should_run:
            records = self.redis.xread({self.stream: last_id}, block=0)[0][1]
            # logging.info('Total events downloaded:', len(records))
            for last_id, event in records:
                try:
                    event['data'] = json.loads(event['data'])
                except KeyError:
                    pass
                logging.info("[{}] Received event".format(self.name))
                self.event_queue.put(event)

    def stop(self):
        logging.info("[{}] Stopping event source".format(self.name))
        self.redis.delete(self.stream)
        self.__should_run = False
        self.terminate()

    @property
    def config(self):
        d = {'type': 'redis', 'host': self.host, 'port': self.port,
             'password': self.password, 'stream': self.stream}
        return d
