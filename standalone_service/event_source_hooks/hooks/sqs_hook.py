import json
import logging
from uuid import uuid1
from enum import Enum
from typing import List

import boto3

from multiprocessing import Queue
from standalone_service.event_source_hooks.model import Hook


class SQSCloudEventSourceHook(Hook):
    def __init__(self,
                 event_queue: Queue,
                 queue_url: str,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.queue_url = queue_url
        self.event_queue = event_queue
        self.client = None

    def run(self):
        sqs = boto3.resource('sqs')  # Consumer expects queue to be publicly accessible
        queue = sqs.Queue(self.queue_url)

        while True:
            for message in queue.receive_messages():
                termination_event = json.loads(message.body)
                termination_cloudevent = {'specversion': '1.0',
                                          'id': termination_event['requestContext']['requestId'],
                                          'source': termination_event['requestContext']['functionArn'],
                                          'type': 'termination.event.{}'.format(
                                              termination_event['requestContext']['condition'].lower()),
                                          'time': termination_event['timestamp'],
                                          'subject': termination_event['requestContext']['functionArn'].split(':')[-2],
                                          'datacontenttype': 'application/json',
                                          'data': json.loads(termination_event['responsePayload']['body'])}
                self.event_queue.put(termination_cloudevent)
                # message.delete()

    def poll(self):
        raise NotImplementedError()

    def body(self, record):
        raise NotImplementedError()

    def commit(self, records):
        raise NotImplementedError()

    def stop(self):
        raise NotImplementedError()
