import json
import logging
import threading
from multiprocessing import Queue

import boto3
from arnparse import arnparse

from .model import EventSourceHook

logging.getLogger('boto3').setLevel(logging.CRITICAL)
logging.getLogger('botocore').setLevel(logging.CRITICAL)


class SQSEventSource(EventSourceHook):
    def __init__(self,
                 event_queue: Queue,
                 queue: str,
                 access_key_id: str,
                 secret_access_key: str,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.__access_key_id = access_key_id
        self.__secret_access_key = secret_access_key

        self.event_queue = event_queue
        self.queue = queue
        self.client = None
        self.sqs = None
        self.records = {}
        self.__commit_queue = Queue()
        self.__committer = None

    def run(self):

        def delete_messages():
            commit_records = self.__commit_queue.get()
            for record in commit_records:
                if record not in self.records:
                    continue
                logging.debug('[{}] Deleting message {} from queue {}'.format(self.name, record, self.queue))
                if record in self.records:
                    commit_message = self.records[record]
                    commit_message.delete()

        self.__committer = threading.Thread(target=delete_messages)
        self.__committer.start()

        self.sqs = boto3.resource('sqs',
                                  aws_access_key_id=self.__access_key_id,
                                  aws_secret_access_key=self.__secret_access_key)
        self.client = boto3.client('sqs',
                                   aws_access_key_id=self.__access_key_id,
                                   aws_secret_access_key=self.__secret_access_key)

        response = self.client.get_queue_url(QueueName=self.queue)
        queue_url = response['QueueUrl']
        sqs_queue = self.sqs.Queue(queue_url)

        while True:
            messages = sqs_queue.receive_messages(WaitTimeSeconds=10)
            for message in messages:
                event = json.loads(message.body)
                print(event)
                if {'specversion', 'id', 'source', 'type'}.issubset(set(event)):
                    logging.info('[{}] Received CloudEvent'.format(self.name))

                    event_id = event['id']
                    if event_id in self.records:
                        continue

                    cloudevent = event
                else:
                    event_id = event['requestContext']['requestId']

                    if event_id in self.records:
                        continue

                    subject = event['responsePayload']['__TRIGGERFLOW_SUBJECT']
                    del event['responsePayload']['__TRIGGERFLOW_SUBJECT']
                    event_type = 'lambda.success' if event['requestContext']['condition'] == 'Success' else 'lambda.failure'

                    cloudevent = {'specversion': '1.0',
                                  'id': event['requestContext']['requestId'],
                                  'source': event['requestContext']['functionArn'],
                                  'type': event_type,
                                  'time': event['timestamp'],
                                  'subject': subject,
                                  'datacontenttype': 'application/json',
                                  'data': event['responsePayload']}

                self.event_queue.put(cloudevent)
                self.records[event_id] = message

    def commit(self, records):
        self.__commit_queue.put(records)

    def stop(self):
        raise NotImplementedError()
