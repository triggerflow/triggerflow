"""Service class, CanaryDocumentGenerator class.

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
"""

import logging
import json
import time
import requests
import dill
import base64
from uuid import uuid4
from enum import Enum
from requests.auth import HTTPBasicAuth
from datetime import datetime
from multiprocessing import Process
from confluent_kafka import TopicPartition

from service.datetimeutils import seconds_since
from service.cloudant_client import CloudantClient
from service.kafka_client import KafkaClient


class AuthHandlerException(Exception):
    def __init__(self, response):
        self.response = response


class Worker(Process):
    class State(Enum):
        INITIALIZED = 'Initialized'
        RUNNING = 'Running'
        FINISHED = 'Finished'

    max_retries = 5

    def __init__(self, namespace, private_credentials, user_credentials):
        super().__init__()

        self.worker_status = {}
        self.namespace = namespace
        self.worker_id = str(uuid4())
        self.__private_credentials = private_credentials
        self.__user_credentials = user_credentials

        self.triggers = {}
        self.source_events = {}

        self.__cloudant_client = CloudantClient(self.__private_credentials['cloudant']['username'],
                                                self.__private_credentials['cloudant']['apikey'])

        self.__kafka_client = KafkaClient(brokers=self.__private_credentials['eventstreams']['kafka_brokers_sasl'],
                                          username=self.__private_credentials['eventstreams']['user'],
                                          password=self.__private_credentials['eventstreams']['password'])

        self.kafka_topic = self.namespace

        cf_auth = user_credentials['ibm_cloud_functions']['api_key'].split(':')
        self.cf_auth_handler = HTTPBasicAuth(cf_auth[0], cf_auth[1])

        self.current_state = Worker.State.INITIALIZED

    def __update_triggers(self):
        try:
            self.source_events = self.__cloudant_client.get(database_name=self.namespace, document_id='source_events')
            new_triggers = self.__cloudant_client.get(database_name=self.namespace, document_id='triggers')
            for k, v in new_triggers.items():
                if k not in self.triggers:
                    v['condition'] = dill.loads(base64.b64decode(v['condition']))
                    v['action'] = dill.loads(base64.b64decode(v['action']))
                    self.triggers[k] = v
            return True
        except KeyError:
            logging.error('Could not retrieve triggers and/or source events for {}'.format(self.namespace))
            return None

    def run(self):
        logging.info('[{}] Starting worker {}'.format(self.namespace, self.worker_id))
        worker_start_time = datetime.now()
        self.current_state = Worker.State.RUNNING
        topic_created = self.__kafka_client.create_topic(self.kafka_topic)
        triggers_updated = self.__update_triggers()

        if not all([triggers_updated, topic_created]):
            self.kafka_consumer = self.__kafka_client.create_consumer(self.kafka_topic)

            while self.__shouldRun():
                kafka_record = self.kafka_consumer.poll()
                if kafka_record:
                    cloudevent = json.loads(kafka_record.value())
                    print('New Event-->', cloudevent)
                    subject = cloudevent['subject']
                    if subject in self.source_events:
                        trigger_id = self.source_events[subject]
                        trigger = self.triggers[trigger_id]

                        condition = trigger['condition']
                        action = trigger['action']
                        context = trigger['context']
                        if condition(context, cloudevent):
                            action(context, cloudevent)
                    else:
                        logging.warn('[{}] Received unexpected subject {} from event'.format(self.namespace, subject))
        else:
            logging.info('[{}] Worker {} cannot run'.format(self.namespace, self.worker_id))

        self.worker_status['worker_start_time'] = str(worker_start_time)
        self.worker_status['worker_end_time'] = str(datetime.now())
        self.worker_status['worker_elapsed_time'] = seconds_since(worker_start_time)
        self.__cloudant_client.put(database_name=self.namespace,
                                   document_id='worker_{}'.format(self.worker_id), data=self.worker_status)
        self.__kafka_client.delete_topic(self.kafka_topic)
        logging.info('[{}] Worker {} finished - {} seconds'.format(self.namespace, self.worker_id,
                                                                   self.worker_status['worker_elapsed_time']))
        print('--------------- WORKER FINISHED ---------------')

    def __fire_trigger(self, trigger, events):
        for task_id in trigger:
            trigger_name = trigger[task_id]['triggerName']
            trigger_url = trigger[task_id]['triggerURL']

            payload = trigger[task_id]['args'].copy()
            retry = True
            retry_count = 0

            logging.info("[{}] Firing trigger {} with payload: {} ".format(trigger_name, trigger_url, payload))

            payload['__OW_COMPOSER_KAFKA_BROKERS'] = self.private_credentials['eventstreams']['kafka_brokers_sasl']
            payload['__OW_COMPOSER_KAFKA_USERNAME'] = self.private_credentials['eventstreams']['user']
            payload['__OW_COMPOSER_KAFKA_PASSWORD'] = self.private_credentials['eventstreams']['password']
            payload['__OW_COMPOSER_KAFKA_TOPIC'] = self.kafka_topic
            payload['__OW_COMPOSER_EXTRAMETA'] = {'task_id': task_id}

            while retry:
                try:
                    response = requests.post(trigger_url, json=payload, auth=self.cf_auth_handler, timeout=10.0,
                                             verify=True)
                    status_code = response.status_code
                    logging.info("[{}] Repsonse status code {}".format(trigger_name, status_code))

                    # Manually commit offset if the trigger was fired successfully. Retry firing the trigger
                    # for a select set of status codes
                    if status_code in range(200, 300):
                        self.worker_status['tasks'][task_id]['time_start'] = datetime.now()
                        if status_code == 204:
                            logging.info("[{}] Successfully fired trigger".format(trigger_name))
                        else:
                            response_json = response.json()
                            if 'activationId' in response_json and response_json['activationId'] is not None:
                                logging.info("[{}] Fired trigger with activation {}".format(trigger_name, response_json[
                                    'activationId']))
                                self.worker_status['tasks'][task_id]['activation_id'] = response_json['activationId']
                            else:
                                logging.info("[{}] Successfully fired trigger".format(trigger_name))
                        if events:
                            self.kafka_consumer.commit(offsets=self.__get_offset_list(events), async=False)
                        retry = False
                    elif self.__should_disable(status_code):
                        retry = False
                        logging.error(
                            '[{}] Error talking to OpenWhisk, status code {}'.format(trigger_name, status_code))
                        self.__dump_request_response(trigger_name, response)
                except requests.exceptions.RequestException as e:
                    logging.error('[{}] Error talking to OpenWhisk: {}'.format(trigger_name, e))
                except AuthHandlerException as e:
                    logging.error("[{}] Encountered an exception from auth handler, status code {}").format(
                        trigger_name, e.response.status_code)
                    self.__dump_request_response(trigger_name, e.response)
                    if self.__should_disable(e.response.status_code):
                        retry = False

                if retry:
                    retry_count += 1
                    if retry_count <= self.max_retries:
                        sleepyTime = pow(2, retry_count)
                        logging.info("[{}] Retrying in {} second(s)".format(trigger_name, sleepyTime))
                        time.sleep(sleepyTime)
                    else:
                        logging.warn("[{}] Tetrying failed after {} attemps".format(trigger_name, self.max_retries))
                        retry = False

    def __should_run(self):
        return self.current_state == Worker.State.Running

    @staticmethod
    def __should_disable(status_code):
        """
        decide whether or not to disable a trigger based on the status code returned
        from firing the trigger. Specifically, disable on all 4xx status codes
        except 408 (gateway timeout), 409 (document update conflict), and 429 (throttle)
        """
        return status_code in range(400, 500) and status_code not in [408, 409, 429]

    @staticmethod
    def __dump_request_response(trigger_name, response):
        response_dump = {
            'request': {
                'method': response.request.method,
                'url': response.request.url,
                'path_url': response.request.path_url,
                'headers': response.request.headers,
                'body': response.request.body
            },
            'response': {
                'status_code': response.status_code,
                'ok': response.ok,
                'reason': response.reason,
                'url': response.url,
                'headers': response.headers,
                'content': response.content
            }
        }

        logging.error('[{}] Dumping the content of the request and response:\n{}'.format(trigger_name, response_dump))

    # return list of TopicPartition which represent the _next_ offset to consume
    @staticmethod
    def __get_offset_list(events):
        offsets = []
        for message in events:
            # Add one to the offset, otherwise we'll consume this message again.
            # That's just how Kafka works, you place the bookmark at the *next* message.
            offsets.append(TopicPartition(message.topic(), message.partition(), message.offset() + 1))

        return offsets

    def __del__(self):
        if self.current_state == Worker.State.Running:
            logging.info('dag: {} - worker finished: {}'.format(self.dag_id, self.run_id))
            self.__kafka_client.delete_topic(self.kafka_topic)
