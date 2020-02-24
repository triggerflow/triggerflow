import logging
import traceback
from uuid import uuid4
from enum import Enum
from datetime import datetime
from multiprocessing import Process, Queue

import eventprocessor.event_source_hooks.hooks as hooks
from eventprocessor.libs.cloudant_client import CloudantClient
from eventprocessor.event_store import AsyncEventStore

import eventprocessor.conditions.default as default_conditions
import eventprocessor.actions.default as default_actions


class AuthHandlerException(Exception):
    def __init__(self, response):
        self.response = response


class Worker(Process):
    class State(Enum):
        INITIALIZED = 'Initialized'
        RUNNING = 'Running'
        FINISHED = 'Finished'

    def __init__(self, namespace, private_credentials):
        super().__init__()

        self.worker_status = {}
        self.namespace = namespace
        self.worker_id = str(uuid4())
        self.__private_credentials = private_credentials

        self.triggers = {}
        self.trigger_events = {}
        self.global_context = {}
        self.events = {}
        self.event_queue = None
        self.dead_letter_queue = None
        self.event_source_hooks = []
        self.store_event_queue = None

        # Instantiate DB client
        # TODO Make storage abstract
        self.__cloudant_client = CloudantClient(**self.__private_credentials['cloudant'])

        # Get global context
        self.global_context = self.__cloudant_client.get(database_name=namespace, document_id='.global_context')

        self.current_state = Worker.State.INITIALIZED

    def run(self):
        logging.info('[{}] Starting worker {}'.format(self.namespace, self.worker_id))
        worker_start_time = datetime.now()
        self.current_state = Worker.State.RUNNING
        self.__update_triggers()

        self.event_queue = Queue()
        self.dead_letter_queue = Queue()

        # Instantiate broker client
        event_sources = self.__cloudant_client.get(database_name=self.namespace, document_id='.event_sources')
        for evt_src in event_sources.values():
            hook_class = getattr(hooks, '{}Hook'.format(evt_src['class']))
            hook = hook_class(event_queue=self.event_queue, **evt_src['spec'])
            hook.start()
            self.event_source_hooks.append(hook)

        # Instantiate async event store
        self.store_event_queue = Queue()
        event_store = AsyncEventStore(self.store_event_queue, self.namespace, self.__cloudant_client)
        event_store.start()

        while self.__should_run():
            logging.info('[{}] Waiting for events...'.format(self.namespace))
            event = self.event_queue.get()
            logging.info('[{}] New Event: {}'.format(self.namespace, event))
            subject = event['subject']
            event_type = event['type']

            if subject in self.trigger_events and event_type in self.trigger_events[subject]:

                if subject not in self.events:
                    self.events[subject] = []
                self.events[subject].append(event)

                triggers = self.trigger_events[subject][event_type]
                success = True
                for trigger_id in triggers:
                    condition_name = self.triggers[trigger_id]['condition']['name']
                    action_name = self.triggers[trigger_id]['action']['name']
                    context = self.triggers[trigger_id]['context']

                    context['global_context'] = self.global_context
                    context['namespace'] = self.namespace
                    context['local_event_queue'] = self.event_queue
                    context['events'] = self.events
                    context['trigger_events'] = self.trigger_events
                    context['triggers'] = self.triggers
                    context['trigger_id'] = trigger_id
                    context['depends_on_events'] = self.triggers[trigger_id]['depends_on_events']
                    context['condition'] = self.triggers[trigger_id]['condition']
                    context['action'] = self.triggers[trigger_id]['action']

                    condition = getattr(default_conditions, '_'.join(['condition', condition_name.lower()]))
                    action = getattr(default_actions, '_'.join(['action', action_name.lower()]))

                    try:
                        if condition(context, event):
                            action(context, event)
                            if 'counter' in context:
                                del context['counter']
                        else:
                            success = False
                    except Exception as e:
                        print(traceback.format_exc())
                        # TODO Handle condition/action exceptions
                        raise e
                if success:
                    logging.info('[{}] Successfully processed "{}" subject'.format(self.namespace, subject))
                    self.store_event_queue.put((subject, self.events[subject]))
                    if subject in self.events:
                        del self.events[subject]
            else:
                logging.warn('[{}] Event with subject {} not in cache'.format(self.namespace, subject))
                self.__update_triggers()
                if subject in self.trigger_events:
                    self.event_queue.put(event)  # Put the event to the queue to process it again
                else:
                    self.dead_letter_queue.put(event)

    def stop_worker(self):
        for hook in self.event_source_hooks:
            hook.stop()

        logging.info("[{}] Worker {} stopped".format(self.namespace, self.worker_id))
        self.terminate()

    def __should_run(self):
        return self.current_state == Worker.State.RUNNING

    def __update_triggers(self):
        logging.info("[{}] Updating triggers cache".format(self.namespace))
        try:
            all_triggers = self.__cloudant_client.get(database_name=self.namespace, document_id='.triggers')
            new_triggers = {key: all_triggers[key] for key in all_triggers.keys() if key not in self.triggers}

            for new_trigger_id, new_trigger in new_triggers.items():
                for event in new_trigger['depends_on_events']:
                    if event['subject'] not in self.trigger_events:
                        self.trigger_events[event['subject']] = {}
                    if event['type'] not in self.trigger_events[event['subject']]:
                        self.trigger_events[event['subject']][event['type']] = []

                    self.trigger_events[event['subject']][event['type']].append(new_trigger_id)

            for k, v in new_triggers.items():
                if k not in self.triggers:
                    self.triggers[k] = v
        except KeyError:
            logging.error('Could not retrieve triggers and/or source events for {}'.format(self.namespace))
        logging.info("[{}] Triggers updated".format(self.namespace))

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
