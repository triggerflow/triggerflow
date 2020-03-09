import os
import yaml
import logging
import traceback
from uuid import uuid4
from enum import Enum
from datetime import datetime
from multiprocessing import Process, Queue

from triggerflow.service.databases import RedisDatabase
import triggerflow.service.eventsources as hooks
import triggerflow.service.conditions.default as default_conditions
import triggerflow.service.actions.default as default_actions


class Worker(Process):
    class State(Enum):
        INITIALIZED = 'Initialized'
        RUNNING = 'Running'
        FINISHED = 'Finished'

    def __init__(self, workspace, private_credentials):
        super().__init__()
        self.workspace = workspace
        self.worker_id = str(uuid4())[:6]
        self.__private_credentials = private_credentials

        self.tart_time = 0
        self.triggers = {}
        self.trigger_events = {}
        self.global_context = {}
        self.events = {}
        self.eventsources = []
        self.event_queue = Queue()
        self.dead_letter_queue = Queue()

        self.current_state = Worker.State.INITIALIZED

    def __start_db(self):
        logging.info('[{}] Creating database connection'.format(self.workspace))
        # Instantiate DB client
        # TODO Make storage abstract
        self.__db = RedisDatabase(**self.__private_credentials['redis'])

    def __get_global_context(self):
        logging.info('[{}] Getting workspace global context'.format(self.workspace))
        # Get global context
        self.global_context = self.__db.get(workspace=self.workspace, document_id='global_context')

    def __update_triggers(self):
        logging.info("[{}] Updating triggers cache".format(self.workspace))
        try:
            all_triggers = self.__db.get(workspace=self.workspace, document_id='triggers')
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
            logging.error('Could not retrieve triggers and/or source events for {}'.format(self.workspace))
        logging.info("[{}] Triggers updated".format(self.workspace))

    def __start_eventsources(self):
        event_sources = self.__db.get(workspace=self.workspace, document_id='event_sources')
        for evt_src in event_sources.values():
            eventsource_class = getattr(hooks, '{}'.format(evt_src['class']))
            eventsource = eventsource_class(event_queue=self.event_queue, **evt_src['spec'])
            eventsource.start()
            self.eventsources.append(eventsource)

    def __should_run(self):
        return self.current_state == Worker.State.RUNNING

    def run(self):
        logging.info('[{}] Starting worker {}'.format(self.workspace, self.worker_id))
        self.tart_time = datetime.now()

        self.__start_db()
        self.__get_global_context()
        self.__update_triggers()
        self.__start_eventsources()

        logging.info('[{}] Worker {} Started'.format(self.workspace, self.worker_id))
        self.current_state = Worker.State.RUNNING

        while self.__should_run():
            logging.info('[{}] Waiting for events...'.format(self.workspace))
            event = self.event_queue.get()
            logging.info('[{}] New Event: {}'.format(self.workspace, event))
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
                    context['workspace'] = self.workspace
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
                    logging.info('[{}] Successfully processed "{}" subject'.format(self.workspace, subject))
                    if subject in self.events:
                        del self.events[subject]
            else:
                logging.warn('[{}] Event with subject {} not in cache'.format(self.workspace, subject))
                self.__update_triggers()
                if subject in self.trigger_events:
                    self.event_queue.put(event)  # Put the event to the queue to process it again
                else:
                    self.dead_letter_queue.put(event)

    def stop_worker(self):
        for eventosurce in self.eventsources:
            eventosurce.stop()

        logging.info("[{}] Worker {} stopped".format(self.workspace, self.worker_id))
        self.terminate()


def main():
    workspace = os.environ.get('WORKSPACE')
    print('Starting workspace {}'.format(workspace))
    print('Loading private credentials')
    with open('config.yaml', 'r') as config_file:
        credentials = yaml.safe_load(config_file)
    worker = Worker(workspace, credentials)
    worker.run()


if __name__ == "__main__":
    main()
