import logging
import traceback
from uuid import uuid4
from enum import Enum
from datetime import datetime
from multiprocessing import Process, Queue
from threading import Thread
from collections import defaultdict

from . import triggerstorage
from . import eventsources
from . import conditions as default_conditions
from . import actions as default_actions
from .trigger import Trigger, Context


class AuthHandlerException(Exception):
    def __init__(self, response):
        self.response = response


class Worker(Process):
    class State(Enum):
        INITIALIZED = 'Initialized'
        RUNNING = 'Running'
        FINISHED = 'Finished'

    def __init__(self, workspace, config):
        super().__init__()
        self.workspace = workspace
        self.worker_id = str(uuid4())[:6]
        self.__config = config

        self.start_time = 0
        self.trigger_storage = None
        self.triggers = {}
        self.trigger_mapping = {}
        self.events = defaultdict(list)
        self.global_context = {}
        self.event_sources = {}
        self.event_queue = Queue()
        self.checkpoint_queue = Queue()
        self.dead_letter_queue = Queue()
        self.deleted_triggers = {}

        self.state = Worker.State.INITIALIZED

    def __start_db(self):
        logging.info('[{}] Creating database connection'.format(self.workspace))
        # Instantiate DB client
        backend = self.__config['trigger_storage']['backend']
        trigger_storage_class = getattr(triggerstorage, backend.capitalize() + 'TriggerStorage')
        self.trigger_storage = trigger_storage_class(**self.__config['trigger_storage']['parameters'])

    def __start_event_sources(self):
        logging.info("[{}] Starting event sources ".format(self.workspace))
        event_sources = self.trigger_storage.get(workspace=self.workspace, document_id='event_sources')
        for evt_src in event_sources.values():
            if evt_src['name'] in self.event_sources:
                continue
            logging.info("[{}] Starting {}".format(self.workspace, evt_src['name']))
            event_source_class = getattr(eventsources, '{}'.format(evt_src['class']))
            event_source = event_source_class(event_queue=self.event_queue,
                                              name=evt_src['name'],
                                              **evt_src['parameters'])
            event_source.start()
            self.event_sources[evt_src['name']] = event_source

    def __stop_event_sources(self):
        logging.info("[{}] Stopping event sources ".format(self.workspace))
        for evt_src in list(self.event_sources):
            self.event_sources[evt_src].stop()
            del self.event_sources[evt_src]

    def __get_global_context(self):
        logging.info('[{}] Getting workspace global context'.format(self.workspace))
        self.global_context = self.trigger_storage.get(workspace=self.workspace, document_id='global_context')

    def __get_triggers(self):
        logging.info("[{}] Updating triggers cache".format(self.workspace))
        try:
            all_triggers = self.trigger_storage.get(workspace=self.workspace, document_id='triggers')
            new_triggers = {key: all_triggers[key] for key in all_triggers.keys() if key not in self.triggers}

            for new_trigger_id, new_trigger_json in new_triggers.items():
                if new_trigger_id == "0":
                    continue
                for event in new_trigger_json['activation_events']:
                    if event['subject'] not in self.trigger_mapping:
                        self.trigger_mapping[event['subject']] = {}
                    if event['type'] not in self.trigger_mapping[event['subject']]:
                        self.trigger_mapping[event['subject']][event['type']] = []

                    condition_callable_name = '_'.join(['condition', new_trigger_json['condition']['name'].lower()])
                    action_callable_name = '_'.join(['action', new_trigger_json['action']['name'].lower()])
                    condition_callable = getattr(default_conditions, condition_callable_name)
                    action_callable = getattr(default_actions, action_callable_name)

                    new_trigger_context = Context(global_context=self.global_context,
                                                  workspace=self.workspace,
                                                  local_event_queue=self.event_queue,
                                                  events=self.events,
                                                  trigger_mapping=self.trigger_mapping,
                                                  triggers=self.triggers,
                                                  trigger_id=new_trigger_id,
                                                  activation_events=new_trigger_json['activation_events'],
                                                  condition=condition_callable,
                                                  action=action_callable)

                    new_trigger_context.update(new_trigger_json['context'])

                    new_trigger = Trigger(condition=condition_callable,
                                          action=action_callable,
                                          context=new_trigger_context,
                                          trigger_id=new_trigger_id,
                                          condition_meta=new_trigger_json['condition'],
                                          action_meta=new_trigger_json['action'],
                                          context_parser=new_trigger_json['context_parser'],
                                          activation_events=new_trigger_json['activation_events'],
                                          transient=new_trigger_json['transient'],
                                          uuid=new_trigger_json['uuid'],
                                          workspace=new_trigger_json['workspace'],
                                          timestamp=new_trigger_json['timestamp'])
                    self.triggers[new_trigger_id] = new_trigger
                    self.trigger_mapping[event['subject']][event['type']].append(new_trigger_id)

        except KeyError:
            logging.error('Could not retrieve triggers and/or source events for {}'.format(self.workspace))
        logging.info("[{}] Triggers updated".format(self.workspace))

    def __start_commiter(self):

        def commiter(commit_queue):
            """
            Commit events and delete transient triggers
            """
            logging.info('[{}] Starting committer thread'.format(self.workspace))

            while self.__should_run():
                events_subject = commit_queue.get()

                if events_to_commit is None:
                    break

                events_to_commit = self.events[events_subject]

                if events_to_commit:
                    logging.info('[{}] Committing {} events'.format(self.workspace, len(events_to_commit)))

                    for event_source in self.event_sources.values():
                        event_source.commit([event['id'] for event in events_to_commit])

                for trigger in self.triggers.values():
                    if trigger.context.modified:
                        logging.info('[{}] Checkpoint of trigger {}'.format(self.workspace, trigger.trigger_id))
                        self.trigger_storage.set_key(workspace=self.workspace,
                                                     document_id='triggers',
                                                     key=trigger.trigger_id,
                                                     value=self.triggers[trigger.trigger_id].to_dict())

        self.__commiter = Thread(target=commiter, args=(self.checkpoint_queue,))
        self.__commiter.start()

    def __should_run(self):
        return self.state == Worker.State.RUNNING

    def run(self):
        logging.info('[{}] Starting worker {}'.format(self.workspace, self.worker_id))
        self.start_time = datetime.now()

        self.__start_db()
        self.__start_event_sources()
        self.__get_global_context()
        self.__get_triggers()

        logging.info('[{}] Worker {} Started'.format(self.workspace, self.worker_id))
        self.state = Worker.State.RUNNING

        self.__start_commiter()

        while self.__should_run():
            logging.info('[{}] Waiting for events...'.format(self.workspace))
            event = self.event_queue.get()
            logging.info('[{}] New event from {}'.format(self.workspace, event['source']))
            subject = event['subject']
            event_type = event['type']

            if subject in self.trigger_mapping and event_type in self.trigger_mapping[subject]:
                self.events[subject].append(event)

                fired = False
                for trigger_id in self.trigger_mapping[subject][event_type]:
                    trigger = self.triggers[trigger_id]

                    try:
                        if trigger.condition(trigger.context, event):
                            trigger.action(trigger.context, event)

                            # Delete transient fired trigger
                            if trigger.transient:
                                self.trigger_mapping[subject][event_type].remove(trigger_id)
                            fired = True
                    except Exception:
                        trigger.context['exception'] = traceback.format_exc()
                        logging.warning(trigger.context['exception'])
                        self.checkpoint_queue.put('')

                if fired:
                    logging.info('[{}] Performing state checkpoint'.format(self.workspace))
                    self.checkpoint_queue.put(subject)
                    del self.events[subject]
            else:
                logging.warning('[{}] Event with subject {} not in cache'.format(self.workspace, subject))
                self.__get_triggers()
                if subject in self.trigger_mapping:
                    self.event_queue.put(event)  # Put the event to the queue to process it again
                else:
                    self.dead_letter_queue.put(event)

        logging.info("[{}] Worker {} finished".format(self.workspace, self.worker_id))

    def stop_worker(self):
        logging.info("[{}] Stopping Worker {}".format(self.workspace, self.worker_id))
        self.state = Worker.State.FINISHED
        self.checkpoint_queue.put("")  # Checkpoint missing triggers
        self.checkpoint_queue.put(None)  # Stop committer
        self.__commiter.join()
        self.__stop_event_sources()
        try:
            self.terminate()
        except Exception:
            pass
        logging.info("[{}] Worker {} stopped".format(self.workspace, self.worker_id))
