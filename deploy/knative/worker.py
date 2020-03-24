import logging
import traceback
from queue import Empty
from uuid import uuid4
from enum import Enum
from datetime import datetime
from multiprocessing import Process, Queue
from threading import Thread
from triggerflow.service.databases import RedisDatabase
import triggerflow.service.conditions.default as default_conditions
import triggerflow.service.actions.default as default_actions


class AuthHandlerException(Exception):
    def __init__(self, response):
        self.response = response


class Worker(Process):
    class State(Enum):
        INITIALIZED = 'Initialized'
        RUNNING = 'Running'
        FINISHED = 'Finished'

    def __init__(self, workspace, credentials, event_queue):
        super().__init__()
        self.workspace = workspace
        self.worker_id = str(uuid4())[:6]
        self.__credentials = credentials

        self.tart_time = 0
        self.triggers = {}
        self.trigger_events = {}
        self.global_context = {}
        self.eventsources = {}
        self.event_queue = event_queue
        self.commit_queue = Queue()
        self.dead_letter_queue = Queue()

        self.current_state = Worker.State.INITIALIZED

    def __start_db(self):
        print('[{}] Creating database connection'.format(self.workspace))
        # Instantiate DB client
        # TODO Make storage abstract
        self.__db = RedisDatabase(**self.__credentials['redis'])

    def __get_global_context(self):
        print('[{}] Getting workspace global context'.format(self.workspace))
        self.global_context = self.__db.get(workspace=self.workspace, document_id='global_context')

    def __get_triggers(self):
        print("[{}] Updating triggers cache".format(self.workspace))
        try:
            all_triggers = self.__db.get(workspace=self.workspace, document_id='triggers')
            new_triggers = {key: all_triggers[key] for key in all_triggers if key not in self.triggers}

            for new_trigger_id, new_trigger in new_triggers.items():
                if new_trigger_id == "0":
                    continue
                for event in new_trigger['activation_events']:
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
        print("[{}] Triggers updated".format(self.workspace))

    def __start_commiter(self):

        def commiter(commit_queue):
            """
            Commit events and delete transient triggers
            """
            print('[{}] Starting committer thread'.format(self.workspace))

            while self.__should_run():
                triggers_to_delete = commit_queue.get()

                # Delete all transient triggers from DB
                if triggers_to_delete:
                    print('[{}] Deleting {} transient triggers'
                          .format(self.workspace, len(triggers_to_delete)))
                    self.__db.delete_keys(workspace=self.workspace,
                                          document_id='triggers',
                                          keys=triggers_to_delete)

        self.__commiter = Thread(target=commiter, args=(self.commit_queue, ))
        self.__commiter.daemon = True
        self.__commiter.start()

    def __should_run(self):
        return self.current_state == Worker.State.RUNNING

    def run(self):
        print('[{}] Starting worker {}'.format(self.workspace, self.worker_id))
        self.tart_time = datetime.now()

        self.__start_db()
        self.__get_global_context()
        self.__get_triggers()

        print('[{}] Worker {} Started'.format(self.workspace, self.worker_id))
        self.current_state = Worker.State.RUNNING

        self.__start_commiter()

        events = {}
        triggers_to_delete = []

        while self.__should_run():
            print('[{}] Waiting for events...'.format(self.workspace))
            event = self.event_queue.get()
            print('[{}] New event from {}'.format(self.workspace, event['source']))
            subject = event['subject']
            event_type = event['type']

            if subject in self.trigger_events and event_type in self.trigger_events[subject]:

                if subject not in events:
                    events[subject] = []
                events[subject].append(event)

                success = True
                for trigger_id in self.trigger_events[subject][event_type]:
                    trigger = self.triggers[trigger_id]

                    condition = trigger['condition']
                    action = trigger['action']
                    context = trigger['context']

                    context['global_context'] = self.global_context
                    context['workspace'] = self.workspace
                    context['local_event_queue'] = self.event_queue
                    context['events'] = events
                    context['trigger_events'] = self.trigger_events
                    context['triggers'] = self.triggers
                    context['trigger_id'] = trigger_id
                    context['activation_events'] = trigger['activation_events']
                    context['condition'] = condition
                    context['action'] = action

                    condition = getattr(default_conditions, '_'.join(['condition', condition['name'].lower()]))
                    action = getattr(default_actions, '_'.join(['action', action['name'].lower()]))

                    try:
                        if condition(context, event):
                            # Apply action
                            action(context, event)
                            # Delete transient triggers
                            if trigger['transient']:
                                triggers_to_delete.append(trigger_id)
                                del self.triggers[trigger_id]
                                self.trigger_events[subject][event_type].remove(trigger_id)
                        else:
                            success = False

                    except Exception as e:
                        print(traceback.format_exc())
                        # TODO Handle condition/action exceptions
                        raise e

                if success:
                    print('[{}] Successfully processed "{}" subject'.format(self.workspace, subject))
                    # TODO: Save context in commiter
                    self.commit_queue.put(triggers_to_delete.copy())
                    del events[subject]
                    del self.trigger_events[subject][event_type]
                    triggers_to_delete = []

            else:
                logging.warn('[{}] Event with subject {} not in cache'.format(self.workspace, subject))
                self.__get_triggers()
                if subject in self.trigger_events:
                    self.event_queue.put(event)  # Put the event to the queue to process it again
                else:
                    self.dead_letter_queue.put(event)

        print("[{}] Worker {} finished".format(self.workspace, self.worker_id))
