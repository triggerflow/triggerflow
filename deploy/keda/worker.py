import os
import yaml
import time
import logging
import traceback
from uuid import uuid4
from queue import Empty
from enum import Enum
from datetime import datetime
from multiprocessing import Process, Queue
from threading import Thread
from triggerflow.service.databases import RedisDatabase
import triggerflow.service.eventsources as eventsources
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

    def __init__(self, workspace, credentials):
        super().__init__()
        self.workspace = workspace
        self.worker_id = str(uuid4())[:6]
        self.__credentials = credentials

        self.tart_time = 0
        self.triggers = {}
        self.trigger_events = {}
        self.global_context = {}
        self.eventsources = {}
        self.event_queue = Queue()
        self.commit_queue = Queue()
        self.dead_letter_queue = Queue()

        self.current_state = Worker.State.INITIALIZED

        self.__start_db()
        self.__start_eventsources()

    def __start_db(self):
        print('[{}] Creating database connection'.format(self.workspace))
        # Instantiate DB client
        # TODO Make storage abstract
        self.__db = RedisDatabase(**self.__credentials['redis'])

    def __start_eventsources(self):
        print("[{}] Starting event sources ".format(self.workspace))
        event_sources = self.__db.get(workspace=self.workspace, document_id='event_sources')
        for evt_src in event_sources.values():
            if evt_src['name'] in self.eventsources:
                continue
            print("[{}] Starting {}".format(self.workspace, evt_src['name']))
            eventsource_class = getattr(eventsources, '{}'.format(evt_src['class']))
            eventsource = eventsource_class(event_queue=self.event_queue, **evt_src)
            eventsource.start()
            self.eventsources[evt_src['name']] = eventsource

    def __stop_eventsources(self):
        print("[{}] Stopping event sources ".format(self.workspace))
        for evt_src in list(self.eventsources):
            self.eventsources[evt_src].stop()
            del self.eventsources[evt_src]

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
                ids = {}
                events_to_commit, triggers_to_delete = commit_queue.get()

                if events_to_commit is None and triggers_to_delete is None:
                    continue

                print('[{}] Committing {} events'
                      .format(self.workspace, len(events_to_commit)))

                for event in events_to_commit:
                    event_source = event['event_source']
                    if not event['event_source'] in ids:
                        ids[event_source] = []
                    ids[event_source].append(event['id'])

                for event_source in ids:
                    self.eventsources[event_source].commit(ids[event_source])

                # Delete all transient triggers from DB
                if triggers_to_delete:
                    print('[{}] Deleting {} transient triggers'
                          .format(self.workspace, len(triggers_to_delete)))
                    self.__db.delete_keys(workspace=self.workspace,
                                          document_id='triggers',
                                          keys=triggers_to_delete)

        self.__commiter = Thread(target=commiter, args=(self.commit_queue, ))
        self.__commiter.start()

    def __should_run(self):
        return self.current_state == Worker.State.RUNNING

    def run(self):
        print('[{}] Starting worker {}'.format(self.workspace, self.worker_id))
        self.tart_time = datetime.now()

        self.__get_global_context()
        self.__get_triggers()

        print('[{}] Worker {} Started'.format(self.workspace, self.worker_id))
        self.current_state = Worker.State.RUNNING

        self.__start_commiter()

        events = {}
        triggers_to_delete = []

        while self.__should_run():
            print('[{}] Waiting for events...'.format(self.workspace))
            try:
                # keda is set to scale down the pod in 20 seconds
                event = self.event_queue.get(timeout=10)
            except Empty:
                self.stop_worker()
                continue
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
                    processed_events = events[subject]
                    # TODO: Save context in commiter
                    self.commit_queue.put((processed_events, triggers_to_delete.copy()))
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

        while True:
            time.sleep(5)

    def stop_worker(self):
        print("[{}] Stopping Worker {}".format(self.workspace, self.worker_id))
        self.current_state = Worker.State.FINISHED
        self.commit_queue.put((None, None))  # To finish the __commiter thread
        self.__commiter.join()
        self.__stop_eventsources()
        print("[{}] Worker {} stopped".format(self.workspace, self.worker_id))


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
