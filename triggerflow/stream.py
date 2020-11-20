import itertools
from collections import namedtuple
from typing import Dict
from uuid import uuid4 as uuid

from .client import TriggerflowClient
from .libs.cloudevents.sdk.event.v1 import Event as CloudEvent
from .eventsources.model import EventSource

EventHandler = namedtuple('EventHandler', 'condition action context')
EventPattern = namedtuple('EventPattern', 'subject type')


class EventStream:
    def __init__(self,
                 event_source: EventSource,
                 global_context: dict = None,
                 triggerflow_endpoint: str = None,
                 triggerflow_user: str = None,
                 triggerflow_password: str = None,
                 triggerflow_workspace: str = None):

        if triggerflow_workspace is None:
            triggerflow_workspace = '_'.join(['stream', uuid().hex[8:-8]])

        self.tf = TriggerflowClient(triggerflow_endpoint, triggerflow_user, triggerflow_password)
        self.tf.target_workspace(triggerflow_workspace)
        self.tf.create_workspace(event_source, global_context=global_context)

    def match(self, cases: Dict[EventPattern, EventHandler]):
        for event, trigger in cases.items():
            subjects = event.subject
            if not isinstance(subjects, list):
                subjects = [subjects]
            types = event.type
            if not isinstance(types, list):
                types = [types]

            event_combinations = itertools.product(subjects, types)

            activation_events = [CloudEvent().SetSubject(subject).SetEventType(type)
                                 for subject, type in event_combinations]
            self.tf.add_trigger(event=activation_events, action=trigger.action,
                                condition=trigger.condition, context=trigger.context)
