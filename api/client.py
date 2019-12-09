import requests
from enum import Enum
from typing import Optional, Union, List

from .sources.model import CloudEventSource


class DefaultConditions(Enum):
    TRUE = 0
    JOIN = 1


class DefaultActions(Enum):
    PASS = 0
    TERMINATE = 1
    IBM_CF_INVOKE = 2


class CloudEventProcessorClient:
    def __init__(self,
                 namespace: str,
                 authentication: dict,
                 api_endpoint: str,
                 event_source: Optional[CloudEventSource] = None,
                 default_context: Optional[dict] = None):
        """
        Initializes CloudEventProcessor client
        :param api_endpoint:
        :param namespace: Specifies on which namespace will the triggers be added to.
        """
        self.namespace = namespace
        self.event_source = event_source.dict if event_source is not None else None
        self.api_endpoint = api_endpoint
        self.default_context = {'counter': 0}
        if default_context is not None and type(default_context) is dict:
            self.default_context.update(default_context)

        res = requests.put('/'.join([self.api_endpoint, 'init']),
                           json={'namespace': self.namespace,
                                 'event_source': self.event_source,
                                 'default_context': self.default_context,
                                 'authentication': authentication})

        print("{}: {}".format(res.status_code, res.json()))
        if res.ok:
            self.token = res.json()['token']
        else:
            raise Exception('Could not initialize EventProcessor client')

    def add_trigger(self,
                    event: Union[dict, List[dict]],
                    condition: Optional[DefaultConditions] = DefaultConditions.TRUE,
                    action: Optional[DefaultActions] = DefaultActions.PASS,
                    context: Optional[dict] = None):
        """
        Adds trigger to namespace
        :param event: The event that will fire this trigger.
        :param condition: Function that is executed every time the event is received. If it returns true,
        then Action is executed. Has to satisfy signature contract (context, event).
        :param action: Action to perform when event is received and condition is True. Has to satisfy signature contract
        (context, event).
        :param context: Trigger context.
        """
        # Check for arguments types
        if context is None:
            context = {}
        if type(context) is not dict:
            raise Exception('Context must be an json-serializable dict')

        events = [event] if type(event) is not list else event
        trigger = {
            'condition': condition.name,
            'action': action.name,
            'context': context,
            'trigger_subjects': list(map(lambda evt: evt['subject'], events))}

        res = requests.put('/'.join([self.api_endpoint, 'add_trigger']),
                           json={'namespace': self.namespace,
                                 'events': events,
                                 'trigger': trigger,
                                 'authentication': {'token': self.token}})

        print('{}: {}'.format(res.status_code, res.json()))
        if not res.ok:
            raise Exception(res.json())
