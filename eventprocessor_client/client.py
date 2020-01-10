import requests
import hashlib
import json
import uuid
from enum import Enum
from typing import Optional, Union, List

import api.exceptions
from .sources.model import CloudEventSource


class DefaultConditions(Enum):
    TRUE = 0
    IBM_CF_JOIN = 1


class DefaultActions(Enum):
    PASS = 0
    TERMINATE = 1
    IBM_CF_INVOKE_KAFKA = 2
    IBM_CF_INVOKE_RABBITMQ = 3
    SIM_CF_INVOKE = 4


class CloudEventProcessorClient:
    def __init__(self,
                 api_endpoint: str,
                 authentication: dict,
                 namespace: Optional[str] = None,
                 eventsource_name: Optional[str] = None,
                 global_context: Optional[dict] = None):
        """
        Initializes CloudEventProcessor client
        :param api_endpoint:
        :param namespace: Specifies on which namespace will the triggers be added to.
        """
        self.namespace = namespace
        self.eventsource_name = eventsource_name
        self.api_endpoint = api_endpoint
        self.default_context = {'counter': 0, 'namespace': namespace}
        if global_context is not None and type(global_context) is dict:
            self.default_context.update(global_context)

        res = requests.get('/'.join([self.api_endpoint, 'auth']), json={'authentication': authentication})

        print("{}: {}".format(res.status_code, res.json()))
        if res.ok:
            self.token = res.json()['token']
        else:
            raise Exception('Could not initialize EventProcessor client: {}'.format(res.json()))

    def set_namespace(self, namespace: str):
        self.namespace = namespace

    def create_namespace(self, namespace: str, global_context: Optional[dict] = None):
        if global_context is None:
            global_context = {}

        self.namespace = namespace

        res = requests.put('/'.join([self.api_endpoint, 'namespace', namespace]),
                           headers={'Authorization': 'Bearer '+self.token},
                           json={'global_context': global_context})
        print("{}: {}".format(res.status_code, res.json()))
        if res.ok:
            return res.json()
        elif res.status_code == 409:
            raise api.exceptions.ResourceAlreadyExistsError(res.json())
        else:
            raise Exception(res.json())

    def set_event_source(self, eventsource_name: str):
        self.eventsource_name = eventsource_name

    def add_event_source(self, eventsource: CloudEventSource, overwrite: Optional[bool] = False):
        if self.namespace is None:
            raise api.exceptions.NullNamespaceError()

        res = requests.put('/'.join([self.api_endpoint, 'namespace', self.namespace, 'eventsource', eventsource.name]),
                           params={'overwrite': overwrite},
                           headers={'Authorization': 'Bearer ' + self.token},
                           json={'eventsource': eventsource.json})
        print("{}: {}".format(res.status_code, res.json()))
        if res.ok:
            return res.json()
        elif res.status_code == 409:
            raise api.exceptions.ResourceAlreadyExistsError(res.json())
        else:
            raise Exception(res.json())

    def add_trigger(self,
                    event: Union[dict, List[dict]],
                    condition: Optional[DefaultConditions] = DefaultConditions.TRUE,
                    action: Optional[DefaultActions] = DefaultActions.PASS,
                    context: Optional[dict] = None,
                    transient: Optional[bool] = True,
                    id: Optional[str] = None):
        """
        Adds trigger to namespace
        :param event: The event that will fire this trigger.
        :param condition: Function that is executed every time the event is received. If it returns true,
        then Action is executed. Has to satisfy signature contract (context, event).
        :param action: Action to perform when event is received and condition is True. Has to satisfy signature contract
        (context, event).
        :param context: Trigger context.
        :param transient: Trigger is deleted after action is executed.
        :param id: Custom ID for a persistent trigger.
        """
        if self.namespace is None:
            raise api.exceptions.NullNamespaceError()

        if transient and id is not None:
            raise api.exceptions.NamedTransientTriggerError()

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
            'depends_on_events': list(map(lambda evt: evt['subject'], events)),
            'is_transient': transient,
            'id': id}

        res = requests.post('/'.join([self.api_endpoint, 'namespace', self.namespace, 'trigger']),
                            headers={'Authorization': 'Bearer '+self.token},
                            json={'events': events,
                                  'trigger': trigger})

        print('{}: {}'.format(res.status_code, res.json()))
        if not res.ok:
            raise Exception(res.json())
        else:
            return res.json()

    def db_get(self, uri):
        res = requests.get('/'.join([self.api_endpoint, 'db']),
                           json={'uri': uri,
                                 'authentication': {'token': self.token}})

        print('{}: {}'.format(res.status_code, res.json()))
        if not res.ok:
            raise Exception(res.json())
        else:
            return res.json()

    def db_put(self, uri, data):
        res = requests.put('/'.join([self.api_endpoint, 'db']),
                           json={'uri': uri,
                                 'data': data,
                                 'authentication': {'token': self.token}})

        print('{}: {}'.format(res.status_code, res.json()))
        if not res.ok:
            raise Exception(res.json())
        else:
            return res.json()

    def db_delete(self, uri):
        res = requests.delete('/'.join([self.api_endpoint, 'db']),
                              json={'uri': uri,
                                    'authentication': {'token': self.token}})

        print('{}: {}'.format(res.status_code, res.json()))
        if not res.ok:
            raise Exception(res.json())
        else:
            return res.json()
