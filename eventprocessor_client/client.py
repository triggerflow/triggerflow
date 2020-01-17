import requests
import re
from enum import Enum
from base64 import b64encode
from typing import Optional, Union, List

import eventprocessor_client.exceptions
from .sources.model import CloudEventSource
from .sources.cloudevent import CloudEvent


class DefaultConditions(Enum):
    TRUE = 0
    IBM_CF_JOIN = 1


class DefaultActions(Enum):
    PASS = 0
    TERMINATE = 1
    IBM_CF_INVOKE_KAFKA = 2
    IBM_CF_INVOKE_RABBITMQ = 3
    SIM_CF_INVOKE = 4


# TODO Replace prints with proper logging

class CloudEventProcessorClient:
    def __init__(self,
                 api_endpoint: str,
                 user: str,
                 password: str,
                 namespace: Optional[str] = None,
                 eventsource_name: Optional[str] = None):
        """
        Initializes CloudEventProcessor client.
        :param api_endpoint: Endpoint of the Event Processor API.
        :param user: Username to authenticate this client towards the API REST.
        :param password: Password to authenticate this client towards the API REST.
        :param namespace: Namespace which this client targets by default when managing triggers.
        :param eventsource_name: Eventsource which this client targets by default when managing triggers.
        """
        self.namespace = namespace
        self.eventsource_name = eventsource_name
        self.api_endpoint = api_endpoint

        if not re.fullmatch(r"[a-zA-Z0-9_]+", user):
            raise ValueError('Invalid Username')
        if not re.fullmatch(r"^(?=.*[A-Za-z])[A-Za-z\d@$!%*#?&]+$", password):
            raise ValueError('Invalid Password')

        basic_auth = b64encode(bytes(user + ':' + password, 'utf-8')).decode('utf-8')

        res = requests.get('/'.join([self.api_endpoint, 'auth']),
                           headers={'Authorization': 'Basic ' + basic_auth},
                           json={})

        print("{}: {}".format(res.status_code, res.json()))
        if res.ok:
            self.token = res.json()['token']
        else:
            raise Exception('Could not initialize EventProcessor client: {}'.format(res.json()))

    def set_namespace(self, namespace: str):
        """
        Sets the target namespace of this client.
        :param namespace: Namespace name.
        """
        self.namespace = namespace

    def create_namespace(self, namespace: Optional[str] = None,
                         global_context: Optional[dict] = None,
                         event_source: Optional[CloudEventSource] = None):
        """
        Creates a namespace.
        :param namespace: Namespace name.
        :param global_context: Read-only key-value state that is visible for all triggers in the namespace.
        :param event_source: Applies this event source to the new namespace.
        """
        default_context = {'namespace': namespace}

        if namespace is None and self.namespace is None:
            raise ValueError('Namespace cannot be None')
        elif namespace is None:
            namespace = self.namespace

        self.namespace = namespace

        if global_context is not None and type(global_context) is dict:
            global_context.update(default_context)
        if global_context is not None and type(global_context) is not dict:
            raise Exception('Global context must be json-serializable dict')
        if global_context is None:
            global_context = {}

        res = requests.put('/'.join([self.api_endpoint, 'namespace', namespace]),
                           headers={'Authorization': 'Bearer ' + self.token},
                           json={'global_context': global_context,
                                 'event_source': event_source.json})

        print("{}: {}".format(res.status_code, res.json()))
        if res.ok:
            return res.json()
        elif res.status_code == 409:
            raise eventprocessor_client.exceptions.ResourceAlreadyExistsError(res.json())
        else:
            raise Exception(res.json())

    def set_event_source(self, eventsource_name: str):
        """
        Sets the default event source for this client when managing triggers.
        :param eventsource_name: Event source name.
        """
        self.eventsource_name = eventsource_name

    def add_event_source(self, eventsource: CloudEventSource, overwrite: Optional[bool] = False):
        """
        Adds an event source to the target namespace.
        :param eventsource: Instance of CloudEventSource containing the specifications of the event source.
        :param overwrite: True for overwriting the event source specification.
        """
        if self.namespace is None:
            raise eventprocessor_client.exceptions.NullNamespaceError()

        res = requests.put('/'.join([self.api_endpoint, 'namespace', self.namespace, 'eventsource', eventsource.name]),
                           params={'overwrite': overwrite},
                           headers={'Authorization': 'Bearer ' + self.token},
                           json={'eventsource': eventsource.json})

        print("{}: {}".format(res.status_code, res.json()))
        if res.ok:
            return res.json()
        elif res.status_code == 409:
            raise eventprocessor_client.exceptions.ResourceAlreadyExistsError(res.json())
        else:
            raise Exception(res.json())

    def add_trigger(self,
                    event: Union[CloudEvent, List[CloudEvent]],
                    condition: Optional[DefaultConditions] = DefaultConditions.TRUE,
                    action: Optional[DefaultActions] = DefaultActions.PASS,
                    context: Optional[dict] = None,
                    transient: Optional[bool] = True,
                    id: Optional[str] = None):
        """
        Adds a trigger to the target namespace.
        :param event: Instance of CloudEvent, this CloudEvent's subject and type will fire this trigger.
        :param condition: Callable that is executed every time the event is received. If it returns true,
        then Action is executed. It has to satisfy signature contract (context, event).
        :param action: Action to perform when event is received and condition is True. It has to satisfy signature
        contract (context, event).
        :param context: Trigger key-value state, only visible for this specific trigger.
        :param transient: If true, this trigger is deleted after action is executed.
        :param id: Custom ID for a persistent trigger.
        """
        if self.namespace is None:
            raise eventprocessor_client.exceptions.NullNamespaceError()

        if transient and id is not None:
            raise eventprocessor_client.exceptions.NamedTransientTriggerError()

        # Check for arguments types
        if context is None:
            context = {}
        if type(context) is not dict:
            raise Exception('Context must be an json-serializable dict')

        events = [event] if type(event) is not list else event
        events = list(map(lambda evt: evt.json, events))
        trigger = {
            'condition': condition.name,
            'action': action.name,
            'context': context,
            'depends_on_events': events,
            'transient': transient,
            'id': id}

        res = requests.post('/'.join([self.api_endpoint, 'namespace', self.namespace, 'trigger']),
                            headers={'Authorization': 'Bearer ' + self.token},
                            json={'events': events,
                                  'trigger': trigger})

        print('{}: {}'.format(res.status_code, res.json()))
        if not res.ok:
            raise Exception(res.json())
        else:
            return res.json()

    def db_get(self, uri):
        """
        Gets an object from the database.
        :param uri: URI of the object (e.g. db://foo/bar)
        :return: Object.
        """
        res = requests.get('/'.join([self.api_endpoint, 'db']),
                           json={'uri': uri,
                                 'authentication': {'token': self.token}})

        print('{}: {}'.format(res.status_code, res.json()))
        if not res.ok:
            raise Exception(res.json())
        else:
            return res.json()

    def db_put(self, uri, data):
        """
        Puts an object to the database.
        :param uri: URI of the object (e.g. db://foo/bar)
        :param data: Data to be stored.
        """
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
        """
        Deletes an object from the database.
        :param uri: URI of the object (e.g. db://foo/bar)
        """
        res = requests.delete('/'.join([self.api_endpoint, 'db']),
                              json={'uri': uri,
                                    'authentication': {'token': self.token}})

        print('{}: {}'.format(res.status_code, res.json()))
        if not res.ok:
            raise Exception(res.json())
        else:
            return res.json()
