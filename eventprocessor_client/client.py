import requests
import re
from requests.auth import HTTPBasicAuth
from typing import Optional, Union, List

from . import exceptions
from .sources.model import CloudEventSource
from .sources.cloudevent import CloudEvent
from .conditions_actions import ConditionActionModel, DefaultActions, DefaultConditions


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

        self.basic_auth = HTTPBasicAuth(user, password)

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
        elif global_context is not None and type(global_context) is not dict:
            raise Exception('Global context must be json-serializable dict')
        else:
            global_context = {}

        evt_src = event_source.json if event_source is not None else None

        res = requests.put('/'.join([self.api_endpoint, 'namespace', namespace]),
                           auth=self.basic_auth,
                           json={'global_context': global_context,
                                 'event_source': evt_src})

        print("{}: {}".format(res.status_code, res.json()))
        if res.ok:
            return res.json()
        elif res.status_code == 409:
            raise exceptions.ResourceAlreadyExistsError(res.json())
        else:
            raise Exception(res.json())

    def delete_namespace(self, namespace: str = None):
        if namespace is None and self.namespace is None:
            raise exceptions.NullNamespaceError()
        elif namespace is None:
            namespace = self.namespace

        res = requests.delete('/'.join([self.api_endpoint, 'namespace', namespace]),
                              auth=self.basic_auth,
                              json={})

        print("{}: {}".format(res.status_code, res.json()))
        if res.ok:
            return res.json()
        elif res.status_code == 409:
            raise exceptions.ResourceDoesNotExist(res.json())
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

        res = requests.put('/'.join([self.api_endpoint, 'namespace', self.namespace, 'eventsource', eventsource.name]),
                           params={'overwrite': overwrite},
                           auth=self.basic_auth,
                           json={'eventsource': eventsource.json})

        print("{}: {}".format(res.status_code, res.json()))
        if res.ok:
            return res.json()
        elif res.status_code == 409:
            raise exceptions.ResourceAlreadyExistsError(res.json())
        else:
            raise Exception(res.json())

    def add_trigger(self,
                    event: Union[CloudEvent, List[CloudEvent]],
                    condition: Optional[ConditionActionModel] = DefaultConditions.TRUE,
                    action: Optional[ConditionActionModel] = DefaultActions.PASS,
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
            raise exceptions.NullNamespaceError()

        if transient and id is not None:
            raise exceptions.NamedTransientTriggerError()

        # Check for arguments types
        if context is None:
            context = {}
        if type(context) is not dict:
            raise Exception('Context must be an json-serializable dict')

        events = [event] if type(event) is not list else event
        events = list(map(lambda evt: evt.json, events))
        trigger = {
            'condition': condition.value,
            'action': action.value,
            'context': context,
            'depends_on_events': events,
            'transient': transient,
            'id': id}

        res = requests.post('/'.join([self.api_endpoint, 'namespace', self.namespace, 'trigger']),
                            auth=self.basic_auth,
                            json={'events': events,
                                  'trigger': trigger})

        print('{}: {}'.format(res.status_code, res.json()))
        if not res.ok:
            raise Exception(res.json())
        else:
            return res.json()
