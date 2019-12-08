import requests
import dill
import base64
import inspect
from typing import Optional, Union, List


class CloudEventProcessorClient:
    def __init__(self,
                 namespace: str,
                 ibm_cf_credentials: str,
                 api_endpoint: str,
                 default_context: dict = None):
        """
        Initializes CloudEventProcessor client
        :param api_endpoint:
        :param namespace: Specifies on which namespace will the triggers be added to.
        """
        self.api_endpoint = api_endpoint
        self.namespace = namespace
        self.default_context = {'counter': 0}
        if default_context is not None and type(default_context) is dict:
            self.default_context.update(default_context)

        res = requests.put('/'.join([self.api_endpoint, 'init']),
                           json={'namespace': self.namespace,
                                 'default_context': self.default_context,
                                 'user_credentials': {'ibm_cf_credentials': ibm_cf_credentials}})

        print("{}: {}".format(res.status_code, res.json()))
        if res.ok:
            self.token = res.json()['token']
        else:
            raise Exception('Could not initialize EventProcessor client')

    def add_trigger(self,
                    event: Union[dict, List[dict]],
                    condition: Optional[callable] = (lambda context, event: True),
                    action: Optional[callable] = (lambda context, event: None),
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
        if not inspect.isfunction(action):
            raise Exception('Action must be a function')
        if not inspect.isfunction(condition):
            raise Exception('Condition must be a function')
        if set(inspect.signature(condition).parameters) != {'context', 'event'}:
            raise Exception('Condition must satisfy signature contract (context, event)')
        if set(inspect.signature(action).parameters) != {'context', 'event'}:
            raise Exception('Action must satisfy signature contract (context, event)')

        trigger_context = self.default_context.copy()
        trigger_context.update(context)
        events = [event] if type(event) is not list else event
        trigger = {
            'condition': str(base64.b64encode(dill.dumps(condition)), 'utf-8'),
            'action': str(base64.b64encode(dill.dumps(action)), 'utf-8'),
            'context': trigger_context,
            'trigger_subjects': list(map(lambda evt: evt['subject'], events))}

        res = requests.put('/'.join([self.api_endpoint, 'add_trigger']),
                           json={'namespace': self.namespace,
                                 'events': events,
                                 'trigger': trigger,
                                 'user_credentials': {'token': self.token}})

        print('{}: {}'.format(res.status_code, res.json()))

    def new_event_source(self, subject, type='termination.event.success'):
        return {
            'specversion': '1.0',
            'type': type,
            'subject': subject
        }


def cloudevent_trigger_source(type, source, subject):
    return {
        'specversion': '1.0',
        'type': type,
        'source': source,
        'subject': subject
    }
