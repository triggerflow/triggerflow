import hashlib
import hmac
import re
import requests
import logging
import json
from requests.auth import HTTPBasicAuth
from typing import List, Union, Optional

from .eventsources.model import EventSource
from .functions import ConditionActionModel, DefaultConditions, DefaultActions
from .config import get_config

log = logging.getLogger(__name__)


class TriggerflowClient:
    def __init__(self, endpoint: str = None, user: str = None, password: str = None, workspace: str = None):
        """
        Instantiate the Triggerflow client.
        :param endpoint: Endpoint URL of the Triggerflow Trigger API.
        :param user: User name
        :param password: User password.
        :param workspace: Target workspace.
        """
        config = get_config()['triggerflow']

        self._api_endpoint = config.get('endpoint', None) if endpoint is None else endpoint
        if 'http://' not in self._api_endpoint:
            self._api_endpoint = 'http://' + self._api_endpoint
        user = config.get('user', None) if user is None else user
        password = config.get('password', None) if password is None else password

        password_hash = hmac.new(bytes(password, 'utf-8'), bytes(user, 'utf-8'), hashlib.sha3_256)

        if None in [self._api_endpoint, user, password]:
            raise Exception('Missing parameters')

        self._workspace = workspace
        self._auth = HTTPBasicAuth(user, password_hash.hexdigest())

    @property
    def workspace(self):
        return self._workspace

    def _check_workspace(self):
        if self._workspace is None:
            raise Exception('Target workspace not set')

    def target_workspace(self, workspace: str):
        """
        Set the current target workspace.
        :param workspace: workspace name.
        """
        if not re.fullmatch(r"[a-zA-Z0-9-_.~]*", workspace):
            raise Exception('Invalid Workspace Name')
        log.info('Target workspace {} set'.format(workspace))
        self._workspace = workspace

    def create_workspace(self,
                         event_source: EventSource,
                         workspace_name: Optional[str] = None,
                         global_context: Optional[dict] = None,
                         silent=True):
        """
        Create a workspace.
        :param event_source: Instance of EventSource. This event source will be attached to the workspace upon
        its creation.
        :param workspace_name: Workspace name.
        :param global_context: Key-value data structure that is accessible from all triggers in the workspace.
        :param silent: If set to True, an exception will not be raised when trying to create an existing workspace.
        """
        if workspace_name is None:
            self._check_workspace()
            workspace_name = self.workspace
        elif self._workspace is None:
            self.target_workspace(workspace_name)

        if global_context is None:
            log.warning('Global context is empty')
            global_context = {}

        if event_source.name is None:
            event_source.name = workspace_name + '_' + event_source.__class__.__name__
            log.warning('Event source name is "{}" -- Changed to "{}" instead'.format(None, event_source.name))

        log.info('Creating workspace {}'.format(workspace_name))
        url = '/'.join([self._api_endpoint, 'workspace'])
        payload = {'workspace_name': workspace_name,
                   'global_context': global_context,
                   'event_source': event_source.get_json_eventsource()}

        try:
            res = requests.post(url, auth=self._auth, json=payload)
        except requests.HTTPError as e:
            raise Exception('Triggerflow API unavailable: {}'.format(e))

        if res.status_code == 201:
            log.info('Created Workspace -- {}'.format(workspace_name, res.json()))
        elif res.status_code == 202:
            log.warning('Workspace created but not Worker not deployed -- {}'.format(workspace_name, res.json()))
        elif res.status_code == 400:
            res_json = res.json()
            if res_json['err_code'] == 2 and silent:
                log.warning('Workspace {} already exists'.format(workspace_name))
                self.target_workspace(workspace_name)
            else:
                raise Exception(res_json['error'])
        else:
            raise Exception(res.text)

    def delete_workspace(self):
        """
        Delete the target workspace.
        """
        self._check_workspace()
        log.info('Deleting workspace {}'.format(self._workspace))
        url = '/'.join([self._api_endpoint, 'workspace', self._workspace])
        res = requests.delete(url, auth=self._auth, json={})

        if res.ok:
            log.info('Ok -- Workspace {} deleted'.format(self._workspace))
            self._workspace = None
        else:
            raise Exception(res.text)

    def add_event_source(self, event_source: EventSource, overwrite: Optional[bool] = False):
        """
        Add an event source to the target workspace.
        :param event_source: Instance of EventSource containing the specifications of the event source.
        :param overwrite: True for overwriting the event source if it already exists.
        """
        self._check_workspace()

        if event_source.name is None:
            log.warning('Event source name is "{}" -- Changed to "{}" instead'.format(event_source.name, self._workspace))
            event_source.name = self._workspace

        log.info('Adding event source {} to workspace {}'.format(event_source.name, self._workspace))
        url = '/'.join([self._api_endpoint, 'workspace', self._workspace, 'eventsource'])
        res = requests.post(url, auth=self._auth, params={'overwrite': overwrite},
                            json={'event_source': event_source.get_json_eventsource()})

        if res.ok:
            log.info('Ok -- Event source {} added to workspace'.format(event_source.name, self._workspace))
        else:
            raise Exception(res.text)

    def get_event_source(self, event_source_name: str) -> dict:
        """
        Retrieve an event source metadata from the target workspace.
        :param event_source_name: Event source name
        :return: Event source schema
        """
        self._check_workspace()

        log.info('Getting event source {} from workspace {}'.format(event_source_name, self._workspace))
        url = '/'.join([self._api_endpoint, 'workspace', self._workspace, 'eventsource', event_source_name])
        res = requests.get(url, auth=self._auth, json={})

        if res.ok:
            log.info('Ok -- Got event source {} from workspace'.format(event_source_name, self._workspace))
            res_json = res.json()
            return res_json[event_source_name]
        else:
            raise Exception(res.text)

    def list_event_sources(self) -> List[str]:
        """
        Retrieve the list of current event sources from the target workspace.
        :return: List of the event source names.
        """
        self._check_workspace()

        log.info('Listing event sources from workspace {}'.format(self._workspace))
        url = '/'.join([self._api_endpoint, 'workspace', self._workspace, 'eventsource'])
        res = requests.get(url, auth=self._auth, json={})

        if res.ok:
            log.info('Ok -- Event source list retrieved from workspace {}'.format(self._workspace))
            return res.json()
        else:
            raise Exception(res.text)

    def delete_event_source(self, event_source_name: str):
        """
        Delete an event source form the target workspace given its name.
        :param event_source_name: Event source name to delete.
        """
        self._check_workspace()

        log.info('Deleting event source {} from workspace {}'.format(event_source_name, self._workspace))
        url = '/'.join([self._api_endpoint, 'workspace', self._workspace, 'eventsource', event_source_name])
        res = requests.delete(url, auth=self._auth, json={})

        if res.ok:
            log.info('Ok -- Event source {} deleted from workspace {}'.format(event_source_name, self._workspace))
        else:
            raise Exception(res.text)

    def add_trigger(self,
                    event: Union[object, List[object]],
                    trigger_id: Optional[str] = None,
                    condition: Optional[ConditionActionModel] = DefaultConditions.TRUE,
                    action: Optional[ConditionActionModel] = DefaultActions.PASS,
                    context: Optional[dict] = None,
                    context_parser: Optional[str] = None,
                    transient: Optional[bool] = True):
        """
        Add a trigger to the target workspace.
        :param event: Instance of cloudevents.sdk.event.v1.Event (this CloudEvent's subject and type will fire this trigger).
        :param trigger_id: Trigger ID.
        :param condition: Callable that is executed every time the event is received. If it returns true,
        then Action is executed. It has to satisfy signature contract (context, event).
        :param action: Action to perform when event is received and condition is True. It has to satisfy signature
        contract (context, event).
        :param context: Trigger key-value state, only visible for this specific trigger.
        :param context_parser: Parer used to decode context data struct.
        :param transient: If true, this trigger is deleted after action is executed.
        """
        self._check_workspace()

        # Check for arguments types
        if context is None:
            context = {}
        if type(context) is not dict:
            raise Exception('Context must be an json-serializable dict')

        events = [event] if type(event) is not list else event
        # Check if CloudEvents contain mandatory 'subject' and 'type' attributes
        for event in events:
            subject, _ = event.Get('subject')
            event_type, _ = event.Get('type')
            if None in {subject, event_type}:
                raise Exception('\'Subject\' and \'Type\' CloudEvent attributes are required')

        trigger = {
            'id': trigger_id,
            'condition': condition.value,
            'action': action.value,
            'context': context,
            'context_parser': context_parser,
            'activation_events': [json.loads(e.MarshalJSON(json.dumps).read().decode('utf-8')) for e in events],
            'transient': transient}

        triggers = [trigger] if type(trigger) != list else trigger

        log.info('Adding trigger {} to workspace {}'.format(trigger_id, self._workspace))
        url = '/'.join([self._api_endpoint, 'workspace', self._workspace, 'trigger'])
        res = requests.post(url, auth=self._auth, json={'triggers': triggers})

        if res.ok:
            res_json = res.json()
            if len(res_json['accepted_triggers']) != 1:
                raise Exception(res_json['rejected_triggers'].pop()['reason'])

            accepted_trigger = res_json['accepted_triggers'].pop()
            log.info('Ok -- Trigger {} added to workspace {} -- UUID: {}'.format(trigger_id,
                                                                                 self._workspace,
                                                                                 accepted_trigger['uuid']))
            return res_json
        else:
            raise Exception(res.text)

    def get_trigger(self, trigger_id: str):
        """
        Retrieve a trigger from the target workspace given its ID.
        :param trigger_id:
        :return:
        """
        self._check_workspace()
        log.info('Getting trigger {} from workspace'.format(trigger_id, self._workspace))

        url = '/'.join([self._api_endpoint, 'workspace', self._workspace, 'trigger', trigger_id])
        res = requests.get(url, auth=self._auth, json={})

        if res.ok:
            log.info('Ok -- Trigger {} retrieved from {} trigger storage'.format(trigger_id, self._workspace))
            res_json = res.json()
            return res_json[trigger_id]
        else:
            raise Exception(res.text)

    def list_triggers(self) -> List[str]:
        """
        List all trigger IDs from the target workspace.
        :return: List of the trigger IDs currently in the workspace.
        """
        self._check_workspace()
        log.info('Listing triggers for workspace {}'.format(self._workspace))

        url = '/'.join([self._api_endpoint, 'workspace', self._workspace, 'trigger'])
        res = requests.get(url, auth=self._auth, json={})

        if res.ok:
            log.info('Ok -- Trigger ID list retrieved from workspace {}'.format(self._workspace))
            return res.json()
        else:
            raise Exception(res.text)

    def delete_trigger(self, trigger_id: str):
        """
        Deletes a trigger from the target workspace.
        :param trigger_id: Trigger ID to be deleted.
        """
        self._check_workspace()
        log.info('Deleting trigger {} from workspace'.format(trigger_id, self._workspace))

        url = '/'.join([self._api_endpoint, 'workspace', self._workspace, 'trigger', trigger_id])
        res = requests.delete(url, auth=self._auth, json={})

        if res.ok:
            log.info('Ok -- Trigger {} deleted from workspace'.format(trigger_id, self._workspace))
        else:
            raise Exception(res.text)

    def flush_triggers(self):
        """
        Deletes all triggers from the target workspace.
        """
        log.info('Deleting all triggers from workspace {}'.format(self._workspace))

        trigger_ids = self.list_triggers()

        for trigger_id in trigger_ids:
            self.delete_trigger(trigger_id)

        log.info('Ok -- Deleted all triggers from workspace {}'.format(self._workspace))


class TriggerflowCachedClient(TriggerflowClient):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.__trigger_cache = {}

    def add_trigger(self,
                    event: Union[object, List[object]],
                    trigger_id: Optional[str] = None,
                    condition: Optional[ConditionActionModel] = DefaultConditions.TRUE,
                    action: Optional[ConditionActionModel] = DefaultActions.PASS,
                    context: Optional[dict] = None,
                    context_parser: Optional[str] = None,
                    transient: Optional[bool] = True):
        """
        Add a trigger to the local cache, instead of requesting the addition to the Trigger API directly.
        :param event: Instance of CloudEvent (this CloudEvent's subject and type will fire this trigger).
        :param trigger_id: Trigger ID.
        :param condition: Callable that is executed every time the event is received. If it returns true,
        then Action is executed. It has to satisfy signature contract (context, event).
        :param action: Action to perform when event is received and condition is True. It has to satisfy signature
        contract (context, event).
        :param context: Trigger key-value state, only visible for this specific trigger.
        :param context_parser: Parser used to decode the context data struct.
        :param transient: If true, this trigger is deleted after action is executed.
        """
        self._check_workspace()

        if trigger_id is None:
            raise Exception('Cached trigger must be provided with an ID')

        if context is None:
            context = {}

        log.info('Adding trigger {} to local cache'.format(trigger_id))
        if trigger_id not in self.__trigger_cache:
            events = [event] if not isinstance(event, list) else event
            # Check if CloudEvents contain mandatory 'subject' and 'type' attributes
            for event in events:
                subject, _ = event.Get('subject')
                event_type, _ = event.Get('type')
                if None in (subject, event_type):
                    raise Exception('\'Subject\' and \'Type\' CloudEvent attributes are required')
            trigger = {
                'id': trigger_id,
                'condition': condition.value,
                'action': action.value,
                'context': context.copy(),
                'context_parser': context_parser,
                'activation_events': [json.loads(e.MarshalJSON(json.dumps).read().decode('utf-8')) for e in events],
                'transient': transient}
            self.__trigger_cache[trigger_id] = json.dumps(trigger)
        else:
            raise Exception('{} already in cache'.format(trigger_id))

    def trigger_exists(self, trigger_id: str):
        """
        Check if a trigger exists in the local cache given its ID.
        :param trigger_id: Trigger ID to check.
        :return: True if exists, else False.
        """
        return trigger_id in self.__trigger_cache

    def get_trigger(self, trigger_id: str):
        """
        Retrieve a trigger from the local cache.
        :param trigger_id: Trigger ID to retrieve.
        :return: Trigger.
        """
        if trigger_id in self.__trigger_cache:
            return json.loads(self.__trigger_cache[trigger_id])
        else:
            raise Exception('Trigger {} not found'.format(trigger_id))

    def update_trigger(self, trigger_json):
        if 'id' not in trigger_json:
            raise Exception('trigger_id not in trigger JSON definition')

        trigger_id = trigger_json['id']
        trg = json.loads(self.__trigger_cache[trigger_id])
        trg.update(trigger_json)
        self.__trigger_cache[trigger_id] = json.dumps(trg)

    def list_triggers(self) -> List[str]:
        """
        List all trigger IDs in the local cache.
        :return: List of all trigger IDs in the local cache.
        """
        return list(self.__trigger_cache.keys())

    def delete_trigger(self, trigger_id: str):
        """
        Delete a trigger from the local cache given its ID.
        :param trigger_id: Trigger ID to delete.
        """
        if trigger_id in self.__trigger_cache:
            del self.__trigger_cache[trigger_id]
        else:
            raise Exception('Trigger {} not found'.format(trigger_id))

    def flush_triggers(self):
        """
        Delete all triggers from the local cache.
        """
        self.__trigger_cache = {}

    def commit_cached_triggers(self):
        """
        Pushes and applies all cached triggers to the remote trigger storage and workspace.
        """
        self._check_workspace()

        triggers = [json.loads(trigger) for trigger in self.__trigger_cache.values()]

        log.info('Adding cached triggers to workspace {}'.format(self._workspace))
        url = '/'.join([self._api_endpoint, 'workspace', self._workspace, 'trigger'])
        res = requests.post(url, auth=self._auth, json={'triggers': triggers})

        if res.ok:
            res_json = res.json()
            for accepted_trigger in res_json['accepted_triggers']:
                log.info('Ok -- Trigger {} added to workspace {} -- UUID: {}'.format(accepted_trigger['id'],
                                                                                     self._workspace,
                                                                                     accepted_trigger['uuid']))
            for rejected_trigger in res_json['rejected_triggers']:
                print(rejected_trigger)
                log.warning('Rejected trigger {} -- Reason: {}'.format(rejected_trigger['id'], rejected_trigger['reason']))
        else:
            raise Exception(res.text)
