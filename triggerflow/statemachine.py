import boto3
import logging
import json
from arnparse import arnparse
from uuid import uuid4
from enum import Enum
from platform import node
from collections import defaultdict

from triggerflow import TriggerflowCachedClient, CloudEvent
from triggerflow.config import get_config
from triggerflow.eventsources.model import EventSource
from triggerflow.eventsources.sqs import SQSEventSource
from triggerflow.functions import ConditionActionModel, DefaultActions

log = logging.getLogger('triggerflow')
log.setLevel(logging.DEBUG)


class AwsAsfConditions(ConditionActionModel, Enum):
    AWS_ASF_JOIN_STATEMACHINE = {'name': 'AWS_ASF_JOIN_STATEMACHINE'}
    AWS_ASF_CONDITION = {'name': 'AWS_ASF_CONDITION'}


class AwsAsfActions(ConditionActionModel, Enum):
    AWS_ASF_PASS = {'name': 'AWS_ASF_PASS'}
    AWS_ASF_TASK = {'name': 'AWS_ASF_TASK'}
    AWS_ASF_MAP = {'name': 'AWS_ASF_MAP'}
    AWS_ASF_END_STATEMACHINE = {'name': 'AWS_ASF_END_STATEMACHINE'}


class StateMachine:
    def __init__(self):
        self.state_machine = None
        self.run_id = None
        self.event_source = None
        self.config = get_config()
        self.credentials = self.config['statemachines']['aws']
        self.lambda_client = boto3.client('lambda',
                                          aws_access_key_id=self.credentials['access_key_id'],
                                          aws_secret_access_key=self.credentials['secret_access_key'],
                                          region_name=self.credentials['region'])

    @classmethod
    def json(cls, file_path: str, event_source: EventSource = None):
        sm = cls()
        with open(file_path, 'r') as json_file:
            sm_json = json.loads(json_file.read())
        sm.state_machine = sm_json
        sm.event_source = event_source
        sm.__deploy_state_machine()
        return sm

    @classmethod
    def string(cls, state_machine_json: str, event_source: EventSource = None):
        sm = cls()
        sm_json = json.loads(state_machine_json)
        sm.state_machine = sm_json
        sm.event_source = event_source
        sm.__deploy_state_machine()
        return sm

    def __deploy_state_machine(self):
        uuid = str(uuid4())
        self.run_id = 'sm-' + uuid[24:]
        # self.run_id = 'sm-test'

        if self.event_source is None:
            # Create the SQS queue where the termination events will be sent
            self.event_source = SQSEventSource(name=self.run_id + '_' + 'SQSEventSource',
                                               access_key_id=self.credentials['access_key_id'],
                                               secret_access_key=self.credentials['secret_access_key'],
                                               region=self.credentials['region'],
                                               queue=self.run_id)
            queue_arn = self.__create_sqs_queue()
            lambdas_updated = {}
        else:
            self.event_source.set_stream(self.run_id)
            self.event_source.name = self.run_id
            queue_arn, lambdas_updated = None, None

        triggerflow_client = TriggerflowCachedClient()

        global_context = {'aws_credentials': self.credentials}
        if not isinstance(self.event_source, SQSEventSource):
            global_context['event_source'] = self.event_source.get_json_eventsource()

        triggerflow_client.create_workspace(workspace_name=self.run_id,
                                            event_source=self.event_source,
                                            global_context=global_context)

        state_machine_count = 0

        ###################################
        def state_machine(states, trigger_event):
            nonlocal state_machine_count, queue_arn, lambdas_updated
            state_machine_id = 'StateMachine{}'.format(state_machine_count)
            state_machine_count += 1

            upstream_relatives = defaultdict(list)
            final_states = []
            choices = {}

            for state_name, state in states['States'].items():
                if 'End' in state and state['End']:
                    final_states.append(state_name)
                elif 'Next' in state:
                    upstream_relatives[state['Next']].append(state_name)
                elif state['Type'] == 'Choice':
                    for choice in state['Choices']:
                        upstream_relatives[choice['Next']].append(state_name)

            upstream_relatives[states['StartAt']].extend(trigger_event)

            for state_name, state in states['States'].items():
                context = {'Subject': state_name, 'State': state.copy()}

                if state_name in choices:
                    context['Condition'] = choices[state_name].copy()

                if state['Type'] == 'Pass' or state['Type'] == 'Task':

                    subjects = upstream_relatives[state_name]
                    activation_events = [CloudEvent().SetEventType('lambda.success').SetSubject(sub) for sub in
                                         subjects]

                    action = AwsAsfActions.AWS_ASF_TASK if state['Type'] == 'Task' else AwsAsfActions.AWS_ASF_PASS

                    if state['Type'] == 'Task' and isinstance(self.event_source, SQSEventSource) and \
                            state['Resource'] not in lambdas_updated:
                        self.__add_destination_to_lambda(state['Resource'], queue_arn)
                        lambdas_updated[state['Resource']] = True

                    triggerflow_client.add_trigger(event=activation_events,
                                                   condition=AwsAsfConditions.AWS_ASF_CONDITION,
                                                   action=action,
                                                   context=context,
                                                   trigger_id=state_name,
                                                   transient=False)

                elif state['Type'] == 'Choice':
                    choices = {}
                    for choice in state['Choices']:
                        upstream_relatives[choice['Next']] = upstream_relatives[state_name]
                        choices[choice['Next']] = choice.copy()

                elif state['Type'] == 'Parallel':
                    sub_state_machines = []

                    for branch in state['Branches']:
                        sub_sm_id = state_machine(branch, upstream_relatives[state_name])
                        sub_state_machines.append(sub_sm_id)

                    context['join_multiple'] = len(state['Branches'])
                    del context['State']

                    act_events = [CloudEvent().SetEventType('lambda.success').SetSubject(sub_sm) for sub_sm in
                                  sub_state_machines]

                    triggerflow_client.add_trigger(event=act_events,
                                                   condition=AwsAsfConditions.AWS_ASF_JOIN_STATEMACHINE,
                                                   action=AwsAsfActions.AWS_ASF_PASS,
                                                   context=context,
                                                   trigger_id=state_name,
                                                   transient=False)

                elif state['Type'] == 'Wait':
                    raise NotImplementedError()

                elif state['Type'] == 'Map':
                    iterator = state_machine(state['Iterator'], [state_name])
                    context['join_state_machine'] = iterator
                    del context['State']['Iterator']

                    subjects = upstream_relatives[state_name]
                    activation_events = [CloudEvent().SetEventType('lambda.success').SetSubject(sub) for sub in
                                         subjects]

                    triggerflow_client.add_trigger(event=activation_events,
                                                   condition=AwsAsfConditions.AWS_ASF_CONDITION,
                                                   action=AwsAsfActions.AWS_ASF_MAP,
                                                   context=context,
                                                   trigger_id=state_name,
                                                   transient=False)
                    if 'Next' in state:
                        upstream_relatives[state['Next']].remove(state_name)
                        upstream_relatives[state['Next']].append(iterator)
                    if 'End' in state:
                        final_states.remove(state_name)
                        final_states.append(iterator)

                elif state['Type'] == 'Succeed':
                    raise NotImplementedError()
                elif state['Type'] == 'Fail':
                    raise NotImplementedError()

            activation_events = [CloudEvent().SetEventType('lambda.success').SetSubject(sub) for sub in final_states]
            triggerflow_client.add_trigger(activation_events,
                                           condition=AwsAsfConditions.AWS_ASF_JOIN_STATEMACHINE,
                                           action=AwsAsfActions.AWS_ASF_END_STATEMACHINE,
                                           context={'Subject': state_machine_id},
                                           trigger_id=state_machine_id,
                                           transient=False)

            return state_machine_id

        ###################################

        main_state_machine = state_machine(self.state_machine, ['__init__'])

        final_trigger = {'id': main_state_machine, 'action': DefaultActions.TERMINATE.value}
        triggerflow_client.update_trigger(final_trigger)

        triggerflow_client.commit_cached_triggers()

        return self.run_id

    def trigger(self, execution_input: dict = None):
        log.info("Trigger State Machine Execution: {}".format(self.run_id))

        if execution_input is None:
            execution_input = {}

        uuid = uuid4()
        init_cloudevent = (CloudEvent()
                           .SetSubject('__init__')
                           .SetEventType('lambda.success')
                           .SetEventID(uuid.hex)
                           .SetSource(f'urn:{node()}:{str(uuid)}'))

        if execution_input is not None:
            init_cloudevent.SetData(execution_input)
            init_cloudevent.SetContentType('application/json')

        self.event_source.publish_cloudevent(init_cloudevent)
        log.info("Ok")

    def __create_sqs_queue(self):
        credentials = self.config['statemachines']['aws']

        sqs_client = boto3.client('sqs',
                                  aws_access_key_id=credentials['access_key_id'],
                                  aws_secret_access_key=credentials['secret_access_key'],
                                  region_name=credentials['region'])

        response = sqs_client.create_queue(QueueName=self.run_id)

        if 'QueueUrl' in response:
            queue_url = response['QueueUrl']
            log.debug('Queue URL: {}'.format(queue_url))
        else:
            raise Exception(response)

        response = sqs_client.get_queue_attributes(QueueUrl=queue_url,
                                                   AttributeNames=['QueueArn'])
        if 'Attributes' in response and 'QueueArn' in response['Attributes']:
            queue_arn = response['Attributes']['QueueArn']
            log.debug('Queue ARN: {}'.format(queue_arn))
        else:
            raise Exception('Could not retrieve Queue ARN from {}'.format(queue_url))

        return queue_arn

    def __add_destination_to_lambda(self, lambda_arn, source_arn):
        arn = arnparse(lambda_arn)
        if 'lambda' != arn.service:
            raise Exception(('Resource of type {} is not currently supported, '
                             'as it cannot produce termination events').format(arn.service))

        log.debug('Updating Lambda Destination for {}'.format(lambda_arn))
        self.lambda_client.put_function_event_invoke_config(
            DestinationConfig={'OnSuccess': {'Destination': source_arn}},
            FunctionName=lambda_arn)


def trigger_statemachine(run_id: str, execution_input: dict = None, event_source: EventSource = None):
    log.info("Trigger State Machine Execution: {}".format(run_id))

    if execution_input is None:
        execution_input = {}

    uuid = uuid4()
    init_cloudevent = (CloudEvent()
                       .SetSubject('__init__')
                       .SetEventType('lambda.success')
                       .SetEventID(uuid.hex)
                       .SetSource(f'urn:{node()}:{str(uuid)}'))

    if execution_input is not None:
        init_cloudevent.SetData(execution_input)
        init_cloudevent.SetContentType('application/json')

    if event_source is None:
        credentials = get_config()['statemachines']['aws']
        event_source = SQSEventSource(name=run_id + '_' + 'SQSEventSource',
                                      access_key_id=credentials['access_key_id'],
                                      secret_access_key=credentials['secret_access_key'],
                                      region=credentials['region'],
                                      queue=run_id)

    event_source.set_stream(run_id)
    event_source.publish_cloudevent(init_cloudevent)
    log.info("Ok")
