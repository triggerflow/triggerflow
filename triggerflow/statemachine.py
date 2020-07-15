import boto3
import logging
from arnparse import arnparse
from uuid import uuid4
from enum import Enum
from platform import node
from collections import defaultdict

from triggerflow import TriggerflowCachedClient, CloudEvent
from triggerflow.config import get_config
from triggerflow.eventsources.sqs import SQSEventSource
from triggerflow.functions import ConditionActionModel, DefaultActions


log = logging.getLogger('triggerflow')
log.setLevel(logging.DEBUG)


class AwsAsfConditions(ConditionActionModel, Enum):
    AWS_ASF_JOIN_STATEMACHINE = {'name': 'aws_asf_join_statemachine'}
    AWS_ASF_CONDITION = {'name': 'aws_asf_condition'}


class AwsAsfActions(ConditionActionModel, Enum):
    AWS_ASF_PASS = {'name': 'aws_asf_pass'}
    AWS_ASF_TASK = {'name': 'aws_asf_task'}
    AWS_ASF_CHOICE = {'name': 'aws_asf_choice'}
    AWS_ASF_PARALLEL = {'name': 'aws_asf_parallel'}
    AWS_ASF_MAP = {'name': 'aws_asf_map'}
    AWS_ASF_END_STATEMACHINE = {'name': 'aws_asf_end_statemachine'}


def deploy_state_machine(statemachine_json: dict):
    uuid = str(uuid4())
    run_id = 'sm-' + uuid[24:]

    config = get_config()
    credentials = config['statemachines']['aws']

    event_source = SQSEventSource(name=run_id + '_' + 'SQSEventSource',
                                  access_key_id=credentials['access_key_id'],
                                  secret_access_key=credentials['secret_access_key'],
                                  region=credentials['region'],
                                  queue=run_id)

    # Create the SQS queue where the termination events will be sent
    queue_arn = create_sqs_queue(run_id, credentials)

    triggerflow_client = TriggerflowCachedClient()
    triggerflow_client.create_workspace(workspace_name=run_id,
                                        event_source=event_source,
                                        global_context={'aws_credentials': credentials})

    state_machine_count = 0
    lambda_client = boto3.client('lambda',
                                 aws_access_key_id=credentials['access_key_id'],
                                 aws_secret_access_key=credentials['secret_access_key'],
                                 region_name=credentials['region'])

    ###################################
    def state_machine(states, trigger_event):
        nonlocal state_machine_count, lambda_client, queue_arn
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
                activation_events = [CloudEvent().SetEventType('lambda.success').SetSubject(sub) for sub in subjects]

                action = AwsAsfActions.AWS_ASF_TASK if state['Type'] == 'Task' else AwsAsfActions.AWS_ASF_PASS

                if state['Type'] == 'Task':
                    add_destination_to_lambda(state['Resource'], queue_arn, lambda_client)

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

                act_events = [CloudEvent().SetEventType('lambda.success').SetSubject(sub_sm) for sub_sm in sub_state_machines]

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
                activation_events = [CloudEvent().SetEventType('lambda.success').SetSubject(sub) for sub in subjects]

                triggerflow_client.add_trigger(event=activation_events,
                                               condition=AwsAsfConditions.AWS_ASF_CONDITION,
                                               action=AwsAsfActions.AWS_ASF_MAP,
                                               context=context,
                                               trigger_id=state_name,
                                               transient=False)
                if 'Next' in state:
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

    main_state_machine = state_machine(statemachine_json, ['__init__'])

    final_trigger = {'id': main_state_machine, 'action': DefaultActions.TERMINATE.value}
    triggerflow_client.update_trigger(final_trigger)

    triggerflow_client.commit_cached_triggers()

    return run_id


def trigger_statemachine(run_id: str):
    config = get_config()
    credentials = config['statemachines']['aws']

    event_source = SQSEventSource(access_key_id=credentials['access_key_id'],
                                  secret_access_key=credentials['secret_access_key'],
                                  region=credentials['region'],
                                  queue=run_id)
    uuid = uuid4()
    init_cloudevent = (CloudEvent()
                       .SetSubject('__init__')
                       .SetEventType('lambda.success')
                       .SetEventID(uuid.hex)
                       .SetSource(f'urn:{node()}:{str(uuid)}'))
    event_source.publish_cloudevent(init_cloudevent)


def create_sqs_queue(queue_name, credentials):
    sqs_client = boto3.client('sqs',
                              aws_access_key_id=credentials['access_key_id'],
                              aws_secret_access_key=credentials['secret_access_key'],
                              region_name=credentials['region'])

    response = sqs_client.create_queue(QueueName=queue_name)

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


def add_destination_to_lambda(lambda_arn, source_arn, lambda_client):
    arn = arnparse(lambda_arn)
    if 'lambda' != arn.service:
        raise Exception(('Resource of type {} is not currently supported, '
                         'as it cannot produce termination events').format(arn.service))

    log.debug('Updating Lambda Destination for {}'.format(lambda_arn))
    lambda_client.put_function_event_invoke_config(DestinationConfig={'OnSuccess': {'Destination': source_arn}},
                                                   FunctionName=lambda_arn)
