from uuid import uuid4
from enum import Enum
from platform import node

from triggerflow import Triggerflow, TriggerflowCachedClient
from triggerflow.config import get_config
from triggerflow.libs.cloudevents.sdk.event import v1
from triggerflow.eventsources.sqs import SQSEventSource
from triggerflow.functions import ConditionActionModel, DefaultActions


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

    eventsource = SQSEventSource(access_key_id=credentials['access_key_id'],
                                 secret_access_key=credentials['secret_access_key'],
                                 queue=run_id)

    triggerflow_client = TriggerflowCachedClient()
    triggerflow_client.create_workspace(workspace_name=run_id,
                                        event_source=eventsource,
                                        global_context={})

    statemachine_count = 0

    ###################################
    def statemachine(states, trigger_event):
        nonlocal statemachine_count
        statemachine_id = 'StateMachine{}'.format(statemachine_count)
        statemachine_count += 1

        upstream_relatives = {}
        final_states = []
        choices = {}

        for state_name, state in states['States'].items():
            if 'End' in state and state['End']:
                final_states.append(state_name)
            elif 'Next' in state:
                upstream_relatives[state['Next']] = state_name
            elif state['Type'] == 'Choice':
                for choice in state['Choices']:
                    upstream_relatives[choice['Next']] = state_name

        upstream_relatives[states['StartAt']] = trigger_event

        for state_name, state in states['States'].items():
            context = {'subject': state_name, 'State': state.copy()}

            if state_name in choices:
                context['Condition'] = choices[state_name].copy()

            if state['Type'] == 'Pass' or state['Type'] == 'Task':

                activation_event = (
                    v1.Event()
                        .SetSubject(upstream_relatives[state_name])
                        .SetEventType('event.triggerflow.termination.success')
                )

                action = AwsAsfActions.AWS_ASF_TASK if state['Type'] == 'Task' else AwsAsfActions.AWS_ASF_PASS

                triggerflow_client.add_trigger(event=activation_event,
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
                    sub_sm_id = statemachine(branch, upstream_relatives[state_name])
                    sub_state_machines.append(sub_sm_id)

                context['join_multiple'] = len(state['Branches'])

                activation_events = []
                for sub_state_machine_id in sub_state_machines:
                    activation_event = (
                        v1.Event()
                            .SetSubject(sub_state_machine_id)
                            .SetEventType('event.triggerflow.termination.success')
                    )
                    activation_events.append(activation_event)

                triggerflow_client.add_trigger(event=activation_events,
                                               condition=AwsAsfConditions.AWS_ASF_JOIN_STATEMACHINE,
                                               action=AwsAsfActions.AWS_ASF_PASS,
                                               context=context,
                                               trigger_id=state_name,
                                               transient=False)

            elif state['Type'] == 'Wait':
                raise NotImplementedError()

            elif state['Type'] == 'Map':
                iterator = statemachine(state['Iterator'], state_name)
                context['join_state_machine'] = iterator

                activation_event = (
                    v1.Event()
                    .SetSubject(upstream_relatives[state_name])
                    .SetEventType('event.triggerflow.termination.success')
                )

                triggerflow_client.add_trigger(activation_event,
                                               condition=AwsAsfConditions.AWS_ASF_CONDITION,
                                               action=AwsAsfActions.AWS_ASF_MAP,
                                               context=context,
                                               trigger_id=state_name,
                                               transient=False)
                upstream_relatives[state['Next']] = iterator

            elif state['Type'] == 'Succeed':
                raise NotImplementedError()
            elif state['Type'] == 'Fail':
                raise NotImplementedError()

        activation_events = []
        for final_state in final_states:
            activation_event = (
                v1.Event()
                .SetSubject(final_state)
                .SetEventType('event.triggerflow.termination.success')
            )
            activation_events.append(activation_event)
        triggerflow_client.add_trigger(activation_events,
                                       condition=AwsAsfConditions.AWS_ASF_JOIN_STATEMACHINE,
                                       action=AwsAsfActions.AWS_ASF_END_STATEMACHINE,
                                       context={'subject': statemachine_id},
                                       trigger_id=statemachine_id,
                                       transient=False)

        return statemachine_id
    ###################################

    main_statemachine = statemachine(statemachine_json, '$init')

    activation_event = (
        v1.Event()
            .SetSubject(main_statemachine)
            .SetEventType('event.triggerflow.termination.success')
    )
    triggerflow_client.add_trigger(event=activation_event,
                                   condition=AwsAsfConditions.AWS_ASF_JOIN_STATEMACHINE,
                                   action=DefaultActions.TERMINATE,
                                   trigger_id='$end',
                                   transient=False)

    triggerflow_client.commit_cached_triggers()

    return run_id


def trigger_statemachine(run_id: str):
    config = get_config()
    credentials = config['statemachines']['aws']

    event_source = SQSEventSource(access_key_id=credentials['access_key_id'],
                                  secret_access_key=credentials['secret_access_key'],
                                  queue=run_id)
    uuid = uuid4()
    init_cloudevent = (
        v1.Event()
        .SetSubject('__init__')
        .SetEventType('event.triggerflow.init')
        .SetID(uuid4.hex)
        .SetSource(f'urn:{node()}:{str(uuid)}')
    )
    event_source.publish_cloudevent(init_cloudevent)
