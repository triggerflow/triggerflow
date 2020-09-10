import boto3
import json
from uuid import uuid4
from platform import node

from jsonpath_ng import parse
from datetime import datetime


def action_aws_asf_pass(context, event):
    input = {}
    result = {}

    if 'State' in context:
        if 'InputPath' in context['State']:
            exp = parse(context['State']['InputPath'])
            matches = [x.value for x in exp.find(event['data'])]
            if len(matches) == 1:
                input = matches.pop()
            else:
                input = matches

        if 'Result' in context['State']:
            for key, value in context['State']['Result']:
                if value.startswith('$'):
                    exp = parse(value)
                    result[key] = {}
                    exp.update(result[key], input)

        if 'ResultPath' in context['State']:
            key = context['State']['ResultPath'].split('.')[1]
            result[key] = {}
            exp = parse(context['State']['ResultPath'])
            exp.update(result, input)

    uuid = uuid4()
    termination_cloudevent = {'specversion': '1.0',
                              'id': uuid.hex,
                              'source': f'urn:{node()}:{str(uuid)}',
                              'type': 'lambda.success',
                              'time': str(datetime.utcnow().isoformat()),
                              'subject': context['Subject'],
                              'datacontenttype': 'application/json',
                              'data': result}

    context.local_event_queue.put(termination_cloudevent)


def action_aws_asf_task(context, event):
    if 'lambda' not in context['State']['Resource']:
        raise NotImplementedError()

    lambda_client = boto3.client('lambda',
                                 aws_access_key_id=context.global_context['aws_credentials']['access_key_id'],
                                 aws_secret_access_key=context.global_context['aws_credentials']['secret_access_key'],
                                 region_name=context.global_context['aws_credentials']['region'])

    invoke_args = {}
    if 'Parameters' in context['State']:
        for parameter_key, parameter_value in context['State']['Parameters'].items():
            if parameter_key.endswith('.$'):
                key = parameter_key[:-2]
                exp = parse(parameter_value)
                match = exp.find(event['data'])
                if len(match) == 1:
                    invoke_args[key] = match.pop().value
                elif len(match) > 1:
                    invoke_args[key] = [m.value for m in match]
                else:
                    invoke_args[key] = None
            else:
                invoke_args[parameter_key] = parameter_value

    invoke_args['__TRIGGERFLOW_SUBJECT'] = context['Subject']

    lambda_client.invoke(FunctionName=context['State']['Resource'],
                         InvocationType='Event',
                         Payload=json.dumps(invoke_args))


def action_aws_asf_map(context, event):
    if 'InputPath' in context['State']:
        exp = parse(context['State']['InputPath'])
        match = exp.find(event['data'])
        input = match.pop().value
    else:
        input = event['data']

    if 'ItemsPath' in context['State']:
        exp = parse(context['State']['ItemsPath'])
        match = exp.find(input)
        iterator = match.pop().value
    else:
        iterator = input

    if not isinstance(iterator, list):
        raise Exception('`ItemsPath` must be iterable')

    join_sm = context['join_state_machine']
    context.triggers[join_sm].context['join_multiple'] = len(iterator)

    for element in iterator:
        uuid = uuid4()
        termination_cloudevent = {'specversion': '1.0',
                                  'id': uuid.hex,
                                  'source': f'urn:{node()}:{str(uuid)}',
                                  'type': 'lambda.success',
                                  'time': str(datetime.utcnow().isoformat()),
                                  'subject': context['Subject'],
                                  'datacontenttype': 'application/json',
                                  'data': element}

        context.local_event_queue.put(termination_cloudevent)


def action_aws_asf_end_statemachine(context, event):
    uuid = uuid4()
    termination_cloudevent = {'specversion': '1.0',
                              'id': uuid.hex,
                              'source': f'urn:{node()}:{str(uuid)}',
                              'type': 'lambda.success',
                              'time': str(datetime.utcnow().isoformat()),
                              'subject': context['Subject']}

    context.local_event_queue.put(termination_cloudevent)
