import logging
import time
import docker
import json
import tarfile
import io
import dill
import boto3
import urllib3
from base64 import b64decode
from concurrent.futures import ThreadPoolExecutor
from urllib3.exceptions import InsecureRequestWarning
import requests
from requests.auth import HTTPBasicAuth

urllib3.disable_warnings(InsecureRequestWarning)


def action_pass(context, event):
    pass


def action_terminate(context, event):
    pass


def action_docker_image(context, event):
    client = docker.from_env()

    context_copy = context.copy()

    print(client.images.list())

    for k, v in context_copy['triggers'].items():
        del v['context']['triggers']

    env = {'CLASS': context['action']['class_name'],
           'EVENT': json.dumps(event),
           'CONTEXT': json.dumps(context_copy)}
    container = client.containers.create(context['action']['image'], environment=env)
    container.start()
    container.wait()
    output, _ = container.get_archive('/out.json')

    res = {}

    for buffer in output:
        tar = tarfile.open(mode="r|", fileobj=io.BytesIO(buffer))
        for l in tar:
            x = tar.extractfile(l)
            res = json.loads(x.read())

    context.update(res['context'])
    return res['result']


def action_python_callable(context, event):
    decoded_callable = b64decode(context['action']['callable'].encode('utf-8'))
    f = dill.loads(decoded_callable)

    result = f(context=context, event=event)

    return result


ibmcf_session = None


def create_ibmcf_session():
    global ibmcf_session
    ibmcf_session = requests.session()
    adapter = requests.adapters.HTTPAdapter()
    ibmcf_session.mount('https://', adapter)


def action_ibm_cf_invoke(context, event):

    class InvokeException(Exception):
        pass

    if not ibmcf_session:
        create_ibmcf_session()

    cf_auth = context['global_context']['ibm_cf']['api_key'].split(':')
    cf_auth_handler = HTTPBasicAuth(cf_auth[0], cf_auth[1])

    url = context['function_url']
    trigger_id = context['trigger_id']
    workspace = context['workspace']
    subject = context['subject'] if 'subject' in context else None

    # Transform a callasync trigger to a map trigger of a single function
    if context['kind'] == 'callasync':
        function_args = [context['function_args']]
    else:
        function_args = context['function_args']
    total_activations = len(function_args)

    if 'max_retries' not in context:
        max_retries = 5
    else:
        max_retries = context['max_retries']

    if 'sink' not in context:
        sink = list(context['global_context']['event_sources'].values())[0]
    else:
        sink = context['global_context']['event_sources'][workspace]

    tf_data = {'sink': sink, 'workspace': workspace,
               'trigger_id': trigger_id, 'subject': subject}
    payload = {'__OW_TRIGGERFLOW': tf_data}

    ################################################
    def invoke(call_id, args, payload):
        payload.update(args)
        act_id = None
        retry = True
        retry_count = 0
        while retry:
            try:
                start_t = time.time()
                response = ibmcf_session.post(url, json=payload, auth=cf_auth_handler, verify=False)
                status_code = response.status_code
                res_json = response.json()
                et = round(time.time()-start_t, 3)
                if status_code in range(200, 300) and 'activationId' in res_json and \
                        res_json['activationId'] is not None:
                    retry = False
                    act_id = res_json['activationId']
                    logging.info('[{}][{}] Invocation success ({}s) - Activation ID: {}'.format(workspace, call_id, et, act_id))
                elif status_code in range(400, 500) and status_code not in [408, 409, 429]:
                    logging.error('[{}][{}] Invocation failed - Activation status code: {}'.format(workspace, call_id,
                                                                                                   status_code))
                    raise InvokeException('Invocation failed')
            except requests.exceptions.RequestException as e:
                logging.error('[{}][{}] Error talking to OpenWhisk: {}'.format(workspace, call_id, e))
                raise e
            except Exception as e:
                logging.error("[{}][{}] Exception - {}".format(workspace, call_id, e))
            if retry:
                retry_count += 1
                if retry_count <= max_retries:
                    sleepy_time = pow(2, retry_count)
                    logging.info("[{}][{}] Retrying in {} second(s)".format(workspace, call_id, sleepy_time))
                    time.sleep(sleepy_time)
                else:
                    logging.error("[{}][{}] Retrying failed after {} attempts".format(workspace, call_id, max_retries))
                    raise InvokeException('Invocation failed')

        return call_id, act_id

    ################################################

    logging.info("[{}] Firing trigger {} - Activations: {} ".format(workspace, subject, total_activations))
    futures = []
    responses = []
    if total_activations == 1:
        resp = invoke(0, function_args[0], payload)
        responses.append(resp)
    else:
        with ThreadPoolExecutor(max_workers=128) as executor:
            for cid, args in enumerate(function_args):
                res = executor.submit(invoke, cid, args, payload)
                futures.append(res)
        try:
            responses = [fut.result() for fut in futures]
        except InvokeException:
            pass

    activations_done = [call_id for call_id, _ in responses]
    activations_not_done = [call_id for call_id in range(total_activations) if call_id not in activations_done]

    if subject in context['trigger_events']:
        downstream_triggers = context['trigger_events'][subject]['termination.event.success']
        for downstream_trigger in downstream_triggers:
            if context['triggers'][downstream_trigger]['context']['dependencies'][subject]['join'] > 0:
                context['triggers'][downstream_trigger]['context']['dependencies'][subject]['join'] += total_activations
            else:
                context['triggers'][downstream_trigger]['context']['dependencies'][subject]['join'] = total_activations

    # All activations are unsuccessful
    if not activations_done:
        raise Exception('All invocations are unsuccessful')
    # At least one activation is successful
    else:
        # All activations are successful
        if len(activations_done) == total_activations:
            logging.info('[{}][{}] All invocations successful'.format(workspace, subject))
        # Only some activations are successful
        else:
            logging.info(
                "[{}][{}] Could not be completely triggered - {} activations pending".format(workspace, subject,
                                                                                             len(activations_not_done)))


def action_aws_lambda_invoke(context, event):
    class InvokeException(Exception):
        pass

    # Transform a callasync trigger to a map trigger of a single function
    if context['kind'] == 'callasync':
        invoke_args = [context['invoke_args']]
    else:
        invoke_args = context['invoke_args']
    total_activations = len(invoke_args)

    max_retries = 5

    function_name = context['function_name']
    subject = context['subject']
    workspace = context['workspace']

    lambda_client = boto3.client('lambda')

    ################################################
    def invoke(call_id, args):
        act_id = None
        retry = True
        retry_count = 0
        while retry:
            try:
                response = lambda_client.invoke_async(FunctionName=function_name, InvokeArgs=args)
                status_code = response['ResponseMetadata']['HTTPStatusCode']
                if status_code in range(200, 300):
                    retry = False
                    act_id = response['ResponseMetadata']['RequestId']
                    logging.info('[{}][{}] Invocation success - Activation ID: {}'.format(workspace, call_id, act_id))
                elif status_code in range(400, 500) and status_code not in [408, 409, 429]:
                    logging.error('[{}][{}] Invocation failed - Activation status code: {}'.format(workspace, call_id,
                                                                                                   status_code))
            except Exception as e:
                logging.error("[{}][{}] Exception - {}".format(workspace, call_id, e))
            if retry:
                retry_count += 1
                if retry_count <= max_retries:
                    sleepy_time = pow(2, retry_count)
                    logging.info("[{}][{}] Retrying in {} second(s)".format(workspace, call_id, sleepy_time))
                    time.sleep(sleepy_time)
                else:
                    logging.error("[{}][{}] Retrying failed after {} attempts".format(workspace, call_id, max_retries))
                    raise InvokeException('Invocation failed')

        return call_id, act_id

    ################################################

    logging.info("[{}] Firing trigger {} - Activations: {} ".format(workspace, subject, total_activations))
    futures = []
    with ThreadPoolExecutor(max_workers=512) as executor:
        for cid, args in enumerate(invoke_args):
            res = executor.submit(invoke, cid, args)
            futures.append(res)
    responses = []
    try:
        responses = [fut.result() for fut in futures]
    except InvokeException:
        pass

    activations_done = [call_id for call_id, _ in responses]
    activations_not_done = [call_id for call_id in range(total_activations) if call_id not in activations_done]

    if subject in context['trigger_events']:
        downstream_triggers = context['trigger_events'][subject]['termination.event.success']
        for downstream_trigger in downstream_triggers:
            if 'total_activations' in context['triggers'][downstream_trigger]['context']:
                context['triggers'][downstream_trigger]['context']['total_activations'] += total_activations
            else:
                context['triggers'][downstream_trigger]['context']['total_activations'] = total_activations

    # All activations are unsuccessful
    if not activations_done:
        raise Exception('All invocations are unsuccessful')
    # At least one activation is successful
    else:
        # All activations are successful
        if len(activations_done) == total_activations:
            logging.info('[{}][{}] All invocations successful'.format(workspace, subject))
        # Only some activations are successful
        else:
            logging.info(
                "[{}][{}] Could not be completely triggered - {} activations pending".format(workspace, subject,
                                                                                             len(activations_not_done)))
