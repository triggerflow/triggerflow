import logging
import time
import dill
import boto3
import urllib3
import uuid
import docker
from datetime import datetime
from base64 import b64decode
from concurrent.futures import ThreadPoolExecutor
from urllib3.exceptions import InsecureRequestWarning
import requests
from requests.auth import HTTPBasicAuth

urllib3.disable_warnings(InsecureRequestWarning)

python_action_callables = {}
docker_containers = {}
ibmcf_session = None


def action_pass(context, event):
    pass


def action_terminate(context, event):
    logging.info('Terminate action call ({})'.format(time.time()))


def action_dummy_task(context, event):
    termination_cloudevent = {'specversion': '1.0',
                              'id': uuid.uuid4().hex,
                              'source': '/'.join(['local', context.workspace, context.trigger_id]),
                              'type': 'termination.event.success',
                              'time': str(datetime.utcnow().isoformat()),
                              'subject': context['subject']}

    context.local_event_queue.put(termination_cloudevent)

    subject = context['subject']
    if subject in context.trigger_mapping:
        downstream_triggers = context.trigger_mapping[subject]['termination.event.success']
        for downstream_trigger in downstream_triggers:
            if context.triggers[downstream_trigger].context['dependencies'][subject]['join'] > 0:
                context.triggers[downstream_trigger].context['dependencies'][subject]['join'] += 1
            else:
                context.triggers[downstream_trigger].context['dependencies'][subject]['join'] = 1


def action_python_callable(context, event):
    global python_action_callables

    if context.trigger_id not in python_action_callables:
        decoded_callable = b64decode(context.triggers[context.trigger_id].action_meta['callable'].encode('utf-8'))
        f = dill.loads(decoded_callable)
        python_action_callables[context.trigger_id] = f

    f = python_action_callables[context.trigger_id]
    result = f(context=context, event=event)

    return result


def action_docker(context, event):
    global docker_containers
    action_meta = context.triggers[context.trigger_id].action_meta

    image = action_meta['image']

    if image not in docker_containers:
        docker_api = docker.APIClient()
        docker_client = docker.from_env()
        container = docker_client.run(image=image, detach=True)
        container_info = docker_api.inspect_container(container.id)
        container_ip = container_info['NetworkSettings']['Networks']['bridge']['IPAddress']
        docker_containers[image] = container_ip

    proxy_endpoint = docker_containers[image]

    res = requests.post('http://{}/actions/{}'.format(proxy_endpoint, context.trigger_id), data=action_meta['script'])

    res = requests.get('http://{}/actions/{}/run'.format(proxy_endpoint, context.trigger_id), json={'context': context,
                                                                                                    'event': event})
    res_json = res.json()
    context.update(res_json['context'])


def create_ibmcf_session():
    global ibmcf_session
    ibmcf_session = requests.session()
    adapter = requests.adapters.HTTPAdapter()
    ibmcf_session.mount('https://', adapter)


def action_throttle_ibmcf_function(context, event):
    if not ibmcf_session:
        create_ibmcf_session()

    cf_auth = context['global_context']['ibm_cf']['api_key'].split(':')
    cf_auth_handler = HTTPBasicAuth(cf_auth[0], cf_auth[1])

    payload = {'__OW_TRIGGERFLOW': {'THROTTLE': True}}

    for url, n in context['prewarm'].items():
        n = round(n * 1.25)
        with ThreadPoolExecutor(max_workers=n) as thread_pool:
            futures = [thread_pool.submit(
                ibmcf_session.post(url, json=payload, auth=cf_auth_handler, timeout=10.0, verify=True))
                for _ in range(n)]

    response = [f.result() for f in futures]


def action_ibm_cf_invoke(context, event):
    class InvokeException(Exception):
        pass

    if not ibmcf_session:
        create_ibmcf_session()

    cf_auth = context['operator']['api_key'].split(':')
    cf_auth_handler = HTTPBasicAuth(cf_auth[0], cf_auth[1])

    url = context['operator']['url']
    subject = context['subject'] if 'subject' in context else None

    triggerflow_meta = {'subject': subject,
                        'sink': context['operator']['sink']}

    invoke_payloads = []
    if context['operator']['iter_data']:
        keys = list(context['operator']['iter_data'].keys())
        iterdata_keyword = keys.pop()
        for iterdata_value in context['operator']['iter_data'][iterdata_keyword]:
            payload = context['operator']['invoke_kwargs'].copy()
            payload[iterdata_keyword] = iterdata_value
            payload['__OW_TRIGGERFLOW'] = triggerflow_meta
            invoke_payloads.append(payload)
    else:
        payload = context['operator']['invoke_kwargs'].copy()
        payload['__OW_TRIGGERFLOW'] = triggerflow_meta
        invoke_payloads.append(payload)

    if 'max_retries' in context:
        max_retries = context['max_retries']
    else:
        max_retries = 5

    ################################################
    def invoke(call_id, payload):
        act_id = None
        retry = True
        retry_count = 0
        while retry:
            try:
                start_t = time.time()
                # response = requests.post(url, json=payload, auth=cf_auth_handler, timeout=10.0, verify=True)
                response = ibmcf_session.post(url, json=payload, auth=cf_auth_handler, timeout=10.0, verify=True)
                status_code = response.status_code
                res_json = response.json()
                et = round(time.time() - start_t, 3)
                if status_code in range(200, 300) and 'activationId' in res_json and \
                        res_json['activationId'] is not None:
                    retry = False
                    act_id = res_json['activationId']
                    logging.info(
                        '[{}][{}] Invocation success ({}s) - Activation ID: {}'.format(context.workspace, call_id, et,
                                                                                       act_id))
                elif status_code in range(400, 500) and status_code not in [408, 409, 429]:
                    logging.error(
                        '[{}][{}] Invocation failed - Activation status code: {}'.format(context.workspace, call_id,
                                                                                         status_code))
                    raise InvokeException('Invocation failed')
            except requests.exceptions.RequestException as e:
                logging.error('[{}][{}] Error talking to OpenWhisk: {}'.format(context.workspace, call_id, e))
                raise e
            except Exception as e:
                logging.error("[{}][{}] Exception - {}".format(context.workspace, call_id, e))
            if retry:
                retry_count += 1
                if retry_count <= max_retries:
                    sleepy_time = pow(2, retry_count)
                    logging.info("[{}][{}] Retrying in {} second(s)".format(context.workspace, call_id, sleepy_time))
                    time.sleep(sleepy_time)
                else:
                    logging.error(
                        "[{}][{}] Retrying failed after {} attempts".format(context.workspace, call_id, max_retries))
                    raise InvokeException('Invocation failed')

        return call_id, act_id

    ################################################

    total_activations = len(invoke_payloads)
    logging.info("[{}] Firing trigger {} - Activations: {} ".format(context.workspace, subject, total_activations))
    futures = []
    responses = []
    if total_activations == 1:
        resp = invoke(0, invoke_payloads[0])
        responses.append(resp)
    else:
        with ThreadPoolExecutor(max_workers=128) as executor:
            for cid, payload in enumerate(invoke_payloads):
                res = executor.submit(invoke, cid, payload)
                futures.append(res)
        try:
            responses = [fut.result() for fut in futures]
        except InvokeException:
            pass

    activations_done = [call_id for call_id, _ in responses]
    activations_not_done = [call_id for call_id in range(total_activations) if call_id not in activations_done]

    if subject in context.trigger_mapping:
        downstream_triggers = context.trigger_mapping[subject]['event.triggerflow.termination.success']
        for downstream_trigger in downstream_triggers:
            downstream_trigger_ctx = context.triggers[downstream_trigger].context
            if 'total_activations' in downstream_trigger_ctx:
                downstream_trigger_ctx['total_activations'] += total_activations
            else:
                downstream_trigger_ctx['total_activations'] = total_activations

            if 'dependencies' in downstream_trigger_ctx and subject in downstream_trigger_ctx['dependencies']:
                if downstream_trigger_ctx['dependencies'][subject]['join'] > 0:
                    downstream_trigger_ctx['dependencies'][subject]['join'] += total_activations
                else:
                    downstream_trigger_ctx['dependencies'][subject]['join'] = total_activations

    # All activations are unsuccessful
    if not activations_done:
        raise Exception('All invocations are unsuccessful')
    # At least one activation is successful
    else:
        # All activations are successful
        if len(activations_done) == total_activations:
            logging.info('[{}][{}] All invocations successful'.format(context.workspace, subject))
        # Only some activations are successful
        else:
            logging.info(
                "[{}][{}] Could not be completely triggered - {} activations pending".format(context.workspace, subject,
                                                                                             len(activations_not_done)))


def action_aws_lambda_invoke(context, event):
    class InvokeException(Exception):
        pass

    subject = context['subject'] if 'subject' in context else None
    function_name = context['operator']['function_name']

    triggerflow_meta = {'subject': subject,
                        'sink': context['operator']['sink']}

    invoke_payloads = []
    if context['operator']['iter_data']:
        keys = list(context['operator']['iterdata'].keys())
        iterdata_keyword = keys.pop()
        for iterdata_value in context['operator']['iterdata'][iterdata_keyword]:
            payload = context['operator']['invoke_kwargs'].copy()
            payload[iterdata_keyword] = iterdata_value
            payload['__OW_TRIGGERFLOW'] = triggerflow_meta
            invoke_payloads.append(payload)
    else:
        payload = context['operator']['invoke_kwargs'].copy()
        payload['__OW_TRIGGERFLOW'] = triggerflow_meta
        invoke_payloads.append(payload)

    if 'max_retries' in context:
        max_retries = context['max_retries']
    else:
        max_retries = 5

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
                    logging.info(
                        '[{}][{}] Invocation success - Activation ID: {}'.format(context.workspace, call_id, act_id))
                elif status_code in range(400, 500) and status_code not in [408, 409, 429]:
                    logging.error(
                        '[{}][{}] Invocation failed - Activation status code: {}'.format(context.workspace, call_id,
                                                                                         status_code))
            except Exception as e:
                logging.error("[{}][{}] Exception - {}".format(context.workspace, call_id, e))
            if retry:
                retry_count += 1
                if retry_count <= max_retries:
                    sleepy_time = pow(2, retry_count)
                    logging.info("[{}][{}] Retrying in {} second(s)".format(context.workspace, call_id, sleepy_time))
                    time.sleep(sleepy_time)
                else:
                    logging.error(
                        "[{}][{}] Retrying failed after {} attempts".format(context.workspace, call_id, max_retries))
                    raise InvokeException('Invocation failed')

        return call_id, act_id

    ################################################

    total_activations = len(invoke_payloads)
    logging.info("[{}] Firing trigger {} - Activations: {} ".format(context.workspace, subject, total_activations))
    futures = []
    responses = []
    if total_activations == 1:
        resp = invoke(0, invoke_payloads[0])
        responses.append(resp)
    else:
        with ThreadPoolExecutor(max_workers=128) as executor:
            for cid, payload in enumerate(invoke_payloads):
                res = executor.submit(invoke, cid, payload)
                futures.append(res)
        try:
            responses = [fut.result() for fut in futures]
        except InvokeException:
            pass

    activations_done = [call_id for call_id, _ in responses]
    activations_not_done = [call_id for call_id in range(total_activations) if call_id not in activations_done]

    if subject in context.trigger_mapping:
        downstream_triggers = context.trigger_mapping[subject]['termination.event.success']
        for downstream_trigger in downstream_triggers:
            downstream_trigger_ctx = context.triggers[downstream_trigger].context
            if 'total_activations' in downstream_trigger_ctx:
                downstream_trigger_ctx['total_activations'] += total_activations
            else:
                downstream_trigger_ctx['total_activations'] = total_activations

            if 'dependencies' in downstream_trigger_ctx and subject in downstream_trigger_ctx['dependencies']:
                if downstream_trigger_ctx['dependencies'][subject]['join'] > 0:
                    downstream_trigger_ctx['dependencies'][subject]['join'] += total_activations
                else:
                    downstream_trigger_ctx['dependencies'][subject]['join'] = total_activations

    # All activations are unsuccessful
    if not activations_done:
        raise Exception('All invocations are unsuccessful')
    # At least one activation is successful
    else:
        # All activations are successful
        if len(activations_done) == total_activations:
            logging.info('[{}][{}] All invocations successful'.format(context.workspace, subject))
        # Only some activations are successful
        else:
            logging.info(
                "[{}][{}] Could not be completely triggered - {} activations pending".format(context.workspace, subject,
                                                                                             len(activations_not_done)))
