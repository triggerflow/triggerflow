import logging
import time
from concurrent.futures import ThreadPoolExecutor

import requests
from requests.auth import HTTPBasicAuth


def action_pass(context, event):
    pass


def action_terminate(context, event):
    pass


def action_ibm_cf_invoke_rabbitmq(context, event):
    class InvokeException(Exception):
        pass

    cf_auth = context['ibm_cf_credentials']['api_key'].split(':')
    cf_auth_handler = HTTPBasicAuth(cf_auth[0], cf_auth[1])

    url = context['url']
    namespace = context['namespace']
    subject = context['subject']

    # Transform a callasync trigger to a map trigger of a single function
    if context['kind'] == 'callasync':
        function_args = [context['args']]
    else:
        function_args = context['args']
    total_activations = len(function_args)

    max_retries = 5

    ################################################
    def invoke(call_id, args):
        payload = args.copy()
        payload['__OW_COMPOSER_RABBITMQ_EVENTQUEUE'] = context['RabbitMQ']['topic']
        payload['__OW_COMPOSER_RABBITMQ_AMQPURL'] = context['RabbitMQ']['amqp_url']
        payload['__OW_COMPOSER_EXTRAMETA'] = {'namespace': namespace,
                                              'trigger_id': context['trigger_id'],
                                              'subject': subject,
                                              'call_id': call_id}

        act_id = None
        retry = True
        retry_count = 0
        while retry:
            try:
                response = requests.post(url, json=payload, auth=cf_auth_handler, timeout=10.0, verify=True)
                status_code = response.status_code
                res_json = response.json()
                if status_code in range(200, 300) and 'activationId' in res_json and res_json['activationId'] is not None:
                    retry = False
                    act_id = res_json['activationId']
                    logging.info('[{}][{}] Invocation success - Activation ID: {}'.format(namespace, call_id, act_id))
                elif status_code in range(400, 500) and status_code not in [408, 409, 429]:
                    logging.error('[{}][{}] Invocation failed - Activation status code: {}'.format(namespace, call_id,
                                                                                                   status_code))
                    raise InvokeException('Invocation failed')
            except requests.exceptions.RequestException as e:
                logging.error('[{}][{}] Error talking to OpenWhisk: {}'.format(namespace, call_id, e))
                raise e
            except Exception as e:
                logging.error("[{}][{}] Exception - {}").format(namespace, call_id, e)
            if retry:
                retry_count += 1
                if retry_count <= max_retries:
                    sleepy_time = pow(2, retry_count)
                    logging.info("[{}][{}] Retrying in {} second(s)".format(namespace, call_id, sleepy_time))
                    time.sleep(sleepy_time)
                else:
                    logging.error("[{}][{}] Retrying failed after {} attempts".format(namespace, call_id, max_retries))
                    raise InvokeException('Invocation failed')

        return call_id, act_id

    ################################################

    logging.info("[{}] Firing trigger {} - Activations: {} ".format(namespace, subject, total_activations))
    futures = []
    with ThreadPoolExecutor(max_workers=128) as executor:
        for cid, args in enumerate(function_args):
            res = executor.submit(invoke, cid, args)
            futures.append(res)

    responses = []
    try:
        responses = [fut.result() for fut in futures]
    except InvokeException:
        pass

    activations_done = [call_id for call_id, _ in responses]
    activations_not_done = [call_id for call_id in range(total_activations) if call_id not in activations_done]

    context['activations_done'] = activations_done
    context['activations_not_done'] = activations_not_done
    context['total_activations'] = total_activations

    # All activations are unsuccessful
    if not activations_done:
        raise Exception('All invocations are unsuccessful')
    # At least one activation is successful
    else:
        # All activations are successful
        if len(activations_done) == total_activations:
            logging.info('[{}][{}] All invocations successful'.format(namespace, subject))
        # Only some activations are successful
        else:
            logging.info(
                "[{}][{}] Could not be completely triggered - {} activations pending".format(namespace, subject,
                                                                                             len(activations_not_done)))


def action_ibm_cf_invoke_kafka(context, event):
    class InvokeException(Exception):
        pass

    cf_auth = context['ibm_cf_credentials']['api_key'].split(':')
    cf_auth_handler = HTTPBasicAuth(cf_auth[0], cf_auth[1])

    url = context['url']
    namespace = context['namespace']
    subject = context['subject']

    # Transform a callasync trigger to a map trigger of a single function
    if context['kind'] == 'callasync':
        function_args = [context['args']]
    else:
        function_args = context['args']
    total_activations = len(function_args)

    max_retries = 5

    ################################################
    def invoke(call_id, args):
        payload = args.copy()
        payload['__OW_COMPOSER_KAFKA_BROKERS'] = context['kafka_credentials']['kafka_brokers_sasl']
        payload['__OW_COMPOSER_KAFKA_USERNAME'] = context['kafka_credentials']['user']
        payload['__OW_COMPOSER_KAFKA_PASSWORD'] = context['kafka_credentials']['password']
        payload['__OW_COMPOSER_KAFKA_TOPIC'] = context['Kafka']['topic']
        payload['__OW_COMPOSER_EXTRAMETA'] = {'namespace': namespace,
                                              'trigger_id': context['trigger_id'],
                                              'subject': subject,
                                              'call_id': call_id}

        act_id = None
        retry = True
        retry_count = 0
        while retry:
            try:
                response = requests.post(url, json=payload, auth=cf_auth_handler, timeout=10.0, verify=True)
                status_code = response.status_code
                res_json = response.json()
                if status_code in range(200, 300) and 'activationId' in res_json and res_json['activationId'] is not None:
                    retry = False
                    act_id = res_json['activationId']
                    logging.info('[{}][{}] Invocation success - Activation ID: {}'.format(namespace, call_id, act_id))
                elif status_code in range(400, 500) and status_code not in [408, 409, 429]:
                    logging.error('[{}][{}] Invocation failed - Activation status code: {}'.format(namespace, call_id,
                                                                                                   status_code))
                    raise InvokeException('Invocation failed')
            except requests.exceptions.RequestException as e:
                logging.error('[{}][{}] Error talking to OpenWhisk: {}'.format(namespace, call_id, e))
                raise e
            except Exception as e:
                logging.error("[{}][{}] Exception - {}".format(namespace, call_id, e))
            if retry:
                retry_count += 1
                if retry_count <= max_retries:
                    sleepy_time = pow(2, retry_count)
                    logging.info("[{}][{}] Retrying in {} second(s)".format(namespace, call_id, sleepy_time))
                    time.sleep(sleepy_time)
                else:
                    logging.error("[{}][{}] Retrying failed after {} attempts".format(namespace, call_id, max_retries))
                    raise InvokeException('Invocation failed')

        return call_id, act_id

    ################################################

    logging.info("[{}] Firing trigger {} - Activations: {} ".format(namespace, subject, total_activations))
    futures = []
    with ThreadPoolExecutor(max_workers=128) as executor:
        for cid, args in enumerate(function_args):
            res = executor.submit(invoke, cid, args)
            futures.append(res)

    responses = []
    try:
        responses = [fut.result() for fut in futures]
    except InvokeException:
        pass

    activations_done = [call_id for call_id, _ in responses]
    activations_not_done = [call_id for call_id in range(total_activations) if call_id not in activations_done]

    context['activations_done'] = activations_done
    context['activations_not_done'] = activations_not_done
    context['total_activations'] = total_activations

    # All activations are unsuccessful
    if not activations_done:
        raise Exception('All invocations are unsuccessful')
    # At least one activation is successful
    else:
        # All activations are successful
        if len(activations_done) == total_activations:
            logging.info('[{}][{}] All invocations successful'.format(namespace, subject))
        # Only some activations are successful
        else:
            logging.info(
                "[{}][{}] Could not be completely triggered - {} activations pending".format(namespace, subject,
                                                                                             len(activations_not_done)))
