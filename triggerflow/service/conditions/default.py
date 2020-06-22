import dill
import docker
import requests
from base64 import b64decode

docker_containers = {}


def condition_true(context, event):
    return True


def condition_dag_task_join(context, event):
    context['dependencies'][event['subject']]['counter'] += 1

    return all([dep['counter'] == dep['join'] for dep in context['dependencies'].values()])


def condition_function_join(context, event):
    if 'counter' not in context:
        context['counter'] = 1
    else:
        context['counter'] += 1

    return context['counter'] == context['total_activations']


def condition_counter_threshold(context, event):
    if 'counter' not in context:
        context['counter'] = 1
    else:
        context['counter'] += 1

    return context['counter'] >= context['threshold']


def condition_python_callable(context, event):
    decoded_callable = b64decode(context['condition']['callable'].encode('utf-8'))
    f = dill.loads(decoded_callable)

    result = f(context=context, event=event)

    assert isinstance(result, bool)

    return result


def condition_docker(context, event):
    global docker_containers
    condition_meta = context.triggers[context.trigger_id].condition_meta['image']

    image = condition_meta

    if image not in docker_containers:
        docker_api = docker.APIClient()
        docker_client = docker.from_env()
        container = docker_client.run(image=image, detach=True)
        container_info = docker_api.inspect_container(container.id)
        container_ip = container_info['NetworkSettings']['Networks']['bridge']['IPAddress']
        docker_containers[image] = container_ip

    proxy_endpoint = docker_containers[image]

    res = requests.post('http://{}/condition/{}'.format(proxy_endpoint, context.trigger_id),
                        data=condition_meta['script'])

    res = requests.get('http://{}/condition/{}/run'.format(proxy_endpoint, context.trigger_id),
                       json={'context': context,
                             'event': event})
    res_json = res.json()
    context.update(res_json['context'])
    return res_json['result']
