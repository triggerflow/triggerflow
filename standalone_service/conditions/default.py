import docker
import json
import tarfile
import io


def condition_docker_image(context, event):
    client = docker.from_env()

    context_copy = context.copy()

    print(client.images.list())

    for k, v in context_copy['triggers'].items():
        del v['context']['triggers']

    env = {'CLASS': context['condition']['class_name'],
           'EVENT': json.dumps(event),
           'CONTEXT': json.dumps(context_copy)}
    container = client.containers.create(context['condition']['image'], environment=env)
    container.start()
    container.wait()
    output, _ = container.get_archive('/out.json')
    container.remove()

    res = {}

    for buffer in output:
        tar = tarfile.open(mode="r|", fileobj=io.BytesIO(buffer))
        for l in tar:
            x = tar.extractfile(l)
            res = json.loads(x.read())

    context.update(res['context'])
    return res['result']


def condition_true(context, event):
    return True


def condition_ibm_cf_join(context, event):
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
