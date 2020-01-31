from importlib import import_module
import os
import json


def wrapper():
    module_name, callable_name = os.environ['CLASS'].rsplit('.', 1)

    module = import_module(module_name)
    f = getattr(module, callable_name)

    event = json.loads(os.environ['EVENT'])
    context = json.loads(os.environ['CONTEXT'])

    res = f(event=event, context=context)
    return {'result': res, 'context': context}


if __name__ == '__main__':
    out = wrapper()
    with open('out.json', 'w') as out_file:
        out_file.write(json.dumps(out))
