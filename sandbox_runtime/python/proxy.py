import os
import traceback

import dill
from base64 import b64decode

from flask import Flask, request, jsonify

proxy = Flask(__name__)

condition_callables = {}
action_callables = {}


@proxy.route('/action/<function_name>', methods=['POST'])
def put_action(function_name):
    global action_callables

    data = request.get_data()
    decoded_callable = b64decode(data)
    action = dill.loads(decoded_callable)
    action_callables[function_name] = action


@proxy.route('/action/<function_name>/run', methods=['GET'])
def run_action(function_name):
    request_json = request.get_json(silent=True, force=True)

    if not {'context', 'event'}.issubset(set(request_json)):
        return jsonify({'error': 'Bad request parameters'}), 400

    if function_name not in action_callables:
        return jsonify({'error': 'Function {} not registered'.format(function_name)}), 404

    action = action_callables[function_name]
    context = request_json['context']
    event = request_json['event']
    try:
        action(event=event, context=context)
        result = {'result': None, 'context': context}
    except Exception:
        return jsonify({'exception': traceback.format_exc()}), 500

    return jsonify(result), 200


@proxy.route('/condition/<function_name>', methods=['POST'])
def put_condition(function_name):
    global condition_callables

    data = request.get_data()
    decoded_callable = b64decode(data)
    action = dill.loads(decoded_callable)
    condition_callables[function_name] = action


@proxy.route('/condition/<function_name>/run', methods=['GET'])
def run_condition(function_name):
    request_json = request.get_json(silent=True, force=True)

    if not {'context', 'event'}.issubset(set(request_json)):
        return jsonify({'error': 'Bad request parameters'}), 400

    if function_name not in action_callables:
        return jsonify({'error': 'Function {} not registered'.format(function_name)}), 404

    condition = condition_callables[function_name]
    context = request_json['context']
    event = request_json['event']
    try:
        condition_result = condition(event=event, context=context)
        result = {'result': condition_result if isinstance(condition_result, bool) else False, 'context': context}
    except Exception:
        return jsonify({'exception': traceback.format_exc()}), 500

    return jsonify(result), 200


if __name__ == "__main__":
    proxy.run(debug=True, host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
