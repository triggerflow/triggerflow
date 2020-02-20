import logging
import os
from multiprocessing import Queue
from flask import Flask, jsonify, request
from worker import Worker

logger = logging.getLogger('triggerflow-worker')

app = Flask(__name__)
app.debug = False

worker = None
private_credentials = None
db = None
event_queue = Queue()


@app.route('/', methods=['POST'])
def run():
    """
    Get an event and forward it into an internal queue
    """
    def error():
        response = jsonify({'error': 'The worker did not receive a dictionary as an argument.'})
        response.status_code = 404
        return response

    message = request.get_json(force=True, silent=True)
    if message and not isinstance(message, dict):
        return error()

    print('Receiving message...')
    event_queue.put(message)
    print('Message received')

    return jsonify('Message received'), 201


def main():
    global private_credentials, worker

    namespace = os.environ.get('NAMESPACE')
    worker = Worker(namespace, event_queue, private_credentials)
    worker.start()


if __name__ == '__main__':
    main()
