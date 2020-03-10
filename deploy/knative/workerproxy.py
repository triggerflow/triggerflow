import logging
import os
from multiprocessing import Queue
from flask import Flask, jsonify, request

from .worker import Worker

# TODO: Logger does not work with gunicorn
logger = logging.getLogger('triggerflow-worker')

app = Flask(__name__)

event_queue = Queue()
namespace = os.environ.get('WORKSPACE')
worker = Worker(event_queue)
worker.start()


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
    port = int(os.getenv('PORT', 8080))
    app.run(debug=True, host='0.0.0.0', port=port)


if __name__ == '__main__':
    main()
