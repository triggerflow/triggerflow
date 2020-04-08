from eventprocessor.client import eventprocessorClient, CloudEvent, PythonCallable
from eventprocessor.client.utils import load_config_yaml
from eventprocessor.client.sources import KafkaEventSource

if __name__ == "__main__":
    tf_config = load_config_yaml('~/client_config.yaml')
    tf = eventprocessorClient(**tf_config['eventprocessor'])
    es = KafkaEventSource(**tf_config['kafka'])
    tf.create_workspace(workspace='python_callable_example', event_source=es)

    def my_condition(context, event):
        if 'data' in event:
            return event['data']['number'] == 123
        else:
            return False

    def my_action(context, event):
        print('Hello')

    tf.target_workspace('python_callable_example')
    tf.add_trigger(CloudEvent('hello'),
                   condition=PythonCallable(function=my_condition),
                   action=PythonCallable(function=my_action))
