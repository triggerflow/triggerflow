from triggerflow.client import TriggerflowClient, CloudEvent, PythonCallable
from triggerflow.client.utils import load_config_yaml
from triggerflow.client.sources import KafkaEventSource

if __name__ == "__main__":
    tf_config = load_config_yaml('~/client_config.yaml')

    tf = TriggerflowClient(**tf_config['event_processor'])

    kafka = KafkaEventSource(**tf_config['kafka'])

    tf.create_workspace(namespace='python_callable_example',
                        event_source=kafka)

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
