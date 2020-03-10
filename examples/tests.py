from triggerflow.client import TriggerflowClient
from triggerflow.client.utils import load_config_yaml
from triggerflow.client.sources import KafkaEventSource, KafkaAuthMode

if __name__ == "__main__":
    tf_config = load_config_yaml('~/client_config.yaml')
    kafka_config = tf_config['kafka']

    tf = TriggerflowClient(**tf_config['triggerflow'])

    kafka = KafkaEventSource(name='my_kafka_eventsource',
                             broker_list=kafka_config['kafka_brokers_sasl'],
                             topic='hello',
                             auth_mode=KafkaAuthMode.SASL_PLAINTEXT,
                             username=kafka_config['user'],
                             password=kafka_config['password'])

    tf.create_workspace(workspace='test',
                        global_context={'ibm_cf': tf_config['ibm_cf']},
                        event_source=kafka)

    # tf.delete_workspace('test')
