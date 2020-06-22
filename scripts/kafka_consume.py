from confluent_kafka import Consumer

if __name__ == '__main__':
    config = {'bootstrap.servers': '127.0.0.1:9092',
              # 'ssl.ca.location': '/etc/ssl/certs/',
              # 'sasl.mechanisms': 'PLAIN',
              # 'sasl.username': kafka_credentials['eventstreams']['user'],
              # 'sasl.password': kafka_credentials['eventstreams']['password'],
              # 'security.protocol': 'sasl_ssl',
              'group.id': '123',
              'default.topic.config': {'auto.offset.reset': 'earliest'},
              'enable.auto.commit': True
              }


    def print_assignment(consumer, partitions):
        print('Assignment: {}'.format(partitions))

    kafka_consumer = Consumer(config)
    kafka_consumer.subscribe(['PiEstimationMontecarlo-65f6529bcba1'], on_assign=print_assignment)
    print('consuming')
    while True:
        msg = kafka_consumer.poll(timeout=1.0)
        if msg is None:
            print('null message')
        else:
            print(msg.value())
