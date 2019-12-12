import json
import time

from confluent_kafka import Producer

from api.client import CloudEventProcessorClient, DefaultActions, DefaultConditions
from api.utils import load_config_yaml
from api.sources.kafka import KafkaCloudEventSource, KafkaSASLAuthMode

n_steps = 5
n_maps = 15
n_funcs_per_map = 1000
triggers2 = []
triggers = ['37cbc6b9-66b7-46dd-9593-4a9740192cf8',
            'd2cb558e-f5a9-4cc3-a7d4-810a320ee4d0', '77aa95e5-b938-4ed5-b543-a51213470730', '8e980529-8fd1-4998-99a8-dcb2835400ae', '10932c80-82c1-49dc-bcee-8794a2ef9701', '517bec52-4081-47c3-897d-c231caafcce3', '2e9d9e5a-dd7c-411a-b78b-d8ebf5bba2d4', 'be10085f-f541-4b67-b8e3-da5609e42c88', 'cb0a64c7-3623-4660-a42d-b36f71666b99', 'eb83ea4a-df12-46e0-80ea-5015a9bfbf73', '063b336e-1c72-476f-a37f-6593b07fda49', '25953655-e6f2-4a9a-8dc9-e981c274df56', '6b08e78e-8489-4821-adc1-0d8bf09483df', '82c0695f-1ed7-4a83-ad27-cb2c4a1ff995', 'f9693ef2-72e3-4169-9580-84b74b9d48b5', '958c6913-64f5-472a-bdce-67e11171f894', 'af66fca6-8a62-4c80-a82a-64dabd766bfd', '4ab386f8-cab8-4262-beec-b83db876b9a7', '15b5c351-b736-497c-b5dc-ad82e1ff37ad', '9535ceea-fe76-4343-b16e-be0d47844219', '2d7b2a73-e50d-4635-acd3-5b10bd45cee3', '100b4e91-e58a-4a04-8bd1-35bcf0dd256a', '10d02c0d-7959-4583-8d54-61c222107055', 'c3abda65-2d96-494d-87c1-05a5146ee1d4', '06070df0-3ff6-45f0-8a4d-36811bd2c64a', '8a842c36-a15c-4e19-9f3c-ab9952146b96', '5a99ee1f-01e3-4cca-95f4-750110d3d30d', '45b6ddce-704a-4b90-9792-703f40b1b37d', '296f4be8-41b4-447b-a581-9613bcf831d0', '8bb02ac7-09ed-4d9a-bae0-acdefa87e42f', 'a1bf650c-6af9-4a23-830f-6a751a4abef5', '3b836725-82b2-47dc-a1b6-d219ac317b1e', 'eb13e226-2915-4a60-87ad-1a837b5d0082', '062615a1-7d4c-4523-a37e-1172171624fa', '026dd62b-f1e1-4812-82d6-3ba68f79fb49', 'e90a1f1a-fb7a-4f86-ab24-0a935d475353', '5bd6da1d-a91d-449f-9d8f-282c1d652f6c', 'aa97ec09-2cfa-4b22-b793-d16da19e8474', '529b4951-e05a-4107-83de-9f721ecc7e93', 'c5cf9480-2974-4ad9-89cb-c827f745e0f8', '45688f98-a436-42d7-aeb1-f71654adf082', 'dabbf7aa-de72-419c-9803-7b12b564d657', '04c74cfe-8731-4d1a-86ba-b0c14533c1f4', '9144cf0b-028c-470b-bbca-1930e9112dd0', '9db2f999-fbdc-46a8-b7fc-1b7ad85fca85', '14edb867-177c-419a-bc19-158a8c5158ce', '730446e0-0a14-4efd-8ae4-8371fba7be94', '04222df9-57d6-40a0-b200-288cbbe4cfd8', 'e1d75b12-8288-4908-bd06-482c879efb26', '9a4991ed-0c11-41d1-a75c-2be79f76bbd5', 'c73dd887-14f5-467b-b784-fa22194877bb', '2c9c0d75-b6e7-400b-a5b5-b5b05f32d31c', '836da722-d2de-4dc3-a8ce-ec4284f1e089', 'bb9140c3-252e-41eb-9b46-b4237d2506c7', '007b0d73-92f7-4327-81f4-8966c832c71d', 'c31ff142-798f-4acb-af33-6e3b05040cc0', 'a7c3b032-f405-450d-a727-533466ee2f01', '9ec24273-2f7c-4768-bbaf-54f3690b6bc0', '75c28ea7-9bb5-4b03-864a-8fd70c7acb6d', '941d8e89-cab6-45a0-84b4-ab98ab477c8c', '1e4567f7-3e6a-4f88-9a31-951090fc5260', '8f1de988-505d-4470-97be-4ae7110de3b4', '7b90feee-b74e-4d98-8559-b01726d458a6', '6c0eb86b-d182-4c7c-b436-3875ea37b273', '1a726b55-1f6e-4492-988e-1af618f7949a', 'fbd4b946-1766-4388-9f0a-133b2f275c5f', '9ea8170c-427d-43aa-a867-2f236b42d2b1', '0e867417-2534-421b-9c58-06787fb90286', 'f8203b16-dd3b-4c83-b6e7-5225ccfce847', '23ba9c92-3d3b-4e12-8fc2-a8abd8695da0', 'a34e1a88-05f9-4efa-bff4-61c842b78113', '78ef951e-2438-47c0-9de0-f3863454a1c6', '37ec511e-169e-4333-9b6f-a73f2c8785ce', '28d5c023-cd0b-41cb-8b41-a9badd3fcd60', 'f4c66d5f-b6e8-432e-9913-583fb44c6183', '33fee0f2-9629-4ccd-9bf2-4dd7f03ff106']


def create_triggers():
    client_config = load_config_yaml('~/event-processor_credentials.yaml')
    kafka_credentials = load_config_yaml('~/kafka_credentials.yaml')

    kafka = KafkaCloudEventSource(broker_list=kafka_credentials['eventstreams']['kafka_brokers_sasl'],
                                  topic='stress_test',
                                  auth_mode=KafkaSASLAuthMode.SASL_PLAINTEXT,
                                  username=kafka_credentials['eventstreams']['user'],
                                  password=kafka_credentials['eventstreams']['password'])

    er = CloudEventProcessorClient(namespace='stress_test',
                                   event_source=kafka,
                                   global_context={
                                       'ibm_cf_credentials': client_config['authentication']['ibm_cf_credentials'],
                                       'kafka_credentials': kafka_credentials['eventstreams']},
                                   api_endpoint=client_config['event_processor']['api_endpoint'],
                                   authentication=client_config['authentication'])

    # [map1-1 ... map1-10] >> [map2-1 ... map2-10] >> [map3-1 ... map3-10]

    for i in range(n_maps):
        trg = er.add_trigger(kafka.event('init__'),
                             condition=DefaultConditions.TRUE,
                             action=DefaultActions.SIM_CF_INVOKE,
                             context={'subject': 'map0-{}'.format(i),
                                      'args': [{'x': x} for x in range(n_funcs_per_map)],
                                      'kind': 'map'})
        triggers.append(trg['trigger_id'])

    for step in range(1, n_steps):
        for i in range(n_maps):
            trg = er.add_trigger(kafka.event('map{}-{}'.format(step - 1, i)),
                                 condition=DefaultConditions.IBM_CF_JOIN,
                                 action=DefaultActions.SIM_CF_INVOKE,
                                 context={'subject': 'map{}-{}'.format(step, i),
                                          'args': [{'x': x} for x in range(n_funcs_per_map)], 'kind': 'map'})
            triggers.append(trg['trigger_id'])

    trg = er.add_trigger([kafka.event('map{}-{}'.format(n_steps - 1, x)) for x in range(n_maps)],
                         condition=DefaultConditions.IBM_CF_JOIN,
                         action=DefaultActions.TERMINATE)
    triggers.append(trg['trigger_id'])


def generate_events():
    kafka_credentials = load_config_yaml('~/kafka_credentials.yaml')

    config = {'bootstrap.servers': ','.join(kafka_credentials['eventstreams']['kafka_brokers_sasl']),
              'ssl.ca.location': '/etc/ssl/certs/',
              'sasl.mechanisms': 'PLAIN',
              'sasl.username': kafka_credentials['eventstreams']['user'],
              'sasl.password': kafka_credentials['eventstreams']['password'],
              'security.protocol': 'sasl_ssl'
              }

    kafka_producer = Producer(**config)
    termination_event = {'subject': 'init__'}
    kafka_producer.produce(topic='stress_test', value=json.dumps(termination_event))
    kafka_producer.flush()

    time.sleep(0.05)

    for step in range(n_steps):
        for i in range(n_maps):
            trg = triggers.pop(0)
            for _ in range(n_funcs_per_map):
                kafka_producer.produce(topic='stress_test', value=json.dumps({'subject': 'map{}-{}'.format(step, i),
                                                                              'triggersource': trg}))
            kafka_producer.flush()
        time.sleep(0.05)


if __name__ == '__main__':
    # create_triggers()
    # print(triggers)
    # time.sleep(15)
    generate_events()
