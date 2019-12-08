from api.client import CloudEventProcessorClient, cloudevent_trigger_source
from api.utils import load_config_yaml
import logging

if __name__ == "__main__":
    client_config = load_config_yaml()

    er = CloudEventProcessorClient(namespace='test',
                                   default_context={'my_things': 'hi'},
                                   api_endpoint=client_config['event_processor']['api_endpoint'],
                                   ibm_cf_credentials=client_config['ibm_cf_credentials'])

    event_source = cloudevent_trigger_source('test.event.trigger', 'mysource', 'tenant/test/t1')
    er.add_trigger(event_source, context={'some_important_things': 'hi'})
