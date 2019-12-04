from api.client import CloudEventProcessorClient, cloudevent_trigger_source

if __name__ == "__main__":
    er = CloudEventProcessorClient('test')

    event_source = cloudevent_trigger_source('test.event.trigger', 'mysource', 'tenant/test/t1')
    er.add_trigger(event_source, condition=Condition.JOIN, action=Action.INVOKE,
                   context={'some_important_things': 'hi'})
