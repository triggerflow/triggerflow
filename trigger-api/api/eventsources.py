from triggerflow.service.storage import TriggerStorage


def add_event_source(trigger_storage: TriggerStorage, workspace: str, event_source: dict, overwrite: bool):
    # Check eventsource schema
    if {'name', 'class', 'parameters'} != set(event_source):
        return {"error": "Invalid eventsource object"}

    exists = trigger_storage.key_exists(workspace=workspace, document_id='event_sources', key=event_source['name'])
    if not exists or (exists and overwrite):
        trigger_storage.set_key(workspace=workspace, document_id='event_sources',
                                key=event_source['name'], value=event_source.copy())
        res = {"message": "Created/updated {}".format(event_source['name'])}, 201
    else:
        res = {"error": "Event source {} already exists".format(event_source['name'])}, 409

    return res


def get_event_source(trigger_storage: TriggerStorage, workspace: str, event_source_name: str):
    event_source = trigger_storage.get_key(workspace=workspace, document_id='event_sources', key=event_source_name)

    if event_source is not None:
        return {event_source_name: event_source}, 200
    else:
        return {"error": "Event source {} not found".format(event_source_name)}, 404


def list_event_sources(trigger_storage: TriggerStorage, workspace: str):
    event_sources = trigger_storage.keys(workspace=workspace, document_id='event_sources')
    return event_sources, 200


def delete_event_source(trigger_storage: TriggerStorage, workspace: str, event_source_name: str):
    if trigger_storage.key_exists(workspace=workspace, document_id='event_sources', key=event_source_name):
        trigger_storage.delete_key(workspace=workspace, document_id='event_sources', key=event_source_name)
        return {"message": "Event source {} deleted".format(event_source_name)}, 200
    else:
        return {"error": "Event source {} not found".format(event_source_name)}, 404
