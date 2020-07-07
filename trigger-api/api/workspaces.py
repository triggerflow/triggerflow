import re

from triggerstorage import TriggerStorage


def create_workspace(trigger_storage: TriggerStorage, workspace: str, global_context: dict, event_source: dict):
    if trigger_storage.workspace_exists(workspace=workspace):
        return {'error': 'Workspace {} already exists'.format(workspace), 'err_code': 2}, 400

    # Workspace name can only contain alphanumeric, hyphens or underscore characters
    if not re.fullmatch(r"^[a-zA-Z0-9._-]*$", workspace):
        return {'error': 'Illegal workspace name', 'err_code': 3}, 400

    if {'name', 'class', 'parameters'} != set(event_source):
        return {'error': 'Invalid event source', 'err_code': 4}, 400

    trigger_storage.create_workspace(workspace, {event_source['name']: event_source}, global_context)
    return {'message': 'Created workspace {}'.format(workspace)}, 200


def get_workspace(trigger_storage: TriggerStorage, workspace: str):
    triggers = trigger_storage.get(workspace=workspace, document_id='triggers')
    triggerIDs = [trigger["id"] for trigger in triggers]
    event_sources = trigger_storage.get(workspace=workspace, document_id='event_sources')
    event_source_names = [event_source['name'] for event_source in event_sources]
    global_context = trigger_storage.get(workspace=workspace, document_id='global_context')

    return {'triggers': triggerIDs, 'event_sources': event_source_names, 'global_context': global_context}, 200


def delete_workspace(trigger_storage: TriggerStorage, workspace: str):
    trigger_storage.delete_workspace(workspace=workspace)
    return {'message': 'Workspace {} deleted'.format(workspace)}, 200
