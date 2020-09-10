from uuid import uuid4
from datetime import datetime

from triggerflow.service.storage import TriggerStorage


def add_triggers(trigger_storage: TriggerStorage, workspace: str, triggers: list):
    accepted_triggers = []
    rejected_triggers = []

    for trigger in triggers:
        # Check trigger schema
        if not isinstance(trigger, dict):
            continue
        if {'id', 'condition', 'action', 'context', 'activation_events', 'transient'} != set(trigger):
            trigger['reason'] = 'Malformed trigger schema'
            rejected_triggers.append(trigger)
            continue

        # Named trigger, check if it already exists
        if trigger['id'] and trigger_storage.key_exists(workspace=workspace, document_id='triggers', key=trigger['id']):
            trigger['reason'] = "Trigger {} already exists".format(trigger['id'])
            rejected_triggers.append(trigger)
            continue

        trigger_uuid = uuid4()
        trigger['id'] = trigger_uuid.hex if not trigger['id'] else trigger['id']
        trigger['uuid'] = str(trigger_uuid)
        trigger['workspace'] = workspace
        trigger['timestamp'] = datetime.utcnow().isoformat()

        trigger_storage.set_key(workspace=workspace, document_id='triggers', key=trigger['id'], value=trigger)
        accepted_triggers.append(trigger)

    return {'accepted_triggers': accepted_triggers, 'rejected_triggers': rejected_triggers}, 200


def get_trigger(trigger_storage: TriggerStorage, workspace: str, trigger_id: str):
    trigger = trigger_storage.get_key(workspace=workspace, document_id='triggers', key=trigger_id)

    if trigger is not None:
        return {trigger_id: trigger}, 200
    else:
        return {'error': 'Trigger {} not found'.format(trigger_id)}, 404


def list_triggers(trigger_storage: TriggerStorage, workspace: str):
    trigger_ids = trigger_storage.keys(workspace=workspace, document_id='triggers')

    return trigger_ids, 200


def delete_trigger(trigger_storage: TriggerStorage, workspace: str, trigger_id: str):
    deleted = trigger_storage.delete_key(workspace=workspace, document_id='triggers', key=trigger_id)

    if deleted != 0:
        return {'message': 'Deleted {}'.format(trigger_id)}, 200
    else:
        return {'error': 'Trigger {} not found'.format(trigger_id)}, 404
