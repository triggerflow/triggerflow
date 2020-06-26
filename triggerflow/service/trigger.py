from dataclasses import dataclass
from typing import List, Dict
from multiprocessing import Queue
from datetime import datetime


@dataclass
class Context(dict):
    global_context: dict
    workspace: str
    local_event_queue: Queue
    events: Dict[str, List[dict]]
    trigger_mapping: Dict[str, Dict[str, str]]
    triggers: Dict[str, object]
    trigger_id: str
    activation_events: List[dict]
    condition: callable
    action: callable
    modified: bool = False

    def __setitem__(self, key, value):
        self.modified = True
        super().__setitem__(key, value)


@dataclass
class Trigger:
    condition: callable
    action: callable
    context: Context
    trigger_id: str
    condition_meta: dict
    action_meta: dict
    context_parser: str
    activation_events: List[dict]
    transient: bool
    uuid: str
    workspace: str
    timestamp: str

    def to_dict(self):
        return {
            'id': self.trigger_id,
            'condition': self.condition_meta,
            'action': self.action_meta,
            'context': self.context.copy(),
            'context_parser': self.context_parser,
            'activation_events': self.activation_events,
            'transient': self.transient,
            'uuid': self.uuid,
            'workspace': self.workspace,
            'timestamp': datetime.utcnow().isoformat()
        }
