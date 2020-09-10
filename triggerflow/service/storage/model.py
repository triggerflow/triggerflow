

class TriggerStorage:
    def __init__(self):
        pass

    def put(self, workspace: str, document_id: str, data: dict):
        raise NotImplementedError()

    def get(self, workspace: str, document_id: str):
        raise NotImplementedError()

    def delete(self, workspace: str, document_id: str):
        raise NotImplementedError()

    def get_auth(self, username: str):
        raise NotImplementedError()

    def set_auth(self, username: str, password: str):
        raise NotImplementedError()

    def list_workspaces(self):
        raise NotImplementedError()

    def create_workspace(self, workspace, event_sources, global_context):
        raise NotImplementedError()

    def workspace_exists(self, workspace):
        raise NotImplementedError()

    def delete_workspace(self, workspace):
        raise NotImplementedError()

    def document_exists(self, workspace, document_id):
        raise NotImplementedError()

    def keys(self, workspace, document_id):
        raise NotImplementedError()

    def key_exists(self, workspace, document_id, key):
        raise NotImplementedError()

    def set_key(self, workspace, document_id, key, value):
        raise NotImplementedError()

    def get_key(self, workspace, document_id, key):
        raise NotImplementedError()

    def delete_key(self, workspace, document_id, key):
        raise NotImplementedError()

    def delete_keys(self, workspace: str, document_id: str, keys: list):
        raise NotImplementedError()

    def new_trigger(self, workspace):
        raise NotImplementedError()
