from ..models.baseoperator import BaseOperator


class DummyOperator(BaseOperator):
    trigger_action_name = 'DAG_DUMMY_TASK'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = 'DummyOperator'

    def json_marshal(self):
        base_operator = super().json_marshal()
        base_operator['operator'] = {'trigger_action': self.trigger_action_name,
                                     'class': self.__class__.__name__,
                                     'parameters': {}}
        return base_operator

    def get_trigger_meta(self):
        pass
