from standalone_service.context.model import Context


class LocalContext(Context):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__local_dict = dict(kwargs)

    def __getitem__(self, key):
        return self.__local_dict[key]

    def __setitem__(self, key, value):
        self.__local_dict[key] = value

    def __delitem__(self, key):
        del self.__local_dict[key]

    def __iter__(self):
        return iter(self.__local_dict)

    def __len__(self):
        return len(self.__local_dict)
