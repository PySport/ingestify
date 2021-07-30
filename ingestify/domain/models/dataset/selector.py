class DatasetSelector:
    def __init__(self, **kwargs):
        self.__dict__ = kwargs

    @property
    def key(self):
        return ""
