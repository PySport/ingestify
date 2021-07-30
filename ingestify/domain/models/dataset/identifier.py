from utils import key_from_dict
from .selector import DatasetSelector


class DatasetIdentifier:
    def __init__(self, selector: DatasetSelector, **kwargs):
        self.selector = selector
        self.attributes = kwargs

        self.key = self.selector.key + "/" + key_from_dict(self.attributes)

    def __getattr__(self, item):
        return self.attributes[item]

    def __hash__(self):
        return hash(self.key)

    def __repr__(self):
        attributes = {k: v for k, v in self.attributes.items() if not k.startswith('_')}
        return f"DatasetIdentifier(selector={self.selector}, attributes={attributes})"
