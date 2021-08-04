from typing import Dict
from string import Template

from utils import key_from_dict


class DatasetSelector:
    def __init__(self, **kwargs):
        self.attributes = kwargs
        self.key = key_from_dict(self.attributes)

    def __getattr__(self, item):
        if item in self.__dict__:
            return self.__dict__[item]
        if "attributes" in self.__dict__ and item in self.attributes:
            return self.attributes[item]
        raise AttributeError

    def format_string(self, string: str):
        return Template(string).substitute(**self.attributes)

    def matches(self, attributes: Dict) -> bool:
        for k, v in self.attributes.items():
            if attributes.get(k) != v:
                return False
        return True

    def __repr__(self):
        attributes = {k: v for k, v in self.attributes.items() if not k.startswith("_")}
        return f"DatasetSelector(attributes={attributes})"
