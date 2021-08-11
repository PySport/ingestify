from utils import key_from_dict

from .selector import Selector


class Identifier:
    def __init__(self, selector: Selector, **kwargs):
        # TODO: decide if we should keep track of the selector. Probaly
        # should create a classmethod `from_selector` and ditch the
        # reference to the selector
        self.selector = selector
        self.attributes = kwargs

        self.key = self.selector.key + "/" + key_from_dict(self.attributes)

    def __getattr__(self, item):
        if item in self.__dict__:
            return self.__dict__[item]
        if "attributes" in self.__dict__ and item in self.attributes:
            return self.attributes[item]
        raise AttributeError

    def __hash__(self):
        return hash(self.key)

    @property
    def filtered_attributes(self):
        return {k: v for k, v in self.attributes.items() if not k.startswith("_")}

    def __repr__(self):
        attributes = self.filtered_attributes
        return f"DatasetIdentifier(selector={self.selector}, attributes={attributes})"
