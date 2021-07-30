import re

from utils import key_from_dict


class DatasetSelector:
    def __init__(self, **kwargs):
        self.attributes = kwargs
        self.key = key_from_dict(self.attributes)

    def __getattr__(self, item):
        return self.attributes[item]

    def format_string(self, string: str):
        return re.sub('\\$([a-z0-9_]+)', lambda m: str(self.attributes[m.group(1)]), string)

    def __repr__(self):
        attributes = {k: v for k, v in self.attributes.items() if not k.startswith('_')}
        return f"DatasetSelector(attributes={attributes})"
