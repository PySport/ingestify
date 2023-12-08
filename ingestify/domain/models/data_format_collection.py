import copy
from dataclasses import dataclass
from typing import Dict, Union, List, Set


@dataclass
class DataFormatCollection:
    items: Dict[str, Set[str]]

    @classmethod
    def from_dict(cls, items: Dict[str, Union[str, List[str], Set[str]]]):
        items_ = {}
        for filename, formats in items.items():
            if isinstance(formats, str):
                formats = {formats}
            elif isinstance(formats, list):
                formats = set(formats)
            items_[filename] = formats

        return cls(items_)

    def copy(self):
        return DataFormatCollection(copy.deepcopy(self.items))

    def merge(self, other: 'DataFormatCollection'):
        for filename, formats in other.items.items():
            if filename in self.items:
                self.items[filename].update(formats)
            else:
                self.items[filename] = formats


