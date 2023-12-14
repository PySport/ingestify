import copy
from dataclasses import dataclass
from typing import Dict, Union, List, Set


@dataclass
class DataSpecVersionCollection:
    items: Dict[str, Set[str]]

    @classmethod
    def from_dict(cls, items: Dict[str, Union[str, List[str], Set[str]]]):
        items_ = {}
        for data_feed_key, data_spec_versions in items.items():
            if isinstance(data_spec_versions, str):
                data_spec_versions = {data_spec_versions}
            elif isinstance(data_spec_versions, list):
                data_spec_versions = set(data_spec_versions)
            items_[data_feed_key] = data_spec_versions

        return cls(items_)

    def copy(self):
        return DataSpecVersionCollection(copy.deepcopy(self.items))

    def merge(self, other: "DataSpecVersionCollection"):
        for data_feed_key, data_spec_versions in other.items.items():
            if data_feed_key in self.items:
                self.items[data_feed_key].update(data_spec_versions)
            else:
                self.items[data_feed_key] = data_spec_versions
