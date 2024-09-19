from ingestify.domain.models.data_spec_version_collection import (
    DataSpecVersionCollection,
)
from ingestify.utils import AttributeBag


class Selector(AttributeBag):
    def __bool__(self):
        return len(self.filtered_attributes) > 0

    @classmethod
    def build(cls, attributes, data_spec_versions: DataSpecVersionCollection):
        if callable(attributes):
            return cls(
                _data_spec_versions=data_spec_versions.copy(), _matcher=attributes
            )
        else:
            return cls(_data_spec_versions=data_spec_versions.copy(), **attributes)

    @property
    def is_dynamic(self):
        return "_matcher" in self.attributes

    def is_match(self, selector: dict):
        return self._matcher(selector)

    @property
    def data_spec_versions(self):
        return self._data_spec_versions

    @property
    def custom_attributes(self):
        return {
            k: v
            for k, v in self.items()
            if k not in ("_matcher", "_data_spec_versions")
        }
