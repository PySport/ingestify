from ingestify.domain.models.data_spec_version_collection import (
    DataSpecVersionCollection,
)
from ingestify.utils import AttributeBag


class Selector(AttributeBag):
    def __bool__(self):
        return len(self.filtered_attributes) > 0

    @classmethod
    def build(cls, data_spec_versions: DataSpecVersionCollection, **kwargs):
        return cls(_data_spec_versions=data_spec_versions.copy(), **kwargs)

    @property
    def data_spec_versions(self):
        return self._data_spec_versions
