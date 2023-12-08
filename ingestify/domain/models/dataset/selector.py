from ingestify.domain.models.data_format_collection import DataFormatCollection
from ingestify.utils import AttributeBag


class Selector(AttributeBag):
    def __bool__(self):
        return len(self.filtered_attributes) > 0

    @classmethod
    def build(cls, data_formats: DataFormatCollection, **kwargs):
        return cls(
            _data_formats=data_formats.copy(),
            **kwargs
        )

    @property
    def data_formats(self):
        return self._data_formats


