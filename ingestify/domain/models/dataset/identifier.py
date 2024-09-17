from typing import TYPE_CHECKING

from ingestify.utils import key_from_dict

if TYPE_CHECKING:
    from ingestify.domain import Selector


class Identifier(dict):
    @classmethod
    def create_from_selector(cls, selector: "Selector", **kwargs):
        identifier = cls(**selector.filtered_attributes)
        identifier.update(kwargs)
        return identifier

    @property
    def key(self):
        return key_from_dict(self)

    def __hash__(self):
        return hash(self.key)

    def __str__(self):
        return "/".join([f"{k}={v}" for k, v in self.items()])
