from datetime import datetime
from typing import Optional, TYPE_CHECKING

from ingestify.utils import AttributeBag

if TYPE_CHECKING:
    from ingestify.domain.models.dataset.dataset import DatasetState


class Identifier(AttributeBag):
    @property
    def last_modified(self) -> Optional[datetime]:
        return self.attributes.get("_last_modified")

    @property
    def name(self) -> Optional[str]:
        return self.attributes.get("_name")

    @property
    def metadata(self) -> dict:
        return self.attributes.get("_metadata", {})

    @property
    def state(self) -> "DatasetState":
        from ingestify.domain.models.dataset.dataset import DatasetState

        return self.attributes.get("_state", DatasetState.SCHEDULED)
