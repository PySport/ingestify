from datetime import datetime
from typing import Optional

from ingestify.utils import AttributeBag


class Identifier(AttributeBag):
    @property
    def last_modified(self) -> Optional[datetime]:
        return self.attributes.get('_last_modified')
