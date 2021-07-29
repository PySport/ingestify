from abc import ABC
from dataclasses import dataclass


@dataclass
class BaseImportConfiguration(ABC):
    @property
    def key(self):
        return ""
