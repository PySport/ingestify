from abc import ABC
from dataclasses import dataclass

from .import_configuration import BaseImportConfiguration


@dataclass
class BaseDatasetDescriptor(ABC):
    configuration: BaseImportConfiguration

    @property
    def key(self):
        return ""
