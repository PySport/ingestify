from abc import ABC
from dataclasses import dataclass
from typing import Generic, TypeVar

from .import_configuration import BaseImportConfiguration


T = TypeVar('T')


@dataclass
class BaseDatasetDescriptor(ABC, Generic[T]):
    configuration: T

    @property
    def key(self):
        return ""
