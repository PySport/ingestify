from dataclasses import dataclass
from typing import IO, AnyStr


@dataclass
class DatasetContent:
    size: int
    content_type: str

    stream: IO[AnyStr]
