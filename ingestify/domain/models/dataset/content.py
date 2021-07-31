from dataclasses import dataclass
from typing import AnyStr, IO


@dataclass
class DatasetContent:
    size: int
    content_type: str

    stream: IO[AnyStr]

