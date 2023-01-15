from datetime import datetime
from typing import Type, Any, TypeVar

from dataclass_factory import Schema, Factory, NameStyle


isotime_schema = Schema(
    parser=lambda x: datetime.fromisoformat(x.replace("Z", "+00:00")),  # type: ignore
    serializer=lambda x: datetime.isoformat(x).replace("+00:00", "Z")
)

factory = Factory(
    schemas={
        datetime: isotime_schema,
    },
    default_schema=Schema(
        name_style=NameStyle.camel_lower
    )
)

T = TypeVar("T")


def serialize(data: T, class_: Type[T] = None) -> Any:
    return factory.dump(data, class_)


def unserialize(data: Any, class_: Type[T]) -> T:
    return factory.load(data, class_)
