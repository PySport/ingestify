import json
from datetime import datetime
from typing import Type, Any, TypeVar

from dataclass_factory import Schema, Factory, NameStyle
from dataclass_factory.schema_helpers import type_checker

from ingestify.domain import DatasetCreated, Identifier
from ingestify.domain.models.dataset.events import MetadataUpdated, RevisionAdded

isotime_schema = Schema(
    parser=lambda x: datetime.fromisoformat(x.replace("Z", "+00:00")),  # type: ignore
    serializer=lambda x: datetime.isoformat(x).replace("+00:00", "Z"),
)

identifier_schema = Schema(
    # json.loads(x) for backwards compatibility
    parser=lambda x: Identifier(x if isinstance(x, dict) else json.loads(x)),
    serializer=lambda x: dict(x),
)

factory = Factory(
    schemas={
        datetime: isotime_schema,
        Identifier: identifier_schema,
        DatasetCreated: Schema(
            pre_parse=type_checker(DatasetCreated.event_type, "event_type")
        ),
        MetadataUpdated: Schema(
            pre_parse=type_checker(MetadataUpdated.event_type, "event_type")
        ),
        RevisionAdded: Schema(
            pre_parse=type_checker(RevisionAdded.event_type, "event_type")
        ),
        # ClipSelectionContent: Schema(pre_parse=type_checker(ClipSelectionContent.content_type, field="contentType")),
        # TeamInfoImageContent: Schema(pre_parse=type_checker(TeamInfoImageContent.content_type, field="contentType")),
        # StaticVideoContent: Schema(pre_parse=type_checker(StaticVideoContent.content_type, field="contentType"))
    },
    default_schema=Schema(),
)

T = TypeVar("T")


def serialize(data: T, class_: Type[T] = None) -> Any:
    return factory.dump(data, class_)


def unserialize(data: Any, class_: Type[T]) -> T:
    return factory.load(data, class_)
