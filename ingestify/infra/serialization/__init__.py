import json
from datetime import datetime
from typing import Type, Any, TypeVar

from dataclass_factory import Schema, Factory, NameStyle
from dataclass_factory.schema_helpers import type_checker

from ingestify.domain import DatasetCreated, Identifier
from ingestify.domain.models.dataset.events import MetadataUpdated, RevisionAdded
from ingestify.domain.models.event import DomainEvent


event_types = {
    DatasetCreated.event_type: DatasetCreated,
    RevisionAdded.event_type: RevisionAdded
}


def deserialize(event_dict: dict) -> DomainEvent:
    event_cls = event_types[event_dict['event_type']]
    event_dict['dataset']['revisions'] = []
    event_dict['dataset']['identifier'] = Identifier(**event_dict['dataset']['identifier'])

    return event_cls.model_validate(event_dict)


def serialize(event: DomainEvent) -> dict:
    event_dict = event.model_dump(mode="json")

    # Make sure event_type is always part of the event_dict. Pydantic might skip it when the type is ClassVar
    event_dict['event_type'] = event.event_type
    return event_dict
