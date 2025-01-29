from ingestify.domain import DatasetCreated
from ingestify.domain.models.dataset.events import RevisionAdded
from ingestify.domain.models.event import DomainEvent


event_types = {
    DatasetCreated.event_type: DatasetCreated,
    RevisionAdded.event_type: RevisionAdded,
}


def deserialize(event_dict: dict) -> DomainEvent:
    event_cls = event_types[event_dict["event_type"]]
    return event_cls.model_validate(event_dict)


def serialize(event: DomainEvent) -> dict:
    event_dict = event.model_dump(mode="json")

    # Make sure event_type is always part of the event_dict. Pydantic might skip it when the type is ClassVar
    event_dict["event_type"] = event.event_type
    return event_dict
