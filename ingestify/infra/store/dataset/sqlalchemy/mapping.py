import datetime
from dataclasses import is_dataclass, asdict
from pathlib import Path

from sqlalchemy import (
    JSON,
    BigInteger,
    Column,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Integer,
    MetaData,
    String,
    Table,
    TypeDecorator,
)
from sqlalchemy.orm import registry, relationship

from ingestify.domain import Selector
from ingestify.domain.models import Dataset, File, Revision
from ingestify.domain.models.dataset.dataset import DatasetState
from ingestify.domain.models.extraction.extraction_job_summary import ExtractionJobSummary


def JSONType(serializer=None, deserializer=None):
    class _JsonType(TypeDecorator):
        impl = JSON

        def process_bind_param(self, value, dialect):
            if serializer is not None:
                return serializer(value)
            return value

        def process_result_value(self, value, dialect):
            if deserializer is not None:
                return deserializer(value)
            return value
    return _JsonType


class TZDateTime(TypeDecorator):
    impl = DateTime
    LOCAL_TIMEZONE = datetime.datetime.utcnow().astimezone().tzinfo
    cache_ok = True

    def process_bind_param(self, value: datetime, dialect):
        if value.tzinfo is None:
            value = value.astimezone(self.LOCAL_TIMEZONE)

        return value.astimezone(datetime.timezone.utc)

    def process_result_value(self, value, dialect):
        if not value:
            return value

        if value.tzinfo is None:
            return value.replace(tzinfo=datetime.timezone.utc)

        return value.astimezone(datetime.timezone.utc)


class PathString(TypeDecorator):
    impl = String(255)

    def process_bind_param(self, value: Path, dialect):
        return str(value)

    def process_result_value(self, value, dialect):
        if not value:
            return value

        return Path(value)


class DatasetStateString(TypeDecorator):
    impl = String(255)

    def process_bind_param(self, value: DatasetState, dialect):
        return value.value

    def process_result_value(self, value, dialect):
        if not value:
            return value

        return DatasetState[value]


mapper_registry = registry()

metadata = MetaData()

dataset_table = Table(
    "dataset",
    metadata,
    Column("bucket", String(255), default=None),
    Column("dataset_id", String(255), primary_key=True),
    Column("provider", String(255)),
    Column("dataset_type", String(255)),
    Column("state", DatasetStateString),
    Column("name", String(255)),
    Column("identifier", JSON),
    Column("metadata", JSON),
    Column("created_at", TZDateTime(6)),
    Column("updated_at", TZDateTime(6)),
)

revision_table = Table(
    "revision",
    metadata,
    Column(
        "dataset_id", String(255), ForeignKey("dataset.dataset_id"), primary_key=True
    ),
    Column("revision_id", Integer, primary_key=True),
    Column("description", String(255)),
    Column("created_at", TZDateTime(6)),
)
file_table = Table(
    "file",
    metadata,
    Column("dataset_id", String(255), primary_key=True),
    Column("revision_id", Integer, primary_key=True),
    Column("file_id", String(255), primary_key=True),
    Column("created_at", TZDateTime(6)),
    Column("modified_at", TZDateTime(6)),
    Column("tag", String(255)),
    Column("content_type", String(255)),
    Column("size", BigInteger),
    Column("data_feed_key", String(255)),
    Column("data_spec_version", String(255)),
    Column("data_serialization_format", String(255)),
    Column("storage_compression_method", String(255)),
    Column("storage_size", BigInteger),
    Column("storage_path", PathString),
    ForeignKeyConstraint(
        ("dataset_id", "revision_id"),
        [revision_table.c.dataset_id, revision_table.c.revision_id],
        ondelete="CASCADE",
    ),
)


mapper_registry.map_imperatively(
    Dataset,
    dataset_table,
    properties={
        "revisions": relationship(
            Revision,
            backref="dataset",
            order_by=revision_table.c.revision_id,
            lazy="selectin",
            cascade="all, delete-orphan",
        ),
    },
)

mapper_registry.map_imperatively(
    Revision,
    revision_table,
    properties={
        "modified_files": relationship(
            File,
            order_by=file_table.c.file_id,
            primaryjoin="and_(Revision.revision_id==File.revision_id, Revision.dataset_id==File.dataset_id)",
            lazy="selectin",
            cascade="all, delete-orphan",
        )
    },
)


mapper_registry.map_imperatively(File, file_table)


extraction_job_summary = Table(
    "extraction_job_summary",
    metadata,
    Column("extraction_job_id", String(255), primary_key=True),

    # From the ExtractionPlan
    Column("source_name", String(255)),
    Column("dataset_type", String(255)),
    Column("data_spec_versions", JSONType()),
    Column("selector", JSONType(serializer=lambda selector: selector.filtered_attributes)),

    Column("started_at", TZDateTime(6)),
    Column("finished_at", TZDateTime(6)),
    # Column("timings", DataClassJSONType),
    # Column("task_summaries", DataClassJSONType)
)

mapper_registry.map_imperatively(ExtractionJobSummary, extraction_job_summary)
