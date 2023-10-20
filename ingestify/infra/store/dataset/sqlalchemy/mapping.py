import datetime
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

from ingestify.domain.models import Dataset, File, Version
from ingestify.domain.models.dataset.dataset import DatasetState


class TZDateTime(TypeDecorator):
    impl = DateTime
    LOCAL_TIMEZONE = datetime.datetime.utcnow().astimezone().tzinfo

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

version_table = Table(
    "version",
    metadata,
    Column(
        "dataset_id", String(255), ForeignKey("dataset.dataset_id"), primary_key=True
    ),
    Column("version_id", Integer, primary_key=True),
    Column("description", String(255)),
    Column("created_at", TZDateTime(6)),
)

file_table = Table(
    "file",
    metadata,
    Column("dataset_id", String(255), primary_key=True),
    Column("version_id", Integer, primary_key=True),
    Column("filename", String(255), primary_key=True),
    Column("modified_at", TZDateTime(6)),
    Column("tag", String(255)),
    Column("content_type", String(255)),
    Column("size", BigInteger),
    Column("storage_size", BigInteger),
    Column("path", PathString),
    ForeignKeyConstraint(
        ("dataset_id", "version_id"),
        [version_table.c.dataset_id, version_table.c.version_id],
        ondelete="CASCADE",
    ),
)

mapper_registry.map_imperatively(
    Dataset,
    dataset_table,
    properties={
        "versions": relationship(
            Version,
            backref="dataset",
            order_by=version_table.c.version_id,
            lazy="joined",
            cascade="all, delete-orphan",
        ),
    },
)

mapper_registry.map_imperatively(
    Version,
    version_table,
    properties={
        "modified_files": relationship(
            File,
            order_by=file_table.c.filename,
            primaryjoin="and_(Version.version_id==File.version_id, Version.dataset_id==File.dataset_id)",
            lazy="joined",
            cascade="all, delete-orphan",
        )
    },
)


mapper_registry.map_imperatively(File, file_table)
