import datetime
from pathlib import Path
from typing import Optional

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

from ingestify.domain import Identifier, DataSpecVersionCollection, Selector
from ingestify.domain.models.dataset.dataset import DatasetState
from ingestify.domain.models.ingestion.ingestion_job_summary import IngestionJobState

from ingestify.domain.models.task.task_summary import Operation, TaskState
from ingestify.domain.models.timing import Timing
from ingestify.domain.models.dataset.revision import RevisionState


def JSONType(serializer=None, deserializer=None):
    class _JsonType(TypeDecorator):
        cache_ok = True
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

    def process_bind_param(self, value: Optional[datetime.datetime], dialect):
        if not value:
            return None

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


class RevisionStateString(TypeDecorator):
    impl = String(255)

    def process_bind_param(self, value: RevisionState, dialect):
        return value.value

    def process_result_value(self, value, dialect):
        if not value:
            return RevisionState.PENDING_VALIDATION

        return RevisionState[value]


class OperationString(TypeDecorator):
    impl = String(255)

    def process_bind_param(self, value: Operation, dialect):
        return value.value

    def process_result_value(self, value, dialect):
        if not value:
            return value

        return Operation[value]


class TaskStateString(TypeDecorator):
    impl = String(255)

    def process_bind_param(self, value: TaskState, dialect):
        return value.value

    def process_result_value(self, value, dialect):
        if not value:
            return value

        return TaskState[value]


class IngestionJobStateString(TypeDecorator):
    impl = String(255)

    def process_bind_param(self, value: IngestionJobState, dialect):
        return value.value

    def process_result_value(self, value, dialect):
        if not value:
            return value

        return IngestionJobState[value]


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
    Column("identifier", JSONType(deserializer=lambda item: Identifier(**item))),
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
    Column("state", RevisionStateString, default=RevisionState.PENDING_VALIDATION),
    Column("source", JSONType()),
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

ingestion_job_summary_table = Table(
    "ingestion_job_summary",
    metadata,
    Column("ingestion_job_summary_id", String(255), primary_key=True),
    Column("ingestion_job_id", String(255), index=True),
    # From the IngestionPlan
    Column("source_name", String(255)),
    Column("provider", String(255)),
    Column("dataset_type", String(255)),
    Column(
        "data_spec_versions",
        JSONType(
            serializer=lambda data_spec_versions: {
                key: list(value) for key, value in data_spec_versions.items()
            },
            deserializer=lambda data_spec_versions: DataSpecVersionCollection.from_dict(
                data_spec_versions
            ),
        ),
    ),
    Column(
        "selector",
        JSONType(
            serializer=lambda selector: selector.filtered_attributes,
            deserializer=lambda selector: Selector(**selector),
        ),
    ),
    Column("started_at", TZDateTime(6)),
    Column("ended_at", TZDateTime(6)),
    # Some task counters
    Column("state", IngestionJobStateString),
    Column("successful_tasks", Integer),
    Column("ignored_successful_tasks", Integer),
    Column("skipped_datasets", Integer),
    Column("failed_tasks", Integer),
    Column(
        "timings",
        JSONType(
            serializer=lambda timings: [
                # Timing is probably already a dictionary. Load it into Timing first, so it can be dumped
                # in json mode
                Timing.model_validate(timing).model_dump(mode="json")
                for timing in timings
            ],
            deserializer=lambda timings: [
                Timing.model_validate(timing) for timing in timings
            ],
        ),
    ),
    # Column(
    #     "task_summaries",
    #     JSONType(
    #         serializer=lambda task_summaries: [
    #             task_summary.model_dump(mode="json") for task_summary in task_summaries
    #         ],
    #         deserializer=lambda task_summaries: [
    #             TaskSummary.model_validate(task_summary)
    #             for task_summary in task_summaries
    #         ],
    #     ),
    # ),
)


task_summary_table = Table(
    "task_summary",
    metadata,
    Column(
        "ingestion_job_summary_id",
        String(255),
        ForeignKey("ingestion_job_summary.ingestion_job_summary_id"),
        primary_key=True,
    ),
    Column("task_id", String(255), primary_key=True),
    Column("started_at", TZDateTime(6)),
    Column("ended_at", TZDateTime(6)),
    Column("operation", OperationString),
    Column(
        "dataset_identifier", JSONType(deserializer=lambda item: Identifier(**item))
    ),
    Column("persisted_file_count", Integer),
    Column("bytes_retrieved", Integer),
    Column("last_modified", TZDateTime(6)),
    Column("state", TaskStateString),
    Column(
        "timings",
        JSONType(
            serializer=lambda timings: [
                Timing.model_validate(timing).model_dump(mode="json")
                for timing in timings
            ],
            deserializer=lambda timings: [
                Timing.model_validate(timing) for timing in timings
            ],
        ),
    ),
    # Column("description", String(255)),
    # Column("created_at", TZDateTime(6)),
    # Column("state", RevisionStateString, default=RevisionState.PENDING_VALIDATION),
    # Column("source", JSONType()),
)
#
#
# mapper_registry = registry()
#
# mapper_registry.map_imperatively(
#     Dataset,
#     dataset_table,
#     properties={
#         "revisions": relationship(
#             Revision,
#             backref="dataset",
#             order_by=revision_table.c.revision_id,
#             lazy="selectin",
#             cascade="all, delete-orphan",
#         ),
#     },
# )
#
# mapper_registry.map_imperatively(
#     Revision,
#     revision_table,
#     properties={
#         "modified_files": relationship(
#             File,
#             order_by=file_table.c.file_id,
#             primaryjoin="and_(Revision.revision_id==File.revision_id, Revision.dataset_id==File.dataset_id)",
#             lazy="selectin",
#             cascade="all, delete-orphan",
#         )
#     },
# )
#
#
# mapper_registry.map_imperatively(File, file_table)
#
# mapper_registry.map_imperatively(
#     IngestionJobSummary,
#     ingestion_job_summary,
#     properties={
#         "task_summaries": relationship(
#             TaskSummary,
#             backref="ingestion_job_summary",
#             # order_by=task_summary_table.c.revision_id,
#             lazy="selectin",
#             cascade="all, delete-orphan",
#         ),
#     },
# )
#
#
# mapper_registry.map_imperatively(TaskSummary, task_summary_table)
