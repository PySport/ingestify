import itertools
import json
import uuid
from collections import defaultdict
from typing import Optional, Union, List

from sqlalchemy import (
    create_engine,
    func,
    text,
    tuple_,
    Table,
    insert,
    Transaction,
    Connection,
)
from sqlalchemy.engine import make_url
from sqlalchemy.exc import NoSuchModuleError
from sqlalchemy.orm import Session, joinedload

from ingestify.domain import File, Revision
from ingestify.domain.models import (
    Dataset,
    DatasetCollection,
    DatasetRepository,
    Identifier,
    Selector,
)
from ingestify.domain.models.base import BaseModel
from ingestify.domain.models.dataset.collection_metadata import (
    DatasetCollectionMetadata,
)
from ingestify.domain.models.ingestion.ingestion_job_summary import IngestionJobSummary
from ingestify.domain.models.task.task_summary import TaskSummary
from ingestify.exceptions import IngestifyError

from .tables import (
    metadata,
    dataset_table,
    file_table,
    revision_table,
    ingestion_job_summary_table,
    task_summary_table,
)


def parse_value(v):
    try:
        return int(v)
    except ValueError:
        return v


def isfloat(x):
    try:
        a = float(x)
    except (TypeError, ValueError):
        return False
    else:
        return True


def isint(x):
    try:
        a = float(x)
        b = int(a)
    except (TypeError, ValueError):
        return False
    else:
        return a == b


class SqlAlchemySessionProvider:
    @staticmethod
    def fix_url(url: str) -> str:
        if url.startswith("postgres://"):
            url = url.replace("postgres://", "postgresql://")
        return url

    @classmethod
    def supports(cls, url: str) -> bool:
        url = cls.fix_url(url)

        _url = make_url(url)
        try:
            _url.get_dialect()
        except NoSuchModuleError:
            return False
        return True

    def _init_engine(self):
        self.engine = create_engine(
            self.url,
            # Use the default isolation level, don't need SERIALIZABLE
            # isolation_level="SERIALIZABLE",
        )
        self.session = Session(bind=self.engine)

    def __init__(self, url: str):
        url = self.fix_url(url)

        self.url = url
        self._init_engine()

        metadata.create_all(self.engine)

    def __getstate__(self):
        return {"url": self.url}

    def __setstate__(self, state):
        self.url = state["url"]
        self._init_engine()

    def _close_engine(self):
        if hasattr(self, "session"):
            self.session.close()
            self.engine.dispose()

    def __del__(self):
        self._close_engine()

    def reset(self):
        self._close_engine()
        self._init_engine()

    def get(self):
        return self.session


class SqlAlchemyDatasetRepository(DatasetRepository):
    def __init__(self, session_provider: SqlAlchemySessionProvider):
        self.session_provider = session_provider

    @property
    def session(self):
        return self.session_provider.get()

    def _upsert(self, connection: Connection, table: Table, entities: list[dict]):
        dialect = self.session.bind.dialect.name
        if dialect == "mysql":
            from sqlalchemy.dialects.mysql import insert
        elif dialect == "postgresql":
            from sqlalchemy.dialects.postgresql import insert
        elif dialect == "sqlite":
            from sqlalchemy.dialects.sqlite import insert
        else:
            raise IngestifyError(f"Don't know how to do an upsert in {dialect}")

        stmt = insert(table).values(entities)

        primary_key_columns = [column for column in table.columns if column.primary_key]

        set_ = {
            name: getattr(stmt.excluded, name)
            for name, column in table.columns.items()
            if column not in primary_key_columns
        }

        stmt = stmt.on_conflict_do_update(index_elements=primary_key_columns, set_=set_)

        connection.execute(stmt)

    def _filter_query(
        self,
        query,
        bucket: str,
        dataset_type: Optional[str] = None,
        provider: Optional[str] = None,
        dataset_id: Optional[Union[str, List[str]]] = None,
        selector: Optional[Union[Selector, List[Selector]]] = None,
    ):
        query = query.filter(dataset_table.c.bucket == bucket)
        if dataset_type:
            query = query.filter(dataset_table.c.dataset_type == dataset_type)
        if provider:
            query = query.filter(dataset_table.c.provider == provider)
        if dataset_id is not None:
            if isinstance(dataset_id, list):
                if len(dataset_id) == 0:
                    # When an empty list is explicitly passed, make sure we
                    # return an empty DatasetCollection
                    return DatasetCollection()

                query = query.filter(dataset_table.c.dataset_id.in_(dataset_id))
            else:
                query = query.filter(dataset_table.c.dataset_id == dataset_id)

        dialect = self.session.bind.dialect.name

        if not isinstance(selector, list):
            where, selector = selector.split("where")
        else:
            where = None

        if selector:
            if isinstance(selector, list):
                selectors = selector
            else:
                selectors = [selector]

            if not selectors:
                raise ValueError("Selectors must contain at least one item")

            keys = list(selectors[0].filtered_attributes.keys())

            columns = []
            first_selector = selectors[0].filtered_attributes

            # Create a query like this:
            #  SELECT * FROM dataset WHERE (column1, column2, column3) IN ((1, 2, 3), (4, 5, 6), (7, 8, 9))
            for k in keys:
                if dialect == "postgresql":
                    column = dataset_table.c.identifier[k]

                    # Take the value from the first selector to determine the type.
                    # TODO: check all selectors to determine the type
                    v = first_selector[k]
                    if isint(v):
                        column = column.as_integer()
                    elif isfloat(v):
                        column = column.as_float()
                    else:
                        column = column.as_string()
                else:
                    column = func.json_extract(dataset_table.c.identifier, f"$.{k}")
                columns.append(column)

            values = []
            for selector in selectors:
                filtered_attributes = selector.filtered_attributes
                values.append(tuple([filtered_attributes[k] for k in keys]))

            query = query.filter(tuple_(*columns).in_(values))

        if where:
            query = query.filter(text(where))
        return query

    def load_datasets(self, dataset_ids: list[str]) -> list[Dataset]:
        if not dataset_ids:
            return []

        dataset_rows = list(
            self.session.query(dataset_table).filter(
                dataset_table.c.dataset_id.in_(dataset_ids)
            )
        )
        revisions_per_dataset = {}
        rows = (
            self.session.query(revision_table)
            .filter(revision_table.c.dataset_id.in_(dataset_ids))
            .order_by(revision_table.c.dataset_id)
        )

        for dataset_id, revisions in itertools.groupby(
            rows, key=lambda row: row.dataset_id
        ):
            revisions_per_dataset[dataset_id] = list(revisions)

        files_per_revision = {}
        rows = (
            self.session.query(file_table)
            .filter(file_table.c.dataset_id.in_(dataset_ids))
            .order_by(file_table.c.dataset_id, file_table.c.revision_id)
        )

        for (dataset_id, revision_id), files in itertools.groupby(
            rows, key=lambda row: (row.dataset_id, row.revision_id)
        ):
            files_per_revision[(dataset_id, revision_id)] = list(files)

        datasets = []
        for dataset_row in dataset_rows:
            dataset_id = dataset_row.dataset_id
            revisions = []
            for revision_row in revisions_per_dataset.get(dataset_id, []):
                files = [
                    File.model_validate(file_row)
                    for file_row in files_per_revision.get(
                        (dataset_id, revision_row.revision_id), []
                    )
                ]
                revision = Revision.model_validate(
                    {**revision_row._mapping, "modified_files": files}
                )
                revisions.append(revision)

            datasets.append(
                Dataset.model_validate({**dataset_row._mapping, "revisions": revisions})
            )
        return datasets

    def get_dataset_collection(
        self,
        bucket: str,
        dataset_type: Optional[str] = None,
        provider: Optional[str] = None,
        dataset_id: Optional[Union[str, List[str]]] = None,
        selector: Optional[Union[Selector, List[Selector]]] = None,
        metadata_only: bool = False,
    ) -> DatasetCollection:
        def apply_query_filter(query):
            return self._filter_query(
                query,
                bucket=bucket,
                dataset_type=dataset_type,
                provider=provider,
                dataset_id=dataset_id,
                selector=selector,
            )

        if not metadata_only:
            dataset_query = apply_query_filter(
                self.session.query(dataset_table.c.dataset_id)
            )
            dataset_ids = [row.dataset_id for row in dataset_query]
            datasets = self.load_datasets(dataset_ids)
        else:
            datasets = []

        metadata_result_row = apply_query_filter(
            self.session.query(
                func.max(dataset_table.c.last_modified_at).label("last_modified_at"),
                func.count().label("row_count"),
            )
        ).first()
        dataset_collection_metadata = DatasetCollectionMetadata(*metadata_result_row)

        return DatasetCollection(dataset_collection_metadata, datasets)

    def save(self, bucket: str, dataset: Dataset):
        # Just make sure
        dataset.bucket = bucket

        self._save([dataset])

    def connect(self):
        return self.session_provider.engine.connect()

    def _save(self, datasets: list[Dataset]):
        """Only do upserts. Never delete. Rows get only deleted when an entire Dataset is removed."""
        datasets_entities = []
        revision_entities = []
        file_entities = []

        for dataset in datasets:
            datasets_entities.append(dataset.model_dump(exclude={"revisions"}))
            for revision in dataset.revisions:
                revision_entities.append(
                    {
                        **revision.model_dump(
                            exclude={"is_squashed", "modified_files"}
                        ),
                        "dataset_id": dataset.dataset_id,
                    }
                )
                for file in revision.modified_files:
                    file_entities.append(
                        {
                            **file.model_dump(),
                            "dataset_id": dataset.dataset_id,
                            "revision_id": revision.revision_id,
                        }
                    )

        with self.connect() as connection:
            try:
                self._upsert(connection, dataset_table, datasets_entities)
                self._upsert(connection, revision_table, revision_entities)
                self._upsert(connection, file_table, file_entities)
            except Exception:
                connection.rollback()
                raise
            else:
                connection.commit()

    def destroy(self, dataset: Dataset):
        with self.connect() as connection:
            try:
                # Delete modified files related to the dataset
                file_table.delete().where(
                    file_table.c.dataset_id == dataset.dataset_id
                ).execute()

                # Delete revisions related to the dataset
                revision_table.delete().where(
                    revision_table.c.dataset_id == dataset.dataset_id
                ).execute()

                # Delete the dataset itself
                dataset_table.delete().where(
                    dataset_table.c.dataset_id == dataset.dataset_id
                ).execute()

                connection.commit()
            except Exception:
                connection.rollback()
                raise

    def next_identity(self):
        return str(uuid.uuid4())

    # TODO: consider moving the IngestionJobSummary methods to a different Repository
    def save_ingestion_job_summary(self, ingestion_job_summary: IngestionJobSummary):
        ingestion_job_summary_entities = [
            ingestion_job_summary.model_dump(exclude={"task_summaries"})
        ]
        task_summary_entities = []
        for task_summary in ingestion_job_summary.task_summaries:
            task_summary_entities.append(
                {
                    **task_summary.model_dump(),
                    "ingestion_job_summary_id": ingestion_job_summary.ingestion_job_summary_id,
                }
            )

        with self.session_provider.engine.connect() as connection:
            try:
                self._upsert(
                    connection,
                    ingestion_job_summary_table,
                    ingestion_job_summary_entities,
                )
                if task_summary_entities:
                    self._upsert(connection, task_summary_table, task_summary_entities)
            except Exception:
                connection.rollback()
                raise
            else:
                connection.commit()

    def load_ingestion_job_summaries(self) -> list[IngestionJobSummary]:
        ingestion_job_summary_ids = [
            row.ingestion_job_summary_id
            for row in self.session.query(
                ingestion_job_summary_table.c.ingestion_job_summary_id
            )
        ]

        ingestion_job_summary_rows = list(
            self.session.query(ingestion_job_summary_table).filter(
                ingestion_job_summary_table.c.ingestion_job_summary_id.in_(
                    ingestion_job_summary_ids
                )
            )
        )

        task_summary_entities_per_job_summary = {}
        rows = (
            self.session.query(task_summary_table)
            .filter(
                task_summary_table.c.ingestion_job_summary_id.in_(
                    ingestion_job_summary_ids
                )
            )
            .order_by(task_summary_table.c.ingestion_job_summary_id)
        )

        for ingestion_job_summary_id, task_summaries_rows in itertools.groupby(
            rows, key=lambda row: row.ingestion_job_summary_id
        ):
            task_summary_entities_per_job_summary[ingestion_job_summary_id] = list(
                task_summaries_rows
            )

        ingestion_job_summaries = []
        for ingestion_job_summary_row in ingestion_job_summary_rows:
            task_summaries = [
                TaskSummary.model_validate(row)
                for row in task_summary_entities_per_job_summary.get(
                    ingestion_job_summary_row.ingestion_job_summary_id, []
                )
            ]

            ingestion_job_summaries.append(
                IngestionJobSummary.model_validate(
                    {
                        **ingestion_job_summary_row._mapping,
                        "task_summaries": task_summaries,
                    }
                )
            )
        return ingestion_job_summaries
