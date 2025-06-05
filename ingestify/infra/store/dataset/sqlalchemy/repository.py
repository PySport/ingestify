import itertools
import logging
import uuid
from typing import Optional, Union, List

from sqlalchemy import (
    create_engine,
    func,
    text,
    Table,
    Connection,
    union_all,
    literal,
    select,
    and_,
    Dialect,
    values,
    CTE,
    column as sqlalchemy_column,
    Integer,
    String,
)
from sqlalchemy.engine import make_url
from sqlalchemy.exc import NoSuchModuleError
from sqlalchemy.orm import Session, Query, sessionmaker, scoped_session

from ingestify.domain import File, Revision
from ingestify.domain.models import (
    Dataset,
    DatasetCollection,
    DatasetRepository,
    DatasetState,
    Selector,
)
from ingestify.domain.models.dataset.collection_metadata import (
    DatasetCollectionMetadata,
)
from ingestify.domain.models.ingestion.ingestion_job_summary import IngestionJobSummary
from ingestify.domain.models.task.task_summary import TaskSummary
from ingestify.exceptions import IngestifyError
from ingestify.utils import get_concurrency

from .tables import (
    metadata,
    dataset_table,
    file_table,
    revision_table,
    ingestion_job_summary_table,
    task_summary_table,
)

logger = logging.getLogger(__name__)


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
            pool_size=get_concurrency(),  # Maximum number of connections in the pool
            max_overflow=5,
            pool_recycle=1800,
            pool_pre_ping=True,
        )
        self.dialect = self.engine.dialect

        session_factory = sessionmaker(bind=self.engine)
        self.session = scoped_session(session_factory)

    def __getstate__(self):
        return {"url": self.url}

    def __setstate__(self, state):
        self.url = state["url"]
        self._init_engine()

    def __init__(self, url: str):
        url = self.fix_url(url)

        self.url = url
        self._init_engine()

        metadata.create_all(self.engine)

    def __del__(self):
        self.close()

    def reset(self):
        self.close()
        self._init_engine()

    def close(self):
        if hasattr(self, "engine"):
            self.engine.dispose()

    def get(self):
        return self.session()


class SqlAlchemyDatasetRepository(DatasetRepository):
    def __init__(self, session_provider: SqlAlchemySessionProvider):
        self.session_provider = session_provider

    @property
    def session(self):
        return self.session_provider.get()

    @property
    def dialect(self) -> Dialect:
        return self.session_provider.dialect

    def _upsert(
        self,
        connection: Connection,
        table: Table,
        entities: list[dict],
        immutable_rows: bool = False,
    ):
        if not entities:
            # Nothing to do
            return

        dialect = self.dialect.name
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

        if immutable_rows:
            stmt = stmt.on_conflict_do_nothing(index_elements=primary_key_columns)
        else:
            set_ = {
                name: getattr(stmt.excluded, name)
                for name, column in table.columns.items()
                if column not in primary_key_columns
            }

            stmt = stmt.on_conflict_do_update(
                index_elements=primary_key_columns, set_=set_
            )

        connection.execute(stmt)

    def _build_cte_sqlite(self, records, name: str) -> CTE:
        """SQLite has a limit of 500 compound select statements. When we have more records,
        create a nested CTE"""
        if len(records) > 500:
            return union_all(
                select(self._build_cte_sqlite(records[:500], name + "1")),
                select(self._build_cte_sqlite(records[500:], name + "2")),
            ).cte(name)

        return union_all(
            *[
                select(*(literal(value).label(key) for key, value in record.items()))
                for record in records
            ]
        ).cte(name)

    def _build_cte(self, records: list[dict], name: str) -> CTE:
        """Build a CTE from a list of dictionaries."""

        if self.dialect.name == "sqlite":
            return self._build_cte_sqlite(records, name)

        first_row = records[0]
        columns = []
        for key, value in first_row.items():
            columns.append(
                sqlalchemy_column(key, Integer if isinstance(value, int) else String)
            )

        # Prepare the data in tuples, in same order as columns
        data = [tuple(record[column.name] for column in columns) for record in records]

        return select(values(*columns, name=name).data(data)).cte(name)

    def _filter_query(
        self,
        query,
        bucket: str,
        dataset_type: Optional[str] = None,
        provider: Optional[str] = None,
        dataset_id: Optional[Union[str, List[str]]] = None,
        selector: Optional[Union[Selector, List[Selector]]] = None,
        dataset_state: Optional[List[DatasetState]] = None,
    ):
        if dataset_id is not None:
            if isinstance(dataset_id, list):
                if len(dataset_id) == 0:
                    # When an empty list is explicitly passed, make sure we
                    # return an empty DatasetCollection
                    return DatasetCollection()

                dataset_ids_cte = self._build_cte(
                    [{"dataset_id": dataset_id} for dataset_id in set(dataset_id)],
                    "dataset_ids",
                )

                query = query.select_from(
                    dataset_table.join(
                        dataset_ids_cte,
                        dataset_ids_cte.c.dataset_id == dataset_table.c.dataset_id,
                    )
                )
            else:
                query = query.filter(dataset_table.c.dataset_id == dataset_id)

        dialect = self.dialect.name

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

            first_selector = selectors[0].filtered_attributes
            keys = list(first_selector.keys())

            if keys:
                attribute_cte = self._build_cte(
                    [selector.filtered_attributes for selector in selectors],
                    "attributes",
                )

                join_conditions = []
                for k in keys:
                    if dialect == "postgresql":
                        column = dataset_table.c.identifier[k]

                        # Take the value from the first selector to determine the type.
                        # TODO: check all selectors to determine the type
                        v = first_selector[k]
                        if isinstance(v, int):
                            column = column.as_integer()
                        else:
                            column = column.as_string()
                    else:
                        column = func.json_extract(dataset_table.c.identifier, f"$.{k}")

                    join_conditions.append(attribute_cte.c[k] == column)

                query = query.select_from(
                    dataset_table.join(attribute_cte, and_(*join_conditions))
                )

        if where:
            query = query.filter(text(where))

        query = query.filter(dataset_table.c.bucket == bucket)
        if dataset_type:
            query = query.filter(dataset_table.c.dataset_type == dataset_type)
        if provider:
            query = query.filter(dataset_table.c.provider == provider)
        if dataset_state:
            query = query.filter(dataset_table.c.state.in_(dataset_state))

        return query

    def _load_datasets(self, dataset_ids: list[str]) -> list[Dataset]:
        if not dataset_ids:
            return []

        dataset_ids_cte = self._build_cte(
            [{"dataset_id": dataset_id} for dataset_id in set(dataset_ids)],
            "dataset_ids",
        )

        dataset_rows = list(
            self.session.query(dataset_table).select_from(
                dataset_table.join(
                    dataset_ids_cte,
                    dataset_ids_cte.c.dataset_id == dataset_table.c.dataset_id,
                )
            )
        )
        revisions_per_dataset = {}
        rows = (
            self.session.query(revision_table)
            .select_from(
                revision_table.join(
                    dataset_ids_cte,
                    dataset_ids_cte.c.dataset_id == revision_table.c.dataset_id,
                )
            )
            .order_by(revision_table.c.dataset_id)
        )

        for dataset_id, revisions in itertools.groupby(
            rows, key=lambda row: row.dataset_id
        ):
            revisions_per_dataset[dataset_id] = list(revisions)

        files_per_revision = {}
        rows = (
            self.session.query(file_table)
            .select_from(
                file_table.join(
                    dataset_ids_cte,
                    dataset_ids_cte.c.dataset_id == file_table.c.dataset_id,
                )
            )
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

    def _debug_query(self, q: Query):
        text_ = q.statement.compile(
            compile_kwargs={"literal_binds": True}, dialect=self.dialect
        )
        logger.debug(f"Running query: {text_}")

    def get_dataset_collection(
        self,
        bucket: str,
        dataset_type: Optional[str] = None,
        provider: Optional[str] = None,
        dataset_id: Optional[Union[str, List[str]]] = None,
        selector: Optional[Union[Selector, List[Selector]]] = None,
        metadata_only: bool = False,
        page: Optional[int] = None,
        page_size: Optional[int] = None,
        dataset_state: Optional[List[DatasetState]] = None,
    ) -> DatasetCollection:
        def apply_query_filter(query):
            return self._filter_query(
                query,
                bucket=bucket,
                dataset_type=dataset_type,
                provider=provider,
                dataset_id=dataset_id,
                selector=selector,
                dataset_state=dataset_state,
            )

        with self.session:
            # Use a contextmanager to make sure it's closed afterwards

            if not metadata_only:
                # Apply sorting by created_at in ascending order
                dataset_query = apply_query_filter(
                    self.session.query(dataset_table.c.dataset_id)
                ).order_by(dataset_table.c.created_at.asc())

                # Apply pagination if both page and page_size are provided
                if page is not None and page_size is not None:
                    offset = (page - 1) * page_size
                    dataset_query = dataset_query.offset(offset).limit(page_size)

                self._debug_query(dataset_query)
                dataset_ids = [row.dataset_id for row in dataset_query]
                datasets = self._load_datasets(dataset_ids)

                dataset_collection_metadata = DatasetCollectionMetadata(
                    last_modified=max(dataset.last_modified_at for dataset in datasets)
                    if datasets
                    else None,
                    row_count=len(datasets),
                )
            else:
                datasets = []

                metadata_result_query = (
                    apply_query_filter(
                        self.session.query(dataset_table.c.last_modified_at)
                    )
                    .order_by(dataset_table.c.last_modified_at.desc())
                    .limit(1)
                )

                self._debug_query(metadata_result_query)

                metadata_row = metadata_result_query.first()
                if metadata_row:
                    dataset_collection_metadata = DatasetCollectionMetadata(
                        last_modified=metadata_row.last_modified_at
                    )
                else:
                    dataset_collection_metadata = DatasetCollectionMetadata(
                        last_modified=None
                    )

        return DatasetCollection(dataset_collection_metadata, datasets)

    def save(self, bucket: str, dataset: Dataset):
        # Just make sure
        dataset.bucket = bucket

        self._save([dataset])

    def connect(self):
        return self.session_provider.engine.connect()

    def __del__(self):
        self.session_provider.close()

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
                self._upsert(
                    connection, revision_table, revision_entities, immutable_rows=True
                )
                self._upsert(connection, file_table, file_entities, immutable_rows=True)
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
