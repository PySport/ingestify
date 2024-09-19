import json
import uuid
from typing import Optional, Union, List

from sqlalchemy import create_engine, func, text, tuple_
from sqlalchemy.engine import make_url
from sqlalchemy.exc import NoSuchModuleError
from sqlalchemy.orm import Session, joinedload

from ingestify.domain import File
from ingestify.domain.models import (
    Dataset,
    DatasetCollection,
    DatasetRepository,
    Identifier,
    Selector,
)
from ingestify.domain.models.dataset.collection_metadata import (
    DatasetCollectionMetadata,
)

from .mapping import dataset_table, metadata


def parse_value(v):
    try:
        return int(v)
    except ValueError:
        return v


def json_serializer(o):
    return json.dumps(o)


def json_deserializer(o):
    o = json.loads(o)
    # THIS BREAKS WHEN USING OTHER JSON COLUMNS!!
    o = Identifier(**o)
    return o


# @compiles(DateTime, "mysql")
# def compile_datetime_mysql(type_, compiler, **kw):
#     return "DATETIME(6)"


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


class SqlAlchemyDatasetRepository(DatasetRepository):
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
            json_serializer=json_serializer,
            json_deserializer=json_deserializer,
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

    def __del__(self):
        self.session.close()
        self.engine.dispose()

    def _filter_query(
        self,
        query,
        bucket: str,
        dataset_type: Optional[str] = None,
        provider: Optional[str] = None,
        dataset_id: Optional[Union[str, List[str]]] = None,
        selector: Optional[Union[Selector, List[Selector]]] = None,
    ):
        query = query.filter(Dataset.bucket == bucket)
        if dataset_type:
            query = query.filter(Dataset.dataset_type == dataset_type)
        if provider:
            query = query.filter(Dataset.provider == provider)
        if dataset_id is not None:
            if isinstance(dataset_id, list):
                if len(dataset_id) == 0:
                    # When an empty list is explicitly passed, make sure we
                    # return an empty DatasetCollection
                    return DatasetCollection()

                query = query.filter(Dataset.dataset_id.in_(dataset_id))
            else:
                query = query.filter(Dataset.dataset_id == dataset_id)

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
                    column = func.json_extract(Dataset.identifier, f"$.{k}")
                columns.append(column)

            values = []
            for selector in selectors:
                filtered_attributes = selector.filtered_attributes
                values.append(tuple([filtered_attributes[k] for k in keys]))

            query = query.filter(tuple_(*columns).in_(values))

        if where:
            query = query.filter(text(where))
        return query

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
                self.session.query(Dataset).options(joinedload(Dataset.revisions))
            )
            datasets = list(dataset_query)
        else:
            datasets = []

        metadata_result_row = apply_query_filter(
            self.session.query(
                func.min(File.modified_at).label("first_modified_at"),
                func.max(File.modified_at).label("last_modified_at"),
                func.count().label("row_count"),
            ).join(Dataset, Dataset.dataset_id == File.dataset_id)
        ).first()
        dataset_collection_metadata = DatasetCollectionMetadata(*metadata_result_row)

        return DatasetCollection(dataset_collection_metadata, datasets)

    def save(self, bucket: str, dataset: Dataset):
        # Just make sure
        dataset.bucket = bucket
        self.session.add(dataset)
        self.session.commit()

    def destroy(self, dataset: Dataset):
        self.session.delete(dataset)
        self.session.commit()

    def next_identity(self):
        return str(uuid.uuid4())
