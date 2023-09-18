import json
import uuid
from typing import Optional

from sqlalchemy import create_engine, func, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import NoSuchModuleError
from sqlalchemy.orm import Session, joinedload

from ingestify.domain.models import (
    Dataset,
    DatasetCollection,
    DatasetRepository,
    Identifier,
    Selector,
)

from .mapping import dataset_table, metadata


def parse_value(v):
    try:
        return int(v)
    except ValueError:
        return v


def json_serializer(o):
    if isinstance(o, Identifier):
        o = o.filtered_attributes
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
    @classmethod
    def supports(cls, url: str) -> bool:
        _url = make_url(url)
        try:
            _url.get_dialect()
        except NoSuchModuleError:
            return False
        return True

    def _init_engine(self):
        self.engine = create_engine(
            self.url,
            isolation_level="SERIALIZABLE",
            json_serializer=json_serializer,
            json_deserializer=json_deserializer,
        )
        self.session = Session(bind=self.engine)

    def __init__(self, url: str):
        self.url = url
        self._init_engine()

        metadata.create_all(self.engine)

    def __getstate__(self):
        return {"url": self.url}

    def __setstate__(self, state):
        self.url = state["url"]
        self._init_engine()

    def get_dataset_collection(
        self,
        bucket: str,
        dataset_type: Optional[str] = None,
        provider: Optional[str] = None,
        selector: Optional[Selector] = None,
    ) -> DatasetCollection:
        query = (
            self.session.query(Dataset)
            .options(joinedload(Dataset.versions))
            .filter(Dataset.bucket == bucket)
        )
        if dataset_type:
            query = query.filter(Dataset.dataset_type == dataset_type)
        if provider:
            query = query.filter(Dataset.provider == provider)

        dialect = self.session.bind.dialect.name

        where, selector = selector.split("where")
        if selector:
            for k, v in selector.attributes.items():
                if dialect == "postgresql":
                    column = dataset_table.c.identifier[k]
                    if isint(v):
                        column = column.as_integer()
                    elif isfloat(v):
                        column = column.as_float()
                    else:
                        column = column.as_string()
                    query = query.filter(column == v)
                else:
                    query = query.filter(
                        func.json_extract(Dataset.identifier, f"$.{k}") == v
                    )

        if where:
            query = query.filter(text(where))

        return DatasetCollection(list(query))

    def save(self, bucket: str, dataset: Dataset):
        # Just make sure
        dataset.bucket = bucket
        self.session.add(dataset)
        self.session.commit()

    def next_identity(self):
        return str(uuid.uuid4())
