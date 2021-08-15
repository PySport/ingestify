import json
import uuid

from domain.models import (Dataset, DatasetCollection, DatasetRepository,
                           Identifier, Selector)
from sqlalchemy import create_engine, func
from sqlalchemy.engine import URL, make_url
from sqlalchemy.exc import NoSuchModuleError
from sqlalchemy.orm import joinedload, sessionmaker

from .mapping import metadata


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


class SqlAlchemyDatasetRepository(DatasetRepository):
    @classmethod
    def supports(cls, url: str) -> bool:
        _url = make_url(url)
        try:
            _url.get_dialect()
        except NoSuchModuleError:
            return False
        return True

    def __init__(self, url: str):
        self.engine = create_engine(
            url,
            isolation_level="READ COMMITTED",
            json_serializer=json_serializer,
            json_deserializer=json_deserializer,
        )
        self.session = sessionmaker(bind=self.engine)()

        metadata.create_all(self.engine)

    def get_dataset_collection(
        self,
        dataset_type: str,
        provider: str,
        selector: Selector,
    ) -> DatasetCollection:
        query = (
            self.session.query(Dataset)
            .options(joinedload(Dataset.versions))
            .filter(Dataset.dataset_type == dataset_type, Dataset.provider == provider)
        )

        for k, v in selector.attributes.items():
            query = query.filter(func.json_extract(Dataset.identifier, f"$.{k}") == v)

        return DatasetCollection(list(query))

    def save(self, dataset: Dataset):
        self.session.add(dataset)
        self.session.commit()

    def next_identity(self):
        return str(uuid.uuid4())
