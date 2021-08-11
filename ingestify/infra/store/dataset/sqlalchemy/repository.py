import glob
import json
import os
import pickle
import uuid
from sqlalchemy import create_engine, DateTime
from sqlalchemy import func
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import sessionmaker, joinedload

from domain.models import (Dataset, DatasetCollection, DatasetRepository,
                           Selector, Version, Identifier)

from .mapping import metadata, dataset_table


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
    o = Identifier(
        **o
    )
    return o


# @compiles(DateTime, "mysql")
# def compile_datetime_mysql(type_, compiler, **kw):
#     return "DATETIME(6)"


class SqlAlchemyDatasetRepository(DatasetRepository):
    def __init__(self, database_url: str):
        self.engine = create_engine(
            database_url,
            isolation_level="READ COMMITTED",
            json_serializer=json_serializer,
            json_deserializer=json_deserializer
        )
        self.session = sessionmaker(bind=self.engine)()

        metadata.create_all(self.engine)

    def get_dataset_collection(
        self, dataset_type: str, provider: str, selector: Selector,
    ) -> DatasetCollection:
        query = (
            self.session
            .query(Dataset)
            .options(
                joinedload(Dataset.versions)
            )
            .filter(
                Dataset.dataset_type == dataset_type,
                Dataset.provider == provider
            )
        )

        for k, v in selector.attributes.items():
            query = query.filter(
                func.json_extract(
                    Dataset.identifier, f'$.{k}'
                ) == v
            )

        return DatasetCollection(list(query))

    def save(self, dataset: Dataset):
        self.session.add(dataset)
        self.session.commit()

    def next_identity(self):
        return str(uuid.uuid4())
