import glob
import os
import pickle
import uuid
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, joinedload

from domain.models import (Dataset, DatasetCollection, DatasetRepository,
                           Selector, Version)

from .mapping import metadata


def parse_value(v):
    try:
        return int(v)
    except ValueError:
        return v


class SqlAlchemyDatasetRepository(DatasetRepository):
    def __init__(self, database_url: str):
        self.engine = create_engine(database_url, isolation_level="READ COMMITTED")
        Session = sessionmaker(bind=self.engine)
        self.session = Session()

        metadata.create_all(self.engine)

    def get_dataset_collection(
        self, selector: Selector
    ) -> DatasetCollection:
        datasets = []
        query = (
            self.session
            .query(Dataset)
            .options(
                joinedload(Dataset.versions)
            )
        )
        for dataset in query:
            print(dataset)

        return DatasetCollection(datasets)

    def save(self, dataset: Dataset):
        self.session.add(dataset)
        self.session.commit()

    def next_identity(self):
        return str(uuid.uuid4())
