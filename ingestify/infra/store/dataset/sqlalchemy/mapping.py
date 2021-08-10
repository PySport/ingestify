
from sqlalchemy import Column, JSON
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import DateTime
from sqlalchemy.orm import registry
from sqlalchemy.orm import relationship

from domain.models import Dataset, Version, File

mapper_registry = registry()

metadata = MetaData()

dataset_table = Table(
    'dataset',
    metadata,
    Column('dataset_id', String(255), primary_key=True),
    Column('created_at', DateTime)
)

version_table = Table(
    'version',
    metadata,
    Column('version_id', Integer, primary_key=True),
    Column('dataset_id', String(255), ForeignKey('dataset.dataset_id'), primary_key=True),
    Column('description', String(255)),
    Column('created_at', DateTime)
)

file_table = Table(
    'file',
    metadata,
    Column('dataset_id', String(255), ForeignKey('version.dataset_id')),
    Column('version_id', Integer, ForeignKey('version.version_id')),
    Column('filename', String(255)),

    Column('file_key', String(255), primary_key=True),

    #Column('description', String(255)),
    #Column('created_at', DateTime),
    #Column('files', JSON)
)

mapper_registry.map_imperatively(Dataset, dataset_table, properties={
    'versions': relationship(Version, backref='dataset', order_by=version_table.c.version_id),
})

mapper_registry.map_imperatively(Version, version_table, properties={
    'modified_files': relationship(
        File,
        order_by=file_table.c.filename,
        primaryjoin="and_(Version.version_id==File.version_id, Version.dataset_id==File.dataset_id)"
        #foreign_keys=[file_table.c.dataset_id, file_table.c.version_id]
    ),
})


mapper_registry.map_imperatively(File, file_table)