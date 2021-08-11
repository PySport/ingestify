
from sqlalchemy import Column, JSON, BigInteger, ForeignKeyConstraint
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
    Column('identifier', JSON)
)

version_table = Table(
    'version',
    metadata,
    Column('dataset_id', String(255), ForeignKey('dataset.dataset_id'), primary_key=True),
    Column('version_id', Integer, primary_key=True),
    Column('description', String(255)),
    Column('created_at', DateTime(6))
)

file_table = Table(
    'file',
    metadata,
    Column('dataset_id', String(255)),
    Column('version_id', Integer),
    Column('filename', String(255)),

    Column('file_key', String(255), primary_key=True),
    Column('modified_at', DateTime(6)),
    Column('tag', String(255)),
    Column('content_type', String(255)),
    Column('size', BigInteger),

    ForeignKeyConstraint(
        ['dataset_id', 'version_id'],
        [version_table.c.dataset_id, version_table.c.version_id],
        ondelete='CASCADE'
    )
)

mapper_registry.map_imperatively(Dataset, dataset_table, properties={
    'versions': relationship(Version, backref='dataset', order_by=version_table.c.version_id),
})

mapper_registry.map_imperatively(Version, version_table, properties={
    'modified_files': relationship(
        File,
        order_by=file_table.c.filename,
        primaryjoin="and_(Version.version_id==File.version_id, Version.dataset_id==File.dataset_id)"
    )
})


mapper_registry.map_imperatively(File, file_table)