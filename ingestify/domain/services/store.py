from domain.models import BaseDatasetDescriptor


class Store:
    async def get_metadata(self, dataset_descriptor: BaseDatasetDescriptor):
        return None

    async def add(self, dataset_descriptor: BaseDatasetDescriptor, data):
        pass
