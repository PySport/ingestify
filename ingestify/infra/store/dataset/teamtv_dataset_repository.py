from typing import Optional


from ingestify.domain.models import (
    Dataset,
    DatasetCollection,
    DatasetRepository,
    Selector,
)
from ingestify.infra.store.dataset.http_dataset_repository import HTTPDatasetRepository


def parse_value(v):
    try:
        return int(v)
    except ValueError:
        return v


class TeamTVDatasetRepository(DatasetRepository):
    @classmethod
    def supports(cls, url: str) -> bool:
        return url.startswith("teamtv://")

    def __init__(self, url: str):
        self.resource_group = url[9:]
        self.http_repository = HTTPDatasetRepository(
            # url=f"https://api.teamtvsport.com/api/ingestify/{bucket}"
            url="http://127.0.0.1:8080/api"
        )

    def get_dataset_collection(
        self,
        bucket: str,
        dataset_type: Optional[str] = None,
        provider: Optional[str] = None,
        selector: Optional[Selector] = None,
        **kwargs
    ) -> DatasetCollection:
        return self.http_repository.get_dataset_collection(
            bucket=bucket,
            dataset_type=dataset_type,
            provider=provider,
            selector=selector,
            **kwargs
        )

    def save(self, bucket: str, dataset: Dataset):
        return self.http_repository.save(bucket, dataset)

    def next_identity(self):
        return self.http_repository.next_identity()
