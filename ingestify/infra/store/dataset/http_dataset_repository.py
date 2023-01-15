import glob
import os
import pickle
import uuid
from typing import Optional

import requests

from ingestify.domain.models import (
    Dataset,
    DatasetCollection,
    DatasetRepository,
    Selector,
)
from ingestify.infra.serialization import unserialize, serialize


def parse_value(v):
    try:
        return int(v)
    except ValueError:
        return v


class HTTPDatasetRepository(DatasetRepository):
    @classmethod
    def supports(cls, url: str) -> bool:
        return url.startswith("https://") or url.startswith("http://")

    def __init__(self, url: str, headers: Optional[dict] = None):
        self.base_url = url
        self.headers = headers

    def _get(self, bucket: str, params: dict):
        url = self.base_url + f"/buckets/{bucket}/datasets"
        response = requests.get(
            url,
            params=params,
            headers=self.headers
        )
        response.raise_for_status()
        return response.json()

    def _patch(self, bucket: str, path: str, body: dict):
        url = self.base_url + f"/buckets/{bucket}/datasets"
        response = requests.patch(
            url + path,
            json=body
        )
        response.raise_for_status()

    def get_dataset_collection(
        self,
        bucket: str,
        dataset_type: Optional[str] = None,
        provider: Optional[str] = None,
        selector: Optional[Selector] = None,
        **kwargs
    ) -> DatasetCollection:
        params = {}
        if dataset_type:
            params['dataset_type'] = dataset_type
        if provider:
            params['provider'] = provider
        if selector:
            params['selector'] = str(selector)

        data = self._get(
            bucket,
            params=params
        )
        datasets = []
        for row in data:
            dataset = unserialize(
                row,
                Dataset
            )
            datasets.append(dataset)

        return DatasetCollection(datasets)

    def save(self, bucket: str, dataset: Dataset):
        self._patch(
            bucket,
            dataset.dataset_id,
            serialize(dataset)
        )

    def next_identity(self):
        return str(uuid.uuid4())
