import itertools
import logging
from typing import Optional, List


from .loader import Loader
from .dataset_store import DatasetStore
from ..domain.models.extract_job import ExtractJob

logger = logging.getLogger(__name__)


class IngestionEngine:
    def __init__(self, store: DatasetStore):

        # Note: disconnect event from loading. Event should only be used for
        #       metadata and 'loaded_files' for the actual data.
        self.store = store
        self.loader = Loader(self.store)

    def add_extract_job(self, extract_job: ExtractJob):
        self.loader.add_extract_job(extract_job)

    def load(self, dry_run: bool = False, provider: Optional[str] = None):
        self.loader.collect_and_run(dry_run=dry_run, provider=provider)

    def list_datasets(self, as_count: bool = False):
        """Consider moving this to DataStore"""
        datasets = sorted(
            self.store.get_dataset_collection(),
            key=lambda dataset_: (
                dataset_.provider,
                dataset_.dataset_type,
                str(dataset_.identifier),
            ),
        )
        if as_count:
            print(f"Count: {len(datasets)}")
        else:
            for provider, datasets_per_provider in itertools.groupby(
                datasets, key=lambda dataset_: dataset_.provider
            ):
                print(f"{provider}:")
                for dataset_type, datasets_per_type in itertools.groupby(
                    datasets_per_provider, key=lambda dataset_: dataset_.dataset_type
                ):
                    print(f"  {dataset_type}:")
                    for dataset in datasets_per_type:
                        print(
                            f"    {dataset.identifier}: {dataset.name} / {dataset.state}   {dataset.dataset_id}"
                        )
            # print(dataset.dataset_id)

    def destroy_dataset(
        self, dataset_id: Optional[str] = None, **selector
    ) -> List[str]:
        datasets = self.store.get_dataset_collection(dataset_id=dataset_id, **selector)
        dataset_ids = []
        for dataset in datasets:
            self.store.destroy_dataset(dataset)
            dataset_ids.append(dataset.dataset_id)
        return dataset_ids
