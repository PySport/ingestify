import itertools
import logging
from typing import Optional, List, Union, Dict, Any, Iterator

from .loader import Loader
from .dataset_store import DatasetStore
from ingestify.domain.models.ingestion.ingestion_plan import IngestionPlan
from ingestify.domain.models import Dataset

logger = logging.getLogger(__name__)


class IngestionEngine:
    def __init__(self, store: DatasetStore):

        # Note: disconnect event from loading. Event should only be used for
        #       metadata and 'loaded_files' for the actual data.
        self.store = store
        self.loader = Loader(self.store)

    def add_ingestion_plan(self, ingestion_plan: IngestionPlan):
        self.loader.add_ingestion_plan(ingestion_plan)

    def load(
        self,
        dry_run: bool = False,
        provider: Optional[str] = None,
        source: Optional[str] = None,
        dataset_type: Optional[str] = None,
        **selector_filters,
    ):
        self.loader.collect_and_run(
            dry_run=dry_run,
            provider=provider,
            source=source,
            dataset_type=dataset_type,
            **selector_filters,
        )

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
                            f"    {dataset.identifier}\t{dataset.dataset_id}\t{dataset.name} / {dataset.state}"
                        )
            # print(dataset.dataset_id)

    def destroy_dataset(
        self, dataset_id: Optional[str] = None, **selector
    ) -> List[str]:
        dataset_collection = self.store.iter_dataset_collection_batches(
            dataset_id=dataset_id,
            **selector,
        )
        dataset_ids = []
        for dataset in dataset_collection:
            self.store.destroy_dataset(dataset)
            dataset_ids.append(dataset.dataset_id)
        return dataset_ids

    def iter_datasets(
        self,
        auto_ingest: Union[bool, Dict[str, Any]] = False,
        dataset_type: Optional[str] = None,
        provider: Optional[str] = None,
        dataset_id: Optional[str] = None,
        batch_size: int = 1000,
        yield_dataset_collection: bool = False,
        dataset_state: Optional[Any] = None,
        **selector_filters,
    ) -> Iterator[Dataset]:
        """
        Iterate over datasets with optional auto-ingestion.

        This method combines dataset discovery, ingestion, and retrieval in a single
        streaming interface. When auto_ingest=True, it will first run the ingestion
        pipeline to discover and ingest matching datasets before yielding results.

        Examples:
            # Basic iteration over existing datasets
            for dataset in engine.iter_datasets(provider="statsbomb"):
                process(dataset)

            # Auto-ingest new data matching criteria before iteration
            for dataset in engine.iter_datasets(
                auto_ingest=True,
                provider="statsbomb",
                competition_id=11
            ):
                process(dataset)  # Includes newly ingested datasets

        Args:
            auto_ingest: Enable auto-ingestion before yielding datasets.
                        Can be True/False or dict with config options.
            dataset_type: Filter by dataset type (e.g., "match", "competition")
            provider: Filter by data provider (e.g., "statsbomb", "wyscout")
            dataset_id: Filter by specific dataset ID
            batch_size: Number of datasets to fetch per batch for pagination
            yield_dataset_collection: If True, yield DatasetCollection objects instead of individual datasets
            dataset_state: Filter by dataset state (e.g., "COMPLETE", "PARTIAL")
            **selector_filters: Additional selector criteria (competition_id, season_id, match_id, etc.)

        Yields:
            Dataset objects matching the specified criteria. If auto_ingest=True,
            includes both existing datasets and newly ingested ones.

        Note:
            Auto-ingestion will only discover datasets that match configured
            IngestionPlans. Requests outside the scope of existing plans will
            not trigger ingestion.
        """
        # Parse auto_ingest config
        if isinstance(auto_ingest, dict):
            auto_ingest_enabled = auto_ingest.get("enabled", True)
            auto_ingest_config = auto_ingest
        else:
            auto_ingest_enabled = bool(auto_ingest)
            auto_ingest_config = {}

        # Run auto-ingestion if enabled
        if auto_ingest_enabled:
            self.load(provider=provider, dataset_type=dataset_type, **selector_filters)

        # Always yield from store (existing + any newly ingested)
        yield from self.store.iter_dataset_collection_batches(
            dataset_type=dataset_type,
            provider=provider,
            dataset_id=dataset_id,
            batch_size=batch_size,
            yield_dataset_collection=yield_dataset_collection,
            dataset_state=dataset_state,
            **selector_filters,
        )

    def load_with_kloppy(self, dataset: Dataset, **kwargs):
        """
        Load a dataset using kloppy.

        Args:
            dataset: The dataset to load
            **kwargs: Additional arguments passed to kloppy's load function

        Returns:
            Kloppy dataset object
        """
        return self.store.load_with_kloppy(dataset, **kwargs)
