import importlib
import logging
import os
import sys
from itertools import product

from pyaml_env import parse_config

from ingestify.application.dataset_store import DatasetStore
from ingestify.application.ingestion_engine import IngestionEngine
from ingestify.domain.models import (
    dataset_repository_factory,
    file_repository_factory,
)

logger = logging.getLogger(__name__)


def _product_selectors(selector_args):
    selector_args_ = {
        k: v if isinstance(v, list) else [v] for k, v in selector_args.items()
    }
    keys, values = zip(*selector_args_.items())
    for bundle in product(*values):
        yield dict(zip(keys, bundle))


def import_cls(name):
    components = name.split('.')
    mod = importlib.import_module(components[0])
    for comp in components[1:]:
        mod = getattr(mod, comp)
    return mod


def get_dataset_store_by_urls(dataset_url: str, file_url: str) -> DatasetStore:
    """
    Initialize a DatasetStore by a DatasetRepository and a FileRepository
    """
    file_repository = file_repository_factory.build_if_supports(url=file_url)
    dataset_repository = dataset_repository_factory.build_if_supports(
        url=dataset_url
    )
    return DatasetStore(
        dataset_repository=dataset_repository,
        file_repository=file_repository
    )


def get_datastore(config_file) -> DatasetStore:
    config = parse_config(config_file)
    return get_dataset_store_by_urls(
        dataset_url=config["main"]["dataset_url"],
        file_url=config["main"]["file_url"],
    )


def get_remote_datastore(url: str, **kwargs) -> DatasetStore:
    return get_dataset_store_by_urls(
        dataset_url=url,
        file_url=url
    )


def get_engine(config_file) -> IngestionEngine:
    config = parse_config(config_file)

    logger.info("Initializing sources")
    sources = {}
    sys.path.append(os.path.dirname(config_file))
    for name, source in config["sources"].items():
        source_cls = import_cls(source["type"])
        sources[name] = source_cls(**source.get("configuration", {}))

    logger.info("Initializing IngestionEngine")
    store = get_dataset_store_by_urls(
        dataset_url=config["main"]["dataset_url"],
        file_url=config["main"]["file_url"],
    )
    ingestion_engine = IngestionEngine(
        store=store,
        sources=sources,
    )

    logger.info("Determining tasks")

    for job in config["extract_jobs"]:
        for selector_args in job["selectors"]:
            for selector in _product_selectors(selector_args):
                ingestion_engine.add_selector(job["source"], selector)

    return ingestion_engine
