import importlib
import logging
import os
import sys
from itertools import product
from typing import Optional, Type

from pyaml_env import parse_config

from ingestify import Source
from ingestify.application.dataset_store import DatasetStore
from ingestify.application.ingestion_engine import IngestionEngine
from ingestify.domain.models import (
    dataset_repository_factory,
    file_repository_factory,
)

logger = logging.getLogger(__name__)


def _product_selectors(selector_args):
    if not selector_args:
        # Empty selector passed
        yield dict()
        return

    selector_args_ = {
        k: v if isinstance(v, list) else [v] for k, v in selector_args.items()
    }
    keys, values = zip(*selector_args_.items())
    for bundle in product(*values):
        yield dict(zip(keys, bundle))


def import_cls(name):
    components = name.split(".")
    mod = importlib.import_module(components[0])
    for comp in components[1:]:
        mod = getattr(mod, comp)
    return mod


def get_dataset_store_by_urls(
    dataset_url: str, file_url: str, bucket: str
) -> DatasetStore:
    """
    Initialize a DatasetStore by a DatasetRepository and a FileRepository
    """
    if not bucket:
        raise Exception("Bucket is not specified")

    file_repository = file_repository_factory.build_if_supports(url=file_url)
    dataset_repository = dataset_repository_factory.build_if_supports(url=dataset_url)
    return DatasetStore(
        dataset_repository=dataset_repository,
        file_repository=file_repository,
        bucket=bucket,
    )


def get_datastore(config_file, bucket: Optional[str] = None) -> DatasetStore:
    config = parse_config(config_file)

    return get_dataset_store_by_urls(
        dataset_url=config["main"]["dataset_url"],
        file_url=config["main"]["file_url"],
        bucket=bucket or config["main"].get("default_bucket"),
    )


def get_remote_datastore(url: str, bucket: str, **kwargs) -> DatasetStore:
    return get_dataset_store_by_urls(dataset_url=url, file_url=url, bucket=bucket)


def get_source_cls(key: str) -> Type[Source]:
    if key.startswith("ingestify."):
        _, type_, dataset_type = key.split(".")
        if type_ == "wyscout":
            from ingestify.infra.source.wyscout import WyscoutEvent, WyscoutPlayer

            if dataset_type == "event":
                return WyscoutEvent
            elif dataset_type == "player":
                return WyscoutPlayer
            else:
                raise Exception(f"Unknown dataset type for Wyscout: '{dataset_type}'")

        elif type_ == "statsbomb_github":
            from ingestify.infra.source.statsbomb_github import StatsbombGithub

            return StatsbombGithub
        else:
            raise Exception(f"Unknown source type 'ingestify.{type_}'")
    else:
        return import_cls(key)


def get_engine(config_file, bucket: Optional[str] = None) -> IngestionEngine:
    config = parse_config(config_file)

    logger.info("Initializing sources")
    sources = {}
    sys.path.append(os.path.dirname(config_file))
    for name, source in config["sources"].items():
        source_cls = get_source_cls(source["type"])
        sources[name] = source_cls(name=name, **source.get("configuration", {}))

    logger.info("Initializing IngestionEngine")
    store = get_dataset_store_by_urls(
        dataset_url=config["main"]["dataset_url"],
        file_url=config["main"]["file_url"],
        bucket=bucket or config["main"].get("default_bucket"),
    )
    ingestion_engine = IngestionEngine(
        store=store,
        sources=sources,
    )

    logger.info("Determining tasks...")

    for job in config["extract_jobs"]:
        for selector_args in job["selectors"]:
            for selector in _product_selectors(selector_args):
                ingestion_engine.add_selector(job["source"], selector)

    return ingestion_engine
