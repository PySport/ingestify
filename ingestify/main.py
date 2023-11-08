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
from ingestify.domain import Selector
from ingestify.domain.models import (
    dataset_repository_factory,
    file_repository_factory,
)
from ingestify.domain.models.event import EventBus, EventPublisher, Subscriber
from ingestify.domain.models.extract_job import ExtractJob
from ingestify.domain.models.fetch_policy import FetchPolicy

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
    mod = importlib.import_module(".".join(components[:-1]))
    return getattr(mod, components[-1])


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
    config = parse_config(config_file, default_value="")

    return get_dataset_store_by_urls(
        dataset_url=config["main"]["dataset_url"],
        file_url=config["main"]["file_url"],
        bucket=bucket or config["main"].get("default_bucket"),
    )


def get_remote_datastore(url: str, bucket: str, **kwargs) -> DatasetStore:
    return get_dataset_store_by_urls(dataset_url=url, file_url=url, bucket=bucket)


def get_source_cls(key: str) -> Type[Source]:
    if key.startswith("ingestify."):
        _, type_ = key.split(".")
        if type_ == "wyscout":
            from ingestify.infra.source.wyscout import Wyscout

            return Wyscout

        elif type_ == "statsbomb_github":
            from ingestify.infra.source.statsbomb_github import StatsbombGithub

            return StatsbombGithub
        else:
            raise Exception(f"Unknown source type 'ingestify.{type_}'")
    else:
        return import_cls(key)


def get_event_subscriber_cls(key: str) -> Type[Subscriber]:
    return import_cls(key)


def get_engine(config_file, bucket: Optional[str] = None) -> IngestionEngine:
    config = parse_config(config_file, default_value="")

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
    # class Dispatcher:
    #     def dispatch(self, event):
    #         print(event)
    #
    # event_bus = EventBus()
    # event_bus.register(Dispatcher())
    # store.set_event_bus(event_bus=event_bus)
    event_bus = EventBus()
    # event_repository = EventRepository()
    # event_bus.register(EventWriter(event_repository))
    # event_bus.register(EventLogger())

    event_publisher = EventPublisher()
    for subscriber in config.get("event_subscribers", []):
        cls = get_event_subscriber_cls(subscriber["type"])
        event_publisher.add_subscriber(cls(store))

    event_bus.register(event_publisher)
    store.set_event_bus(event_bus)

    ingestion_engine = IngestionEngine(
        store=store,
    )

    logger.info("Determining tasks...")

    fetch_policy = FetchPolicy()

    for job in config["extract_jobs"]:
        import_job = ExtractJob(
            source=sources[job["source"]],
            dataset_type=job.get("dataset_type"),
            selectors=[
                Selector(**selector)
                for selector_args in job["selectors"]
                for selector in _product_selectors(selector_args)
            ],
            fetch_policy=fetch_policy,
        )
        ingestion_engine.add_extract_job(import_job)

    return ingestion_engine
