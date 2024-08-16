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
from ingestify.application.secrets_manager import SecretsManager
from ingestify.domain import Selector
from ingestify.domain.models import (
    dataset_repository_factory,
    file_repository_factory,
)
from ingestify.domain.models.data_spec_version_collection import (
    DataSpecVersionCollection,
)
from ingestify.domain.models.event import EventBus, Publisher, Subscriber

from ingestify.domain.models.extract_job import ExtractJob
from ingestify.domain.models.fetch_policy import FetchPolicy
from ingestify.exceptions import ConfigurationError

logger = logging.getLogger(__name__)

secrets_manager = SecretsManager()


def _product_selectors(selector_args):
    if not selector_args:
        # Empty selector passed. This is a special case when
        # a Source doesn't have discover_selectors but also doesn't require
        # selectors
        yield dict()
        return

    if isinstance(selector_args, str):
        if selector_args == "*":
            yield lambda dict_selector: True
        else:
            yield lambda dict_selector: eval(selector_args, {}, dict_selector)
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

    if secrets_manager.supports(dataset_url):
        dataset_url = secrets_manager.load_as_db_url(dataset_url)

    if dataset_url.startswith("postgres://"):
        dataset_url = dataset_url.replace("postgress://", "postgress+")

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


def build_source(name, source_args):
    source_cls = get_source_cls(source_args["type"])
    raw_configuration = source_args.get("configuration", {})
    configuration = {}
    if isinstance(raw_configuration, list):
        # This normally means the data needs to be loaded from somewhere else
        for item in raw_configuration:
            if isinstance(item, dict):
                configuration.update(item)
            elif secrets_manager.supports(item):
                item = secrets_manager.load_as_dict(item)
                configuration.update(item)
            else:
                raise ConfigurationError(
                    f"Don't know how to use source configuration '{item}'"
                )
    elif isinstance(raw_configuration, str):
        configuration = secrets_manager.load_as_dict(raw_configuration)
    else:
        configuration = raw_configuration

    return source_cls(name=name, **configuration)


def get_event_subscriber_cls(key: str) -> Type[Subscriber]:
    return import_cls(key)


def get_engine(config_file, bucket: Optional[str] = None) -> IngestionEngine:
    config = parse_config(config_file, default_value="")

    logger.info("Initializing sources")
    sources = {}
    sys.path.append(os.path.dirname(config_file))
    for name, source_args in config["sources"].items():
        sources[name] = build_source(name=name, source_args=source_args)

    logger.info("Initializing IngestionEngine")
    store = get_dataset_store_by_urls(
        dataset_url=config["main"]["dataset_url"],
        file_url=config["main"]["file_url"],
        bucket=bucket or config["main"].get("default_bucket"),
    )

    # Setup an EventBus and wire some more components
    event_bus = EventBus()
    publisher = Publisher()
    for subscriber in config.get("event_subscribers", []):
        cls = get_event_subscriber_cls(subscriber["type"])
        publisher.add_subscriber(cls(store))
    event_bus.register(publisher)
    store.set_event_bus(event_bus)

    ingestion_engine = IngestionEngine(
        store=store,
    )

    logger.info("Determining tasks...")

    fetch_policy = FetchPolicy()

    for job in config["extract_jobs"]:
        data_spec_versions = DataSpecVersionCollection.from_dict(
            job.get("data_spec_versions", {"default": {"v1"}})
        )

        if "selectors" in job:
            selectors = [
                Selector.build(selector, data_spec_versions=data_spec_versions)
                for selector_args in job["selectors"]
                for selector in _product_selectors(selector_args)
            ]
        else:
            # Add a single empty selector. This won't match anything
            # but makes it easier later one where we loop over selectors.
            selectors = [Selector.build({}, data_spec_versions=data_spec_versions)]

        import_job = ExtractJob(
            source=sources[job["source"]],
            dataset_type=job["dataset_type"],
            selectors=selectors,
            fetch_policy=fetch_policy,
            data_spec_versions=data_spec_versions,
        )
        ingestion_engine.add_extract_job(import_job)

    return ingestion_engine
