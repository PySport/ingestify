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
from ingestify.domain import Selector, FileRepository
from ingestify.domain.models.data_spec_version_collection import (
    DataSpecVersionCollection,
)
from ingestify.domain.models.event import EventBus, Publisher, Subscriber

from ingestify.domain.models.ingestion.ingestion_plan import IngestionPlan
from ingestify.domain.models.fetch_policy import FetchPolicy
from ingestify.domain.services.identifier_key_transformer import IdentifierTransformer
from ingestify.exceptions import ConfigurationError
from ingestify.infra import S3FileRepository, LocalFileRepository
from ingestify.infra.store.dataset.sqlalchemy import SqlAlchemyDatasetRepository
from ingestify.infra.store.dataset.sqlalchemy.repository import (
    SqlAlchemySessionProvider,
)
from ingestify.infra.store.file.dummy_file_repository import DummyFileRepository

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


def build_file_repository(file_url: str, identifier_transformer) -> FileRepository:
    if file_url.startswith("s3://"):
        repository = S3FileRepository(
            url=file_url, identifier_transformer=identifier_transformer
        )
    elif file_url.startswith("file://"):
        repository = LocalFileRepository(
            url=file_url, identifier_transformer=identifier_transformer
        )
    elif file_url.startswith("dummy://"):
        repository = DummyFileRepository(
            url=file_url, identifier_transformer=identifier_transformer
        )
    else:
        raise Exception(f"Cannot find repository to handle file {file_url}")

    return repository


def get_dataset_store_by_urls(
    metadata_url: str, file_url: str, bucket: str, dataset_types
) -> DatasetStore:
    """
    Initialize a DatasetStore by a DatasetRepository and a FileRepository
    """
    if not bucket:
        raise Exception("Bucket is not specified")

    identifier_transformer = IdentifierTransformer()
    for dataset_type in dataset_types:
        for id_key, id_config in dataset_type["identifier_keys"].items():
            identifier_transformer.register_transformation(
                provider=dataset_type["provider"],
                dataset_type=dataset_type["dataset_type"],
                id_key=id_key,
                transformation=id_config["transformation"],
            )

    file_repository = build_file_repository(
        file_url, identifier_transformer=identifier_transformer
    )

    if secrets_manager.supports(metadata_url):
        metadata_url = secrets_manager.load_as_db_url(metadata_url)

    if metadata_url.startswith("postgres://"):
        metadata_url = metadata_url.replace("postgress://", "postgress+")

    sqlalchemy_session_provider = SqlAlchemySessionProvider(metadata_url)

    dataset_repository = SqlAlchemyDatasetRepository(sqlalchemy_session_provider)

    return DatasetStore(
        dataset_repository=dataset_repository,
        file_repository=file_repository,
        bucket=bucket,
    )


def get_datastore(config_file, bucket: Optional[str] = None) -> DatasetStore:
    config = parse_config(config_file, default_value="")

    return get_dataset_store_by_urls(
        metadata_url=config["main"]["metadata_url"],
        file_url=config["main"]["file_url"],
        bucket=bucket or config["main"].get("default_bucket"),
        dataset_types=config.get("dataset_types", []),
    )


def get_remote_datastore(url: str, bucket: str, **kwargs) -> DatasetStore:
    return get_dataset_store_by_urls(metadata_url=url, file_url=url, bucket=bucket)


def get_source_cls(key: str) -> Type[Source]:
    if key.startswith("ingestify."):
        _, type_ = key.split(".", maxsplit=1)
        if type_ == "wyscout":
            from ingestify.infra.source.wyscout import Wyscout

            return Wyscout

        elif type_ == "statsbomb.match":
            from ingestify.infra.source.statsbomb.match import StatsBombMatchAPI

            return StatsBombMatchAPI
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


def get_engine(
    config_file: Optional[str] = None,
    bucket: Optional[str] = None,
    disable_events: bool = False,
    metadata_url: Optional[str] = None,
    file_url: Optional[str] = None,
) -> IngestionEngine:
    sources = {}

    if not config_file:
        if not metadata_url or not file_url:
            raise ValueError(
                f"You must specify metadata_url and file_url in case you don't use a config_file"
            )

        config = {
            "main": {
                "metadata_url": metadata_url,
                "file_url": file_url,
                "default_bucket": bucket or "main",
            }
        }
    elif not config_file:
        raise ValueError("You must specify a config file")
    else:
        config = parse_config(config_file, default_value="")

        logger.info("Initializing sources")
        sys.path.append(os.path.dirname(config_file))
        for name, source_args in config.get("sources", {}).items():
            sources[name] = build_source(name=name, source_args=source_args)

    logger.info("Initializing IngestionEngine")
    store = get_dataset_store_by_urls(
        metadata_url=config["main"]["metadata_url"],
        file_url=config["main"]["file_url"],
        bucket=bucket or config["main"].get("default_bucket"),
        dataset_types=config.get("dataset_types", []),
    )

    # Setup an EventBus and wire some more components
    event_bus = EventBus()
    if not disable_events:
        # When we disable all events we don't register any publishers
        publisher = Publisher()
        for subscriber in config.get("event_subscribers", []):
            cls = get_event_subscriber_cls(subscriber["type"])
            publisher.add_subscriber(cls(store))
        event_bus.register(publisher)
    else:
        logger.info("Disabling all event handlers")

    store.set_event_bus(event_bus)

    ingestion_engine = IngestionEngine(
        store=store,
    )

    logger.info("Adding IngestionPlans...")

    fetch_policy = FetchPolicy()

    # Previous naming
    ingestion_plans = config.get("extract_jobs", [])
    # New naming
    ingestion_plans.extend(config.get("ingestion_plans", []))

    for ingestion_plan in ingestion_plans:
        data_spec_versions = DataSpecVersionCollection.from_dict(
            ingestion_plan.get("data_spec_versions", {"default": {"v1"}})
        )

        if "selectors" in ingestion_plan:
            selectors = [
                Selector.build(selector, data_spec_versions=data_spec_versions)
                for selector_args in ingestion_plan["selectors"]
                for selector in _product_selectors(selector_args)
            ]
        else:
            # Add a single empty selector. This won't match anything
            # but makes it easier later one where we loop over selectors.
            selectors = [Selector.build({}, data_spec_versions=data_spec_versions)]

        ingestion_plan_ = IngestionPlan(
            source=sources[ingestion_plan["source"]],
            dataset_type=ingestion_plan["dataset_type"],
            selectors=selectors,
            fetch_policy=fetch_policy,
            data_spec_versions=data_spec_versions,
        )
        ingestion_engine.add_ingestion_plan(ingestion_plan_)

    return ingestion_engine
