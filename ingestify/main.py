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
    metadata_url: str,
    file_url: str,
    bucket: str,
    dataset_types,
    metadata_options: dict = None,
) -> DatasetStore:
    """
    Initialize a DatasetStore by a DatasetRepository and a FileRepository

    Args:
        metadata_url: Database connection URL
        file_url: File storage URL
        bucket: Bucket name
        dataset_types: Dataset type configurations
        metadata_options: Optional dict with metadata store options (e.g., table_prefix)
    """
    if not bucket:
        raise Exception("Bucket is not specified")

    if metadata_options is None:
        metadata_options = {}

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

    # Extract table_prefix from metadata_options
    table_prefix = metadata_options.get("table_prefix", "")

    sqlalchemy_session_provider = SqlAlchemySessionProvider(
        metadata_url, table_prefix=table_prefix
    )

    dataset_repository = SqlAlchemyDatasetRepository(sqlalchemy_session_provider)

    return DatasetStore(
        dataset_repository=dataset_repository,
        file_repository=file_repository,
        bucket=bucket,
    )


def get_datastore(config_file, bucket: Optional[str] = None) -> DatasetStore:
    config = parse_config(config_file, default_value="")

    # Extract metadata_options if provided
    main_config = config["main"]
    metadata_options = main_config.get("metadata_options", {})

    return get_dataset_store_by_urls(
        metadata_url=main_config["metadata_url"],
        file_url=main_config["file_url"],
        bucket=bucket or main_config.get("default_bucket"),
        dataset_types=config.get("dataset_types", []),
        metadata_options=metadata_options,
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

    # Extract metadata_options if provided
    metadata_options = config["main"].get("metadata_options", {})

    store = get_dataset_store_by_urls(
        metadata_url=config["main"]["metadata_url"],
        file_url=config["main"]["file_url"],
        bucket=bucket or config["main"].get("default_bucket"),
        dataset_types=config.get("dataset_types", []),
        metadata_options=metadata_options,
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


def get_dev_engine(
    source: Source,
    dataset_type: str,
    data_spec_versions: dict,
    ephemeral: bool = True,
    configure_logging: bool = True,
    dev_dir: Optional[str] = None,
) -> IngestionEngine:
    """
    Quick development helper - creates an engine with minimal setup.

    Args:
        source: The source to test
        dataset_type: Dataset type to ingest
        data_spec_versions: Dict like {"hops": "v1"}
        ephemeral: If True, uses temp dir that gets cleaned. If False, uses persistent /tmp storage.
        configure_logging: If True, configures basic logging (default: True)
        dev_dir: Optional custom directory for data storage (overrides ephemeral)

    Returns:
        IngestionEngine configured for development

    Example:
        >>> source = MySource(name="test", ...)
        >>> engine = get_dev_engine(source, "hops", {"hops": "v1"})
        >>> engine.run()
        >>>
        >>> # Access the datasets
        >>> datasets = engine.store.get_dataset_collection()
        >>> print(f"Ingested {len(datasets)} datasets")
    """
    import tempfile
    from pathlib import Path

    if configure_logging:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        )

    if dev_dir:
        # Use provided directory
        dev_dir = Path(dev_dir)
    elif ephemeral:
        # Use temp directory that will be cleaned up
        import uuid

        dev_dir = Path(tempfile.gettempdir()) / f"ingestify-dev-{uuid.uuid4().hex[:8]}"
    else:
        # Use persistent directory
        dev_dir = Path(tempfile.gettempdir()) / "ingestify-dev"

    dev_dir.mkdir(parents=True, exist_ok=True)
    metadata_url = f"sqlite:///{dev_dir / 'database.db'}"
    file_url = f"file://{dev_dir}"

    logger.info(f"Dev mode: storing data in {dev_dir}")

    engine = get_engine(
        metadata_url=metadata_url,
        file_url=file_url,
        bucket="main",
        disable_events=True,
    )

    data_spec_versions_obj = DataSpecVersionCollection.from_dict(data_spec_versions)

    engine.add_ingestion_plan(
        IngestionPlan(
            source=source,
            dataset_type=dataset_type,
            selectors=[Selector.build({}, data_spec_versions=data_spec_versions_obj)],
            fetch_policy=FetchPolicy(),
            data_spec_versions=data_spec_versions_obj,
        )
    )

    return engine


def debug_source(
    source: Source,
    *,
    dataset_type: str,
    data_spec_versions: dict,
    ephemeral: bool = True,
    configure_logging: bool = True,
    dev_dir: Optional[str] = None,
    **kwargs,
) -> IngestionEngine:
    """
    Debug helper - creates a dev engine, runs ingestion, and shows results.

    This is a convenience wrapper around get_dev_engine() that does everything:
    creates the engine, runs ingestion, and displays results.

    Args:
        source: The source to debug
        dataset_type: Dataset type (e.g., "match")
        data_spec_versions: Dict like {"match": "v1"} - explicit, no defaults!
        ephemeral: If True, uses temp dir. If False, uses persistent /tmp storage.
        configure_logging: If True, configures basic logging (default: True)
        dev_dir: Optional custom directory for data storage (overrides ephemeral)
        **kwargs: Selector arguments. For sources with discover_selectors(), these
                  filter discovered selectors. Otherwise passed to find_datasets().

    Returns:
        IngestionEngine: The engine used for ingestion (for further inspection)

    Example:
        >>> # Simple source without discover_selectors
        >>> source = StatsBombHOPSS3(name="test", s3_bucket="my-bucket", s3_prefix="HOPS")
        >>> engine = debug_source(source, dataset_type="hops", data_spec_versions={"hops": "v1"})

        >>> # Source with discover_selectors - discovers all competitions
        >>> source = StatsBombMatchAPI(name="test", ...)
        >>> engine = debug_source(
        ...     source,
        ...     dataset_type="match",
        ...     data_spec_versions={"match": "v6"}
        ... )

        >>> # Filter discovered selectors
        >>> engine = debug_source(
        ...     source,
        ...     dataset_type="match",
        ...     data_spec_versions={"match": "v6"},
        ...     competition_id=46  # Filters to specific competition
        ... )
    """
    logger.info(f"Debug mode for source: {source.name}")

    engine = get_dev_engine(
        source=source,
        dataset_type=dataset_type,
        data_spec_versions=data_spec_versions,
        ephemeral=ephemeral,
        configure_logging=configure_logging,
        dev_dir=dev_dir,
    )

    # Run ingestion
    # Empty selector {} automatically triggers discover_selectors() if available
    # kwargs filter discovered selectors or are passed to find_datasets()
    engine.run(**kwargs)

    # Show results
    datasets = engine.store.get_dataset_collection()
    logger.info("=" * 60)
    logger.info(f"✓ Ingestion complete: {len(datasets)} dataset(s)")
    logger.info("=" * 60)

    return engine
