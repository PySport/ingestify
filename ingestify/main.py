import importlib
import logging
import os
import sys
from itertools import product

from pyaml_env import parse_config

from ingestify.application.ingestion_engine import IngestionEngine

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


def get_engine(config_file) -> IngestionEngine:
    config = parse_config(config_file)

    logger.info("Initializing sources")
    sources = {}
    sys.path.append(os.path.dirname(config_file))
    for name, source in config["sources"].items():
        source_cls = import_cls(source["type"])
        sources[name] = source_cls(**source.get("configuration", {}))

    logger.info("Initializing IngestionEngine")
    ingestion_engine = IngestionEngine(
        dataset_url=config["main"]["dataset_url"],
        file_url=config["main"]["file_url"],
        sources=sources,
    )

    logger.info("Determining tasks")

    for job in config["extract_jobs"]:
        for selector_args in job["selectors"]:
            for selector in _product_selectors(selector_args):
                ingestion_engine.add_selector(job["source"], selector)

    return ingestion_engine
