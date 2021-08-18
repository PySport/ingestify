import logging
import sys
from importlib.machinery import SourceFileLoader
from itertools import product

import click
from dotenv import find_dotenv, load_dotenv
from pyaml_env import parse_config

from ingestify.application.ingestion_engine import IngestionEngine
from ingestify.domain.models import Source, source_factory

logger = logging.getLogger(__name__)


def _load_sources(path):
    """
    Load sources from a python file pointed at by `path`. This file should only contain
    `Source` classes.

    Since the `Source` base class is wired with a registry all sources defined in the file
    are registered by default and therefore we don't need to return the result of the load_module()
    """
    logger.info("Loading custom sources")
    module = SourceFileLoader("sources", path).load_module()
    # TODO: fix why this is not working...
    # for k, v in module.__dict__.items():
    #     if isinstance(v, Source):
    #         logger.info(f"Found source '{k}'")


def _product_selectors(selector_args):
    selector_args_ = {
        k: v if isinstance(v, list) else [v] for k, v in selector_args.items()
    }
    keys, values = zip(*selector_args_.items())
    for bundle in product(*values):
        yield dict(zip(keys, bundle))


@click.group()
def cli():
    pass


@cli.command()
@click.option(
    "--sources",
    "sources_file",
    help="Module containing extra sources to load",
    type=click.Path(exists=True),
)
@click.option(
    "--config",
    "config_file",
    required=True,
    help="Yaml config file",
    type=click.Path(exists=True),
)
def run(sources_file: str, config_file: str):
    config = parse_config(config_file)

    if sources_file:
        _load_sources(sources_file)

    logger.info("Initializing sources")

    sources = {
        name: source_factory.build(source["type"], **source.get("configuration", {}))
        for name, source in config["sources"].items()
    }

    logger.info("Initializing IngestionEngine")
    ingestion_engine = IngestionEngine(
        dataset_url=config["main"]["dataset_url"],
        file_url=config["main"]["file_url"],
        sources=sources
    )


    logger.info("Determining tasks")

    for job in config["extract_jobs"]:
        for selector_args in job["selectors"]:
            for selector in _product_selectors(selector_args):
                ingestion_engine.add_selector(job["source"], selector)

    ingestion_engine.load()

    logger.info("Done")


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        stream=sys.stdout,
    )

    path = find_dotenv(usecwd=True)
    load_dotenv(path)

    cli(obj={})


if __name__ == "__main__":
    main()
# if __name__ == "__main__":
# importlib.import_module
