import logging
import sys
from importlib.machinery import SourceFileLoader
from itertools import product

import click
from dotenv import load_dotenv
from pyaml_env import parse_config

from ingestify.application.ingestion_engine import IngestionEngine
from ingestify.domain.models.source import source_factory

logger = logging.getLogger(__name__)


def _load_sources(path):
    """
    Load sources from a python file pointed at by `path`. This file should only contain
    `Source` classes.

    Since the `Source` base class is wired with a registry all sources defined in the file
    are registered by default and therefore we don't need to return the result of the load_module()
    """
    SourceFileLoader("sources", path).load_module()


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
    if sources_file:
        _load_sources(sources_file)

    config = parse_config(config_file)

    ingestion_engine = IngestionEngine(
        dataset_url=config["main"]["dataset_url"], file_url=config["main"]["file_url"]
    )

    sources = {
        name: source_factory.build(source["type"], **source.get("configuration", {}))
        for name, source in config["sources"].items()
    }

    for task in config["ingestion_tasks"]:
        source = sources[task["source"]]
        for selector_args in task["selectors"]:
            for selector in _product_selectors(selector_args):
                ingestion_engine.add_selector(source, **selector)

    ingestion_engine.collect_and_run()


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        stream=sys.stdout,
    )

    load_dotenv()

    cli(obj={})


if __name__ == "__main__":
    main()
# if __name__ == "__main__":
# importlib.import_module
