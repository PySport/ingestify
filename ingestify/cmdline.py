import logging
import sys
from importlib.machinery import SourceFileLoader

import click
from dotenv import find_dotenv, load_dotenv
from ingestify.main import get_engine

logger = logging.getLogger(__name__)
#
#
# def _load_sources(path):
#     """
#     Load sources from a python file pointed at by `path`. This file should only contain
#     `Source` classes.
#
#     Since the `Source` base class is wired with a registry all sources defined in the file
#     are registered by default and therefore we don't need to return the result of the load_module()
#     """
#     logger.info("Loading custom sources")
#     module = SourceFileLoader("sources", path).load_module()
#     # TODO: fix why this is not working...
#     # for k, v in module.__dict__.items():
#     #     if isinstance(v, Source):
#     #         logger.info(f"Found source '{k}'")


@click.group()
def cli():
    pass


@cli.command()
@click.option(
    "--config",
    "config_file",
    required=True,
    help="Yaml config file",
    type=click.Path(exists=True),
)
def run(config_file: str):
    engine = get_engine(config_file)
    engine.load()

    logger.info("Done")

#
# @cli.command("list")
# @click.option(
#     "--config",
#     "config_file",
#     required=True,
#     help="Yaml config file",
#     type=click.Path(exists=True),
# )
# def list_datastore(config_file: str):
#     engine = get_engine(config_file)
#
#     dataset_collection = engine.get_dataset_collection(
#         where="""
#         metadata->>'$.home_team.home_team_name' == 'Barcelona' OR
#         metadata->>'$.away_team.away_team_name' == 'Barcelona'
#         """
#     )
#     for dataset in dataset_collection:
#         kloppy_dataset = engine.load_with_kloppy(dataset)
#         goals = kloppy_dataset.filter("shot.goal")
#         for goal in goals:
#             print(goal)
#




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
