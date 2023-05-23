import logging
import sys
from pathlib import Path
from typing import Optional

import click
import jinja2
from dotenv import find_dotenv, load_dotenv
from ingestify.main import get_engine

from ingestify import __version__

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
    required=False,
    help="Yaml config file",
    type=click.Path(exists=True),
    default="config.yaml",
)
@click.option(
    "--bucket",
    "bucket",
    required=False,
    help="bucket",
    type=str,
)
def run(config_file: str, bucket: Optional[str]):
    engine = get_engine(config_file, bucket)
    engine.load()

    logger.info("Done")


@cli.command()
@click.option(
    "--template",
    "template",
    required=True,
    help="Template",
    type=click.Choice(["wyscout", "statsbomb_github"]),
)
@click.argument("project_name")
def init(template: str, project_name: str):
    directory = Path(project_name)
    if directory.exists():
        logger.warning(f"Directory '{directory}' already exists")
        return sys.exit(1)

    if template == "wyscout":
        template_dir = Path(__file__).parent / "static/templates/wyscout"
    elif template == "statsbomb_github":
        template_dir = Path(__file__).parent / "static/templates/statsbomb_github"
    else:
        raise Exception(f"Template {template} not found")

    directory.mkdir(parents=True)

    for file in template_dir.glob("*"):
        filename = file.name
        if file.is_file():
            data = file.open("r").read()

            if filename.endswith(".jinja2"):
                raw_input = jinja2.Template(data)
                data = raw_input.render(ingestify_version=__version__)
                filename = filename.rstrip(".jinja2")

            with open(directory / filename, "w") as fp:
                fp.write(data)
        elif file.is_dir():
            (directory / filename).mkdir()

    logger.info(f"Initialized project at `{directory}` with template `{template}`")


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
