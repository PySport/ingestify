import textwrap

import pytest

from ingestify import FetchPolicy
from ingestify.exceptions import ConfigurationError
from ingestify.main import build_fetch_policy, get_engine


class CadenceFetchPolicy(FetchPolicy):
    """A custom policy used to verify config-driven construction."""

    def __init__(self, interval_days: int = 30):
        super().__init__()
        self.interval_days = interval_days


_CADENCE_PATH = "ingestify.tests.test_fetch_policy_config.CadenceFetchPolicy"


def test_build_fetch_policy_from_string():
    policy = build_fetch_policy(_CADENCE_PATH)

    assert isinstance(policy, CadenceFetchPolicy)
    assert policy.interval_days == 30


def test_build_fetch_policy_from_mapping_with_configuration():
    policy = build_fetch_policy(
        {"type": _CADENCE_PATH, "configuration": {"interval_days": 15}}
    )

    assert isinstance(policy, CadenceFetchPolicy)
    assert policy.interval_days == 15


def test_build_fetch_policy_from_mapping_without_configuration():
    policy = build_fetch_policy({"type": _CADENCE_PATH})

    assert isinstance(policy, CadenceFetchPolicy)
    assert policy.interval_days == 30


def test_build_fetch_policy_mapping_requires_type():
    with pytest.raises(ConfigurationError):
        build_fetch_policy({"configuration": {"interval_days": 15}})


def test_build_fetch_policy_rejects_unsupported_type():
    with pytest.raises(ConfigurationError):
        build_fetch_policy(["not", "supported"])


def test_get_engine_wires_per_plan_fetch_policy(tmp_path):
    # A user module living outside the `ingestify` namespace — get_engine adds
    # the config dir to sys.path, so `myproj` becomes importable.
    (tmp_path / "myproj.py").write_text(
        textwrap.dedent(
            '''
            from ingestify import Source, FetchPolicy


            class MySource(Source):
                provider = "test"

                def find_datasets(self, *args, **kwargs):
                    return iter([])


            class MyFetchPolicy(FetchPolicy):
                def __init__(self, interval_days=30):
                    super().__init__()
                    self.interval_days = interval_days
            '''
        )
    )
    (tmp_path / "data").mkdir()
    config = tmp_path / "config.yaml"
    config.write_text(
        textwrap.dedent(
            f"""
            main:
              metadata_url: sqlite:///{tmp_path}/main.db
              file_url: file://{tmp_path}/data
              default_bucket: main

            sources:
              my_source:
                type: myproj.MySource

            ingestion_plans:
              - source: my_source
                dataset_type: match
                fetch_policy:
                  type: myproj.MyFetchPolicy
                  configuration:
                    interval_days: 15
              - source: my_source
                dataset_type: lineup
            """
        )
    )

    engine = get_engine(str(config))
    plans = {p.dataset_type: p.fetch_policy for p in engine.loader.ingestion_plans}

    assert len(plans) == 2
    # Plan with an explicit fetch_policy gets the configured class + kwargs.
    assert type(plans["match"]).__name__ == "MyFetchPolicy"
    assert plans["match"].interval_days == 15
    # Plan without one falls back to the built-in default policy.
    assert type(plans["lineup"]) is FetchPolicy
