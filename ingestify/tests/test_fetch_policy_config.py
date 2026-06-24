from datetime import datetime, timedelta, timezone

import pytest
import yaml

from ingestify import DatasetResource, FetchPolicy, Source
from ingestify.domain import DataSpecVersionCollection, DraftFile, Selector
from ingestify.domain.models.ingestion.ingestion_plan import IngestionPlan
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


def test_get_engine_resolves_plan_fetch_policy_from_config(tmp_path):
    # A plan's fetch_policy is resolved through build_fetch_policy; an invalid
    # one surfaces through get_engine, proving the plan config is consulted.
    config = {
        "main": {
            "metadata_url": f"sqlite:///{tmp_path / 'main.db'}",
            "file_url": f"file://{tmp_path / 'data'}",
            "default_bucket": "main",
        },
        "ingestion_plans": [
            {
                "source": "unused",
                "dataset_type": "match",
                "fetch_policy": ["not-a-class"],
            }
        ],
    }
    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.dump(config))

    with pytest.raises(ConfigurationError):
        get_engine(str(config_path))


# --- End-to-end: a configured policy reading fetch_policy_config actually
#     controls whether a dataset is re-fetched during a real ingestion run. ---

# Monotonic so the file's last_modified is strictly newer on the second run
# (otherwise the engine's fast-skip would short-circuit before the policy runs).
_clock = {"t": datetime(2024, 1, 1, tzinfo=timezone.utc)}
_load_calls = {"n": 0}


def _counting_loader(file_resource, current_file, **kwargs):
    _load_calls["n"] += 1
    # Unique content per fetch, so an actual re-fetch yields a new revision.
    # Stamp the same controlled clock the resource used, so the stored file's
    # modified_at advances in step (and the fast-skip pre-check sees "newer").
    return DraftFile.from_input(
        f"data-{_load_calls['n']}", data_feed_key="f1", modified_at=_clock["t"]
    )


class _ConfigDrivenSource(Source):
    """Yields two datasets whose fetch_policy_config asks for opposite behaviour."""

    provider = "test_provider"

    def find_datasets(
        self, dataset_type, data_spec_versions, dataset_collection_metadata, **kwargs
    ):
        _clock["t"] += timedelta(days=1)
        last_modified = _clock["t"]
        for item_id, refetch in [(0, True), (1, False)]:
            resource = DatasetResource(
                dataset_resource_id={"item_id": item_id},
                provider=self.provider,
                dataset_type="test",
                name=f"item-{item_id}",
                fetch_policy_config={"refetch": refetch},
            )
            resource.add_file(
                last_modified=last_modified,
                data_feed_key="f1",
                data_spec_version="v1",
                file_loader=_counting_loader,
            )
            yield resource


class ConfigDrivenFetchPolicy(FetchPolicy):
    """Ignores last_modified entirely; obeys fetch_policy_config['refetch']."""

    def should_fetch(self, dataset_resource) -> bool:
        return True

    def should_refetch(self, dataset, dataset_resource) -> bool:
        return bool(dataset_resource.fetch_policy_config.get("refetch", False))


def _add_plan(engine, policy):
    dsv = DataSpecVersionCollection.from_dict({"default": {"v1"}})
    engine.add_ingestion_plan(
        IngestionPlan(
            source=_ConfigDrivenSource("s"),
            fetch_policy=policy,
            dataset_type="test",
            selectors=[Selector.build({}, data_spec_versions=dsv)],
            data_spec_versions=dsv,
        )
    )


def test_fetch_policy_config_controls_refetch(engine):
    _add_plan(engine, ConfigDrivenFetchPolicy())

    engine.run()  # initial ingest: both datasets created (one revision each)
    engine.run()  # re-run: the policy decides per dataset via fetch_policy_config

    datasets = {
        d.name: d
        for d in engine.store.get_dataset_collection(
            provider="test_provider", dataset_type="test"
        )
    }

    # refetch=True -> a second revision is added on the re-run.
    assert len(datasets["item-0"].revisions) == 2
    # refetch=False -> stays at the single initial revision, even though the
    # file's last_modified is newer (proves the policy overrides last_modified).
    assert len(datasets["item-1"].revisions) == 1
