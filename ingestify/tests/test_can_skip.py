"""Tests for the ``can_skip`` fetch-policy hook.

``can_skip`` is a cheap, one-sided pre-check (à la a bloom filter): it may return
True *only* when the policy is certain, from a lightweight per-dataset summary,
that the existing dataset is up-to-date. In that case the engine skips the
dataset without loading the full ``Dataset`` graph and without reaching the
authoritative ``should_refetch``. Returning False means "unknown" — fall through
to ``get_dataset_collection`` + ``should_refetch``. The base ``FetchPolicy``
returns False, so the hook is purely additive and non-breaking.

Two observables:
- the file loader (does the fetch task run at all), and
- ``should_refetch`` (is the authoritative, full-dataset path reached).
A ``can_skip`` that works avoids both on an up-to-date dataset. (Revision count
can't tell a skip apart from a refetch-that-got-squashed-to-ignored, so it isn't
used here.)
"""
from ingestify import Source, DatasetResource
from ingestify.domain import DataSpecVersionCollection, DraftFile, Selector
from ingestify.domain.models.fetch_policy import FetchPolicy
from ingestify.domain.models.ingestion.ingestion_plan import IngestionPlan
from ingestify.utils import utcnow


class CountingSource(Source):
    """Yields 5 datasets; counts how often their file loader is invoked."""

    provider = "test_provider"

    def __init__(self, name):
        super().__init__(name)
        self.load_calls = 0

    def find_datasets(
        self, dataset_type, data_spec_versions, dataset_collection_metadata, **kwargs
    ):
        def loader(file_resource, current_file, **kwargs):
            self.load_calls += 1
            return DraftFile.from_input("data", data_feed_key="f1")

        for i in range(5):
            r = DatasetResource(
                dataset_resource_id={"item_id": i},
                provider=self.provider,
                dataset_type="test",
                name=f"item-{i}",
            )
            r.add_file(
                last_modified=utcnow(),
                data_feed_key="f1",
                data_spec_version="v1",
                file_loader=loader,
            )
            yield r


class CountingPolicy(FetchPolicy):
    """Base-behaviour policy that records how often should_refetch is reached."""

    def __init__(self):
        super().__init__()
        self.can_skip_calls = 0
        self.should_refetch_calls = 0

    def should_refetch(self, dataset, dataset_resource) -> bool:
        self.should_refetch_calls += 1
        return super().should_refetch(dataset, dataset_resource)


class SkipExistingPolicy(CountingPolicy):
    """Always certain an existing dataset is up-to-date."""

    def can_skip(self, summary, dataset_resource) -> bool:
        self.can_skip_calls += 1
        return True


def _add_plan(engine, source, fetch_policy):
    dsv = DataSpecVersionCollection.from_dict({"default": {"v1"}})
    engine.add_ingestion_plan(
        IngestionPlan(
            source=source,
            fetch_policy=fetch_policy,
            dataset_type="test",
            selectors=[Selector.build({}, data_spec_versions=dsv)],
            data_spec_versions=dsv,
        )
    )


def test_can_skip_true_avoids_fetch_and_should_refetch(engine):
    """can_skip=True short-circuits before the full-dataset path: on the second
    run the file loader never runs and should_refetch is never reached."""
    source = CountingSource("s")
    policy = SkipExistingPolicy()
    _add_plan(engine, source, policy)

    engine.run()  # first run: nothing exists yet -> 5 creates
    engine.run()  # second run: can_skip -> True -> skipped

    assert source.load_calls == 5, "can_skip=True must skip the fetch on the second run"
    assert (
        policy.should_refetch_calls == 0
    ), "can_skip=True must short-circuit before should_refetch"
    assert (
        policy.can_skip_calls == 5
    ), "can_skip must be consulted once per existing dataset"


def test_base_fetch_policy_still_fetches_can_skip_false(engine):
    """Regression guard: base FetchPolicy.can_skip is False (default), so the
    second run still reaches should_refetch and runs the fetch — unchanged."""
    source = CountingSource("s")
    policy = CountingPolicy()
    _add_plan(engine, source, policy)

    engine.run()
    engine.run()

    assert source.load_calls == 10, "base policy must still fetch (can_skip=False)"
    assert (
        policy.should_refetch_calls == 5
    ), "base policy must still reach should_refetch on the second run"
