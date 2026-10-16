"""Tests for migrating datasets after a data_spec_version bump.

A file's id is ``<data_feed_key>__<data_spec_version>``, so bumping a feed's
version gives a dataset a file id it has never been stored under.
``Revision.is_changed`` treats an unknown id as changed -- but two last-modified
shortcuts run before it and neither of them can see an id:

1. a whole selector is skipped when the lake is newer than the selector, so
   ``find_datasets`` never runs;
2. surviving that, a per-dataset pre-check drops every dataset whose stored
   timestamp is at or past the reported files'.

Both are deliberate: re-fetching a provider's whole history on a config change
is an expensive surprise, and the fan-out downstream is worse. So the default
stays "skip", and ``run_pre_checks=False`` is how a migration run asks for the
other behaviour. These tests pin both halves -- that the default really does
skip, and that the flag really does reach both shortcuts.

Timestamps are frozen on purpose. A source that stamps its files with something
stable -- a match kickoff, a publication date -- never moves them again once the
event is over, so the file ids are the only evidence a refetch is needed.
"""
from datetime import datetime, timedelta, timezone

from ingestify import DatasetResource, Source
from ingestify.domain import DataSpecVersionCollection, DraftFile, Selector
from ingestify.domain.models.fetch_policy import FetchPolicy
from ingestify.domain.models.ingestion.ingestion_plan import IngestionPlan

# Fixed timestamp so last_modified doesn't change between runs
FIXED_TIME = datetime(2026, 1, 1, tzinfo=timezone.utc)


class VersionedSource(Source):
    """Yields one dataset whose declared files can be changed between runs."""

    provider = "test_provider"

    def __init__(self, name):
        super().__init__(name)
        # (data_feed_key, data_spec_version) pairs, standing in for what an
        # ingestion plan's data_spec_versions resolves to in a real source.
        self.declared_files = [("f1", "v1")]
        self.load_calls = 0
        self.find_datasets_calls = 0

    def find_datasets(
        self, dataset_type, data_spec_versions, dataset_collection_metadata, **kwargs
    ):
        self.find_datasets_calls += 1

        def loader(file_resource, current_file, **kwargs):
            self.load_calls += 1
            return DraftFile.from_input(
                f"data-{self.load_calls}",
                data_feed_key=file_resource.data_feed_key,
                data_spec_version=file_resource.data_spec_version,
            )

        dataset_resource = DatasetResource(
            dataset_resource_id={"item_id": 0},
            provider=self.provider,
            dataset_type="test",
            name="item-0",
        )
        for data_feed_key, data_spec_version in self.declared_files:
            dataset_resource.add_file(
                last_modified=FIXED_TIME,
                data_feed_key=data_feed_key,
                data_spec_version=data_spec_version,
                file_loader=loader,
            )
        yield dataset_resource


def _setup(engine, source, selector_last_modified=None):
    dsv = DataSpecVersionCollection.from_dict({"default": {"v1", "v2"}})
    attributes = {}
    if selector_last_modified is not None:
        # What a source sets during discovery to let a whole selector be skipped.
        attributes["_last_modified"] = selector_last_modified
    engine.add_ingestion_plan(
        IngestionPlan(
            source=source,
            fetch_policy=FetchPolicy(),
            dataset_type="test",
            selectors=[Selector.build(attributes, data_spec_versions=dsv)],
            data_spec_versions=dsv,
        )
    )


def test_unchanged_file_ids_are_skipped(engine):
    """The pre-check still does its job: when nothing moved, nothing is fetched
    twice. A migration flag must not come at the cost of this."""
    source = VersionedSource("s")
    _setup(engine, source)

    engine.run()
    assert source.load_calls == 1

    engine.run()
    assert source.load_calls == 1, "nothing changed, so the second run must skip"


def test_changed_data_spec_version_is_skipped_by_default(engine):
    """A bumped version is deliberately NOT picked up by an ordinary run: the
    pre-check compares timestamps, which a rename leaves untouched. Pinned so
    the default cannot start cascading re-fetches by accident."""
    source = VersionedSource("s")
    _setup(engine, source)

    engine.run()
    assert source.load_calls == 1

    source.declared_files = [("f1", "v2")]
    engine.run()

    assert source.load_calls == 1, "an ordinary run must leave existing datasets alone"


def test_recheck_all_refetches_a_changed_data_spec_version(engine):
    """With the flag the per-dataset pre-check is bypassed, should_refetch sees
    a file id it has never stored, and the dataset is migrated."""
    source = VersionedSource("s")
    _setup(engine, source)

    engine.run()
    assert source.load_calls == 1

    source.declared_files = [("f1", "v2")]
    engine.run(run_pre_checks=False)
    assert source.load_calls == 2, "a new data spec version must be refetched"

    # The dataset now holds f1__v1 and f1__v2, so the ids it reports are a
    # subset of what is stored and an ordinary run settles again.
    engine.run()
    assert source.load_calls == 2, "after migrating, ordinary runs skip again"


def test_recheck_all_bypasses_the_selector_skip(engine):
    """The second shortcut sits in front of find_datasets, so a flag that only
    reached the per-dataset pre-check would still strand a whole selector."""
    source = VersionedSource("s")
    _setup(engine, source, selector_last_modified=FIXED_TIME - timedelta(days=1))

    engine.run()
    assert source.find_datasets_calls == 1
    assert source.load_calls == 1

    # The lake is now newer than the selector, so discovery is skipped entirely.
    engine.run()
    assert source.find_datasets_calls == 1, "selector skip should block discovery"

    engine.run(run_pre_checks=False)
    assert source.find_datasets_calls == 2, "the flag must also reach the selector skip"
