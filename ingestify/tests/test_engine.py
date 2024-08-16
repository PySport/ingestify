from ingestify.application.ingestion_engine import IngestionEngine
from ingestify.domain import Identifier, Selector, DataSpecVersionCollection
from ingestify.domain.models.extract_job import ExtractJob
from ingestify.domain.models.fetch_policy import FetchPolicy
from ingestify.main import get_engine


def add_extract_job(engine: IngestionEngine, **selector):
    data_spec_versions = DataSpecVersionCollection.from_dict({"default": {"v1"}})

    engine.add_extract_job(
        ExtractJob(
            source=engine.loader.extract_jobs[0].source,
            fetch_policy=FetchPolicy(),
            selectors=[Selector.build(selector, data_spec_versions=data_spec_versions)],
            dataset_type="match",
            data_spec_versions=data_spec_versions,
        )
    )


def test_engine():
    engine = get_engine("config.yaml", "main")
    engine.load()
    datasets = engine.store.get_dataset_collection()
    assert len(datasets) == 1

    dataset = datasets.first()
    assert dataset.identifier == Identifier(competition_id=1, season_id=2)
    assert len(dataset.revisions) == 1

    engine.load()
    datasets = engine.store.get_dataset_collection()
    assert len(datasets) == 1

    dataset = datasets.first()
    assert dataset.identifier == Identifier(competition_id=1, season_id=2)
    assert len(dataset.revisions) == 2
    assert len(dataset.revisions[0].modified_files) == 2
    assert len(dataset.revisions[1].modified_files) == 1

    add_extract_job(engine, competition_id=1, season_id=3)
    engine.load()

    datasets = engine.store.get_dataset_collection()
    assert len(datasets) == 2

    datasets = engine.store.get_dataset_collection(season_id=3)
    assert len(datasets) == 1
