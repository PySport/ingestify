init(
    store=Store(dataset_url=environ["DATABASE_URL"], file_url="/tmp/blaat"),
    sources={"statsbomb": Source.load("StatsbombGithub")},
    extract_jobs=[Job(source="statsbomb", selectors=[])],
    load_job=[
        Job(
            on=["DatasetCreated", "VersionAdded"],
            filter={"${dataset.dataset_type}": "event"},
        )
    ],
)
