# Ingestify

## Data Management Platform

In general a data management platform contains:
1. Ingestion of data (Extract from Source into Load into Data Lake)
2. Transformation of data (Extract from Data Lake, Transform and Load into Data Warehouse)
3. Utilization of data

<img src="https://www.getdbt.com/ui/img/blog/what-exactly-is-dbt/1-BogoeTTK1OXFU1hPfUyCFw.png" />
Source: https://www.getdbt.com/blog/what-exactly-is-dbt/

TODO: Improve drawings and explain more

## Ingestify

Ingestify focus' on Ingestion of data. 

### How does Ingestify work?

1. A `Source` is asked for all available `Datasets` using the `discover_datasets` method
2. All available `Datasets` are compared with what's already fetched, and if it's changed (using a `FetchPolicy`)
3. A `TaskQueue` is filled with `Tasks` to fetch all missing or stale `Datasets`

<img src="https://raw.githubusercontent.com/PySport/ingestify/refs/heads/main/docs/overview.svg" />

- [Source](blob/main/ingestify/domain/models/source.py) is the main entrance from Ingestify to external sources. A Source must always define:
  - `discover_datasets` - Creates a list of all available datasets on the Source
  - `fetch_dataset_files` - Fetches a single dataset for a Source
- [Dataset Store](blob/main/ingestify/application/dataset_store.py) manages the access to the Metadata storage and the file storage. It keeps track of versions, and knows how to load data.
- [Loader](blob/main/ingestify/application/loader.py) organizes the fetching process. It does this by executing the following steps:
  1. Ask `Source` for all available datasets for a selector
  2. Ask `Dataset Store` for all available datasets for a selector
  3. Determines missing `Datasets`
  4. Create tasks for data retrieval and puts in `TaskQueue`
  5. Use multiprocessing to execute all tasks

## Get started

### Install

Make sure you have installed the latest version:
```bash
pip install git+https://github.com/PySport/ingestify.git

# OR

pip install git+ssh://git@github.com/PySport/ingestify.git
```

### Using a template

Ingestify provides some templates to get started quickly. When using `ingestify init` a new project will be created and example files are copied.
Currently, Ingestify offers a `statsbomb_github` and `wyscout` template. 

#### Statsbomb Github

This uses https://github.com/statsbomb/open-data as source and syncs some competitions.

```
bash# ingestify init --template statsbomb_github /tmp/ingestify-test

2023-05-23 08:57:51,250 [INFO] ingestify.cmdline: Initialized project at `/tmp/ingestify-test` with template `statsbomb_github`
```

#### Wyscout

This requires valid Wyscout credentials. The templates includes some security best practices like using a `.env` file for credentials which isn't part of version control. 

```
bash# ingestify init --template wyscout /tmp/ingestify-test

2023-05-23 08:58:18,720 [INFO] ingestify.cmdline: Initialized project at `/tmp/ingestify-test` with template `wyscout`
```

### Running Ingestify

To actually run Ingestify you first change the current directory to the project directory.

Then run:
```bash
bash# ingestify run

2023-05-23 08:59:07,066 [INFO] ingestify.main: Initializing sources
2023-05-23 08:59:07,068 [INFO] ingestify.main: Initializing IngestionEngine
2023-05-23 08:59:07,086 [INFO] ingestify.main: Determining tasks...
2023-05-23 08:59:07,364 [INFO] ingestify.application.loader: Discovered 33 datasets from StatsbombGithub using selector competition_id=11/season_id=42 => 33 tasks. 0 skipped.
2023-05-23 08:59:07,625 [INFO] ingestify.application.loader: Discovered 35 datasets from StatsbombGithub using selector competition_id=11/season_id=90 => 35 tasks. 0 skipped.
2023-05-23 08:59:07,625 [INFO] ingestify.application.loader: Scheduled 68 tasks. With 10 processes
2023-05-23 08:59:07,654 [INFO] ingestify.application.loader: Running task CreateDatasetTask(StatsbombGithub -> competition_id=11/season_id=42/match_id=303516)
2023-05-23 08:59:07,654 [INFO] ingestify.application.loader: Running task CreateDatasetTask(StatsbombGithub -> competition_id=11/season_id=42/match_id=303731)
2023-05-23 08:59:07,655 [INFO] ingestify.application.loader: Running task CreateDatasetTask(StatsbombGithub -> competition_id=11/season_id=42/match_id=303430)
2023-05-23 08:59:07,655 [INFO] ingestify.application.loader: Running task CreateDatasetTask(StatsbombGithub -> competition_id=11/season_id=42/match_id=303504)
2023-05-23 08:59:07,655 [INFO] ingestify.application.loader: Running task CreateDatasetTask(StatsbombGithub -> competition_id=11/season_id=42/match_id=303421)
2023-05-23 08:59:07,655 [INFO] ingestify.application.loader: Running task CreateDatasetTask(StatsbombGithub -> competition_id=11/season_id=42/match_id=303400)
2023-05-23 08:59:07,656 [INFO] ingestify.application.loader: Running task CreateDatasetTask(StatsbombGithub -> competition_id=11/season_id=42/match_id=303664)
2023-05-23 08:59:07,656 [INFO] ingestify.application.loader: Running task CreateDatasetTask(StatsbombGithub -> competition_id=11/season_id=42/match_id=303680)
2023-05-23 08:59:07,657 [INFO] ingestify.application.loader: Running task CreateDatasetTask(StatsbombGithub -> competition_id=11/season_id=42/match_id=303487)
2023-05-23 08:59:07,658 [INFO] ingestify.application.loader: Running task CreateDatasetTask(StatsbombGithub -> competition_id=11/season_id=42/match_id=303615)
2023-05-23 08:59:08,419 [INFO] ingestify.application.loader: Running task CreateDatasetTask(StatsbombGithub -> competition_id=11/season_id=42/match_id=303532)
2023-05-23 08:59:08,421 [INFO] ingestify.application.loader: Running task CreateDatasetTask(StatsbombGithub -> competition_id=11/season_id=42/match_id=303682)
2023-05-23 08:59:08,444 [INFO] ingestify.application.loader: Running task CreateDatasetTask(StatsbombGithub -> competition_id=11/season_id=42/match_id=303451)
2023-05-23 08:59:08,462 [INFO] ingestify.application.loader: Running task CreateDatasetTask(StatsbombGithub -> competition_id=11/season_id=42/match_id=303596)
2023-05-23 08:59:08,518 [INFO] ingestify.application.loader: Running task CreateDatasetTask(StatsbombGithub -> competition_id=11/season_id=42/match_id=303634)
2023-05-23 08:59:08,528 [INFO] ingestify.application.loader: Running task CreateDatasetTask(StatsbombGithub -> competition_id=11/season_id=42/match_id=303479)
2023-05-23 08:59:08,541 [INFO] ingestify.application.loader: Running task CreateDatasetTask(StatsbombGithub -> competition_id=11/season_id=42/match_id=303696)
2023-05-23 08:59:08,638 [INFO] ingestify.application.loader: Running task CreateDatasetTask(StatsbombGithub -> competition_id=11/season_id=42/match_id=303725)
2023-05-23 08:59:08,684 [INFO] ingestify.application.loader: Running task CreateDatasetTask(StatsbombGithub -> competition_id=11/season_id=42/match_id=303600)
2023-05-23 08:59:08,962 [INFO] ingestify.application.loader: Running task CreateDatasetTask(StatsbombGithub -> competition_id=11/season_id=42/match_id=303493)
2023-05-23 08:59:09,270 [INFO] ingestify.application.loader: Running task CreateDatasetTask(StatsbombGithub -> competition_id=11/season_id=42/match_id=303548)
2023-05-23 08:59:09,276 [INFO] ingestify.application.loader: Running task CreateDatasetTask(StatsbombGithub -> competition_id=11/season_id=42/match_id=303674)
2023-05-23 08:59:09,292 [INFO] ingestify.application.loader: Running task CreateDatasetTask(StatsbombGithub -> competition_id=11/season_id=42/match_id=303700)
2023-05-23 08:59:09,332 [INFO] ingestify.application.loader: Running task CreateDatasetTask(StatsbombGithub -> competition_id=11/season_id=42/match_id=303666)
2023-05-23 08:59:09,411 [INFO] ingestify.application.loader: Running task CreateDatasetTask(StatsbombGithub -> competition_id=11/season_id=42/match_id=303377)
2023-05-23 08:59:09,462 [INFO] ingestify.application.loader: Running task CreateDatasetTask(StatsbombGithub -> competition_id=11/season_id=42/match_id=303517)
2023-05-23 08:59:09,491 [INFO] ingestify.application.loader: Running task CreateDatasetTask(StatsbombGithub -> competition_id=11/season_id=42/match_id=303473)
2023-05-23 08:59:09,511 [INFO] ingestify.application.loader: Running task CreateDatasetTask(StatsbombGithub -> competition_id=11/season_id=90/match_id=3773631)
2023-05-23 08:59:09,726 [INFO] ingestify.application.loader: Running task CreateDatasetTask(StatsbombGithub -> competition_id=11/season_id=90/match_id=3773497)
2023-05-23 08:59:09,757 [INFO] ingestify.application.loader: Running task CreateDatasetTask(StatsbombGithub -> competition_id=11/season_id=90/match_id=3773593)
2023-05-23 08:59:09,957 [INFO] ingestify.application.loader: Running task CreateDatasetTask(StatsbombGithub -> competition_id=11/season_id=42/match_id=303652)
2023-05-23 08:59:09,999 [INFO] ingestify.application.loader: Running task CreateDatasetTask(StatsbombGithub -> competition_id=11/season_id=42/match_id=303715)
2023-05-23 08:59:10,075 [INFO] ingestify.application.loader: Running task CreateDatasetTask(StatsbombGithub -> competition_id=11/season_id=42/match_id=303470)
2023-05-23 08:59:10,103 [INFO] ingestify.application.loader: Running task CreateDatasetTask(StatsbombGithub -> competition_id=11/season_id=42/match_id=303707)
2023-05-23 08:59:10,188 [INFO] ingestify.application.loader: Running task CreateDatasetTask(StatsbombGithub -> competition_id=11/season_id=90/match_id=3773457)
2023-05-23 08:59:10,248 [INFO] ingestify.application.loader: Running task CreateDatasetTask(StatsbombGithub -> competition_id=11/season_id=42/match_id=303524)
2023-05-23 08:59:10,282 [INFO] ingestify.application.loader: Running task CreateDatasetTask(StatsbombGithub -> competition_id=11/season_id=90/match_id=3773665)
2023-05-23 08:59:10,411 [INFO] ingestify.application.loader: Running task CreateDatasetTask(StatsbombGithub -> competition_id=11/season_id=42/match_id=303610)
2023-05-23 08:59:10,563 [INFO] ingestify.application.loader: Running task CreateDatasetTask(StatsbombGithub -> competition_id=11/season_id=90/match_id=3773466)
2023-05-23 08:59:10,711 [INFO] ingestify.application.loader: Running task CreateDatasetTask(StatsbombGithub -> competition_id=11/season_id=90/match_id=3773585)
2023-05-23 08:59:10,768 [INFO] ingestify.application.loader: Running task CreateDatasetTask(StatsbombGithub -> competition_id=11/season_id=90/match_id=3773672)
2023-05-23 08:59:10,778 [INFO] ingestify.application.loader: Running task CreateDatasetTask(StatsbombGithub -> competition_id=11/season_id=90/match_id=3773565)
2023-05-23 08:59:10,867 [INFO] ingestify.application.loader: Running task CreateDatasetTask(StatsbombGithub -> competition_id=11/season_id=90/match_id=3773660)
2023-05-23 08:59:10,954 [INFO] ingestify.application.loader: Running task CreateDatasetTask(StatsbombGithub -> competition_id=11/season_id=90/match_id=3773656)
2023-05-23 08:59:10,974 [INFO] ingestify.application.loader: Running task CreateDatasetTask(StatsbombGithub -> competition_id=11/season_id=90/match_id=3773586)
2023-05-23 08:59:11,026 [INFO] ingestify.application.loader: Running task CreateDatasetTask(StatsbombGithub -> competition_id=11/season_id=90/match_id=3773387)
2023-05-23 08:59:11,136 [INFO] ingestify.application.loader: Running task CreateDatasetTask(StatsbombGithub -> competition_id=11/season_id=90/match_id=3773369)
2023-05-23 08:59:11,438 [INFO] ingestify.application.loader: Running task CreateDatasetTask(StatsbombGithub -> competition_id=11/season_id=90/match_id=3773552)
2023-05-23 08:59:11,515 [INFO] ingestify.application.loader: Running task CreateDatasetTask(StatsbombGithub -> competition_id=11/season_id=90/match_id=3773597)
2023-05-23 08:59:11,586 [INFO] ingestify.application.loader: Running task CreateDatasetTask(StatsbombGithub -> competition_id=11/season_id=90/match_id=3773571)
2023-05-23 08:59:11,610 [INFO] ingestify.application.loader: Running task CreateDatasetTask(StatsbombGithub -> competition_id=11/season_id=90/match_id=3773587)
2023-05-23 08:59:11,690 [INFO] ingestify.application.loader: Running task CreateDatasetTask(StatsbombGithub -> competition_id=11/season_id=90/match_id=3773386)
2023-05-23 08:59:11,727 [INFO] ingestify.application.loader: Running task CreateDatasetTask(StatsbombGithub -> competition_id=11/season_id=90/match_id=3773377)
2023-05-23 08:59:11,757 [INFO] ingestify.application.loader: Running task CreateDatasetTask(StatsbombGithub -> competition_id=11/season_id=90/match_id=3773372)
2023-05-23 08:59:11,899 [INFO] ingestify.application.loader: Running task CreateDatasetTask(StatsbombGithub -> competition_id=11/season_id=90/match_id=3764661)
2023-05-23 08:59:11,901 [INFO] ingestify.application.loader: Running task CreateDatasetTask(StatsbombGithub -> competition_id=11/season_id=90/match_id=3773695)
2023-05-23 08:59:12,006 [INFO] ingestify.application.loader: Running task CreateDatasetTask(StatsbombGithub -> competition_id=11/season_id=90/match_id=3773661)
2023-05-23 08:59:12,186 [INFO] ingestify.application.loader: Running task CreateDatasetTask(StatsbombGithub -> competition_id=11/season_id=90/match_id=3773474)
2023-05-23 08:59:12,283 [INFO] ingestify.application.loader: Running task CreateDatasetTask(StatsbombGithub -> competition_id=11/season_id=90/match_id=3773523)
2023-05-23 08:59:12,339 [INFO] ingestify.application.loader: Running task CreateDatasetTask(StatsbombGithub -> competition_id=11/season_id=90/match_id=3773403)
2023-05-23 08:59:12,426 [INFO] ingestify.application.loader: Running task CreateDatasetTask(StatsbombGithub -> competition_id=11/season_id=90/match_id=3773428)
2023-05-23 08:59:12,582 [INFO] ingestify.application.loader: Running task CreateDatasetTask(StatsbombGithub -> competition_id=11/season_id=90/match_id=3773415)
2023-05-23 08:59:12,583 [INFO] ingestify.application.loader: Running task CreateDatasetTask(StatsbombGithub -> competition_id=11/season_id=90/match_id=3773689)
2023-05-23 08:59:12,705 [INFO] ingestify.application.loader: Running task CreateDatasetTask(StatsbombGithub -> competition_id=11/season_id=90/match_id=3773526)
2023-05-23 08:59:13,510 [INFO] ingestify.application.loader: Running task CreateDatasetTask(StatsbombGithub -> competition_id=11/season_id=90/match_id=3773477)
2023-05-23 08:59:13,538 [INFO] ingestify.application.loader: Running task CreateDatasetTask(StatsbombGithub -> competition_id=11/season_id=90/match_id=3764440)
2023-05-23 08:59:13,592 [INFO] ingestify.application.loader: Running task CreateDatasetTask(StatsbombGithub -> competition_id=11/season_id=90/match_id=3773625)
2023-05-23 08:59:15,017 [INFO] ingestify.application.loader: Running task CreateDatasetTask(StatsbombGithub -> competition_id=11/season_id=90/match_id=3773547)
2023-05-23 08:59:15,917 [INFO] ingestify.cmdline: Done
```

When we run it for the second time:
```bash
bash# ingestify run

2023-05-23 08:59:48,001 [INFO] ingestify.main: Initializing sources
2023-05-23 08:59:48,002 [INFO] ingestify.main: Initializing IngestionEngine
2023-05-23 08:59:48,006 [INFO] ingestify.main: Determining tasks...
2023-05-23 08:59:48,067 [INFO] ingestify.application.loader: Discovered 33 datasets from StatsbombGithub using selector competition_id=11/season_id=42 => 0 tasks. 33 skipped.
2023-05-23 08:59:48,118 [INFO] ingestify.application.loader: Discovered 35 datasets from StatsbombGithub using selector competition_id=11/season_id=90 => 0 tasks. 35 skipped.
2023-05-23 08:59:48,118 [INFO] ingestify.application.loader: Nothing to do.
2023-05-23 08:59:48,119 [INFO] ingestify.cmdline: Done
```

## Using the data

The project contains a `query.py` file with an example of how to use the data.

```bash
bash# python query.py

Loaded dataset with 3702 events
Loaded dataset with 3994 events
Loaded dataset with 3831 events
Loaded dataset with 3647 events
Loaded dataset with 4062 events
Loaded dataset with 4051 events

.....

```


How to go from raw data to parquet files:

```python
from ingestify.main import get_datastore

store = get_datastore("config.yaml")

dataset_collection = store.get_dataset_collection(
    provider="statsbomb", stage="raw"
)

# Store.map is using multiprocessing by default
store.map(
    lambda dataset: (
        store
        
        # As it's related to https://github.com/PySport/kloppy the store can load files using kloppy
        .load_with_kloppy(dataset)
        
        # Convert it into a polars dataframe using all columns in the original data and some more additional ones
        .to_df(
            "*", 
            match_id=dataset.identifier.match_id,
            competition_id=dataset.identifier.competition_id,
            season_id=dataset.identifier.season_id, 
            
            engine="polars"
        )
        
        # Write to parquet format
        .write_parquet(
            f"/tmp/files/blaat/{dataset.identifier.match_id}.parquet"
        )
    ),
    dataset_collection,
)

# TODO: 
#  - when a file is written in parquet format (on any other format) it should be added as such to the store.
```


## Future work

Some future work include:
- Workflow tools - Run custom workflows using with tools like [Airflow](https://airflow.apache.org/), [Dagster](https://docs.dagster.io/getting-started), [Prefect](https://www.prefect.io/), [DBT](https://www.getdbt.com/)
- Execution engines - Run tasks on other execution engines like [AWS Lambda](https://aws.amazon.com/lambda/), [Dask](https://www.dask.org/)
- Lineage - Keep track of lineage with tools like [SQLLineage](https://sqllineage.readthedocs.io/en/latest/index.html)
- Data quality - Monitor data quality with tools like [Great Expectations](https://docs.greatexpectations.io/docs/tutorials/quickstart/)
- Event Bus - Automatically publish events to external systems like [AWS Event Bridge](https://aws.amazon.com/eventbridge/), [Azure Event Grid](https://learn.microsoft.com/en-us/azure/event-grid/overview), [Google Cloud Pub/Sub](https://cloud.google.com/pubsub/docs/overview), [Kafka](https://kafka.apache.org/), [RabbitMQ](https://www.rabbitmq.com/)
- Query Engines - Integrate with query engines to run SQL queries directly on the store using tools like [DuckDB](https://duckdb.org/), [DataBend](https://databend.rs/), [DataFusion](https://arrow.apache.org/datafusion/), [Polars](https://www.pola.rs/), [Spark](https://spark.apache.org/)
- Streaming Data - Ingest streaming data
