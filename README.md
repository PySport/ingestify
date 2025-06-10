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

pip install ingestify
```

### Basic Setup

Ingestify requires a configuration file to define your data sources and ingestion plans. Create a file named `config.yaml` in your project directory.

#### Minimal Configuration Example

```yaml
main:
  metadata_url: sqlite:///database/catalog.db  # SQLite database for metadata
  file_url: file://database/files/  # Local file storage
  default_bucket: main  # Default storage bucket

sources:
  statsbomb:  # Define a Statsbomb data source
    type: ingestify.statsbomb_github

ingestion_plans:
  - source: statsbomb  # Use the Statsbomb source defined above
    dataset_type: match
    selectors:
      - competition_id: 11  # Example: English Premier League
        season_id: [90]  # Example: 2022/2023 season
```

### Running Your First Ingestion

1. Create the necessary directories:
```bash
mkdir -p database
```

2. Run the ingestion:
```bash
ingestify run
```

## Using the data

Once you've ingested data, you can access it using the dataset store. Here's an example of how to iterate over datasets and process them:

```python
import concurrent.futures
from tqdm import tqdm
from ingestify.main import get_datastore

store = get_datastore("config.yaml")

def process_dataset(dataset):
    """Process a single dataset by reading """
    
    (
        store.load_with_kloppy(dataset)
        .to_df(engine="polars")
        .write_parquet(f"{dataset.identifier['match_id']}.parquet")
    )
    
    return dataset.dataset_id

# Iterate over dataset collections in batches
dataset_collection_batches = store.iter_dataset_collection_batches(
    dataset_state="complete",
    dataset_type="match",
    provider="statsbomb",
    competition_id=11,
    season_id=90,
  
    # Fetch datasets in batch
    page_size=1000,
    yield_dataset_collection=True
)

# Process datasets in parallel
with concurrent.futures.ProcessPoolExecutor(max_workers=10) as pool:
    for dataset_batch in dataset_collection_batches:
        for result in tqdm(pool.map(process_dataset, dataset_batch)):
            # Process your results here
            print(f"Completed processing dataset {result}")
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
