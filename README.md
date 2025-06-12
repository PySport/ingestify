# Ingestify

_Ingest everything – JSON, CSV, tracking ZIPs, even MP4 – keep it version‑safe, sync only what changed, and analyse while you ingest._

---

## Why Ingestify?

Football‐data APIs are often **slow**, **rate‑limited** or just **down**. One parsing bug and you’re forced to pull tens of gigabytes again.  
Ingestify fixes that by building **your own data lake** of untouched provider files and fetching only what’s new:

* **Own your lake** – The first time you ask for a match, Ingestify downloads the original files (metadata, line‑ups, events, tracking, video) and stores them untouched in local disk, S3, GCS… every later query hits *your* lake, not the provider.
* **Never re‑fetch the world** – A file‑level checksum / timestamp check moves only changed bundles across the wire.
* **Atomic, complete packages** – A *Dataset* is all‑or‑nothing:

  | Dataset type | Always contains |
  |--------------|-----------------|
  | **Match Dataset** | metadata + line‑ups + events |
  | **Tracking Dataset** | metadata + raw tracking frames |

 You never analyse events v2 with lineups v1, or yesterday’s first half with today’s second half.
* **Query while ingesting** – Datasets stream out of the engine the moment their files land, so notebooks or downstream services can start before the full season is in.

---

## The Ingestify Workflow
<img src="https://raw.githubusercontent.com/PySport/ingestify/refs/heads/main/docs/overview.svg" />

---

## What you gain

### For football‑analytics practitioners

| Pain | Ingestify fix |
|------|---------------|
| API slowness / downtime | One request → lake; retries and parallelism happen behind the scenes. |
| Full re‑ingest after a bug | File‑level deltas mean you fetch only the corrected bundles. |
| Partial / drifting data | Dataset is atomic, versioned, and validated before it becomes visible. |
| Waiting hours for a season to sync | Stream each Dataset as soon as it lands; analyse while you ingest. |
| Boilerplate joins | `engine.load_dataset_with_kloppy(dataset)` → analysis‑ready object. |

### For software engineers

| Need | How Ingestify helps |
|------|---------------------|
| **Domain‑Driven Design** | `Dataset`, `Revision`, `Selector` plus rich domain events read like the problem space. |
| **Event‑driven integrations** | Subscribe to `RevisionAdded` and push to Kafka, AWS Lambda, Airflow… |
| **Pluggable everything** | Swap `Source`, `FetchPolicy`, `DatasetStore` subclasses to add providers, change delta logic, or move storage back‑ends. |
| **Safety & speed** | Multiprocessing downloader with temp‑dir commits – no half‑written matches; near‑linear I/O speed‑ups. |
| **Any file type** | JSON, CSV, MP4, proprietary binaries – stored verbatim so you parse / transcode later under version control. |

---

## Quick start

```bash
pip install ingestify            # or: pip install git+https://github.com/PySport/ingestify.git
```

### Minimal `config.yaml`

```yaml
main:
  metadata_url: sqlite:///database/catalog.db   # where revision metadata lives
  file_url: file://database/files/              # where raw files live
  default_bucket: main

sources:
  statsbomb:
    type: ingestify.statsbomb_github            # open‑data provider

ingestion_plans:
  - source: statsbomb
    dataset_type: match
    # selectors can narrow the scope
    # selectors:
    #   - competition_id: 11
    #     season_id: [90]
```

### First ingest

When you configured event subscribers, all domain events are dispatched to the subscriber. Publishing the events to
Kafka, RabbitMQ or any other system becomes trivial.

```bash
mkdir -p database
pip install kloppy

ingestify run                                # fills your data lake
```

---

## Using the data

By default, Ingestify will search in your DatasetStore when you request data. You can pass several filters to only fetch what you need.

```python
from ingestify.main import get_engine

engine = get_engine("config.yaml")

for dataset in engine.iter_datasets(
        dataset_state="complete",
        provider="statsbomb",
        dataset_type="match",
        competition_id=11,
        season_id=90):
    df = (
        engine
        .load_dataset_with_kloppy(dataset)
        .to_df(engine="polars")
    )
    df.write_parquet(f"out/{dataset.identifier['match_id']}.parquet")
```

#### Auto Ingestion

When you don't want to use event driven architecture but just want to work with the latest data, ingestify got you covered. With the `auto_ingest` option, ingestify syncs the data in the background when you ask for the data. 

        
```python
from ingestify.main import get_engine

engine = get_engine("config.yaml")

for dataset in engine.iter_datasets(
        # When set to True it will first do a full sync and then start yielding datasets
        auto_ingest=True, 
  
        # With streaming enabled all Datasets are yielded when they are up-to-date (not changed, or refetched)
        # auto_ingest={"streaming": True}
  
        dataset_state="complete",
        provider="statsbomb",
        dataset_type="match",
        competition_id=11,
        season_id=90):
    df = (
        engine
        .load_dataset_with_kloppy(dataset)
        .to_df(engine="polars")
    )
    df.write_parquet(f"out/{dataset.identifier['match_id']}.parquet")
```

#### Open data

Ingestify has build-in support for StatsBomb Open Data (more to come).

```shell
mkdir database_open_data
pip install kloppy
```

```python
from ingestify.main import get_engine

engine = get_engine(
    metadata_url="sqlite:///database_open_data/catalog.db",
    file_url="file://database_open_data/files/"
)

dataset_iter = engine.iter_datasets(
    # This will tell ingestify to look for an Open Data provider
    auto_ingest={"use_open_data": True, "streaming": True},

    provider="statsbomb",
    dataset_type="match",
    competition_id=43,
    season_id=281
)

for dataset in dataset_iter:
    kloppy_dataset = engine.load_dataset_with_kloppy(dataset)
```


---

## Roadmap

* Workflow orchestration helpers (Airflow, Dagster, Prefect)
* Built‑in Kafka / Kinesis event emitters
* Streaming data ingestion
* Data quality hooks (Great Expectations)

---

**Stop refetching the world. Own your data lake, keep it version‑safe, and analyse football faster with Ingestify.**
